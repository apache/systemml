/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.instructions.fed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRange;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederatedUDF;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.functionobjects.DiagIndex;
import org.apache.sysds.runtime.functionobjects.RevIndex;
import org.apache.sysds.runtime.functionobjects.SwapIndex;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.lineage.LineageItemUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;

public class ReorgFEDInstruction extends UnaryFEDInstruction {
	
	public ReorgFEDInstruction(Operator op, CPOperand in1, CPOperand out, String opcode, String istr, FederatedOutput fedOut) {
		super(FEDType.Reorg, op, in1, out, opcode, istr, fedOut);
	}

	public ReorgFEDInstruction(Operator op, CPOperand in1, CPOperand out, String opcode, String istr) {
		super(FEDType.Reorg, op, in1, out, opcode, istr);
	}

	public static ReorgFEDInstruction parseInstruction ( String str ) {
		CPOperand in = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);
		CPOperand out = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);

		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		if ( opcode.equalsIgnoreCase("r'") ) {
			InstructionUtils.checkNumFields(str, 2, 3, 4);
			in.split(parts[1]);
			out.split(parts[2]);
			int k = Integer.parseInt(parts[3]);
			FederatedOutput fedOut = FederatedOutput.valueOf(parts[4]);
			return new ReorgFEDInstruction(new ReorgOperator(SwapIndex.getSwapIndexFnObject(), k), in, out, opcode, str, fedOut);
		}
		else if ( opcode.equalsIgnoreCase("rdiag") ) {
			parseUnaryInstruction(str, in, out); //max 2 operands
			return new ReorgFEDInstruction(new ReorgOperator(DiagIndex.getDiagIndexFnObject()), in, out, opcode, str);
		}
		else if ( opcode.equalsIgnoreCase("rev") ) {
			parseUnaryInstruction(str, in, out); //max 2 operands
			return new ReorgFEDInstruction(new ReorgOperator(RevIndex.getRevIndexFnObject()), in, out, opcode, str);
		}
		else {
			throw new DMLRuntimeException("ReorgFEDInstruction: unsupported opcode: "+opcode);
		}
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = ec.getMatrixObject(input1);
		ReorgOperator r_op = (ReorgOperator) _optr;

		if( !mo1.isFederated() )
			throw new DMLRuntimeException("Federated Reorg: "
				+ "Federated input expected, but invoked w/ "+mo1.isFederated());

		if(instOpcode.equals("r'")) {
			//execute transpose at federated site
			FederatedRequest fr1 = FederationUtils.callInstruction(instString,
				output, new CPOperand[] {input1},
				new long[] {mo1.getFedMapping().getID()}, true);
			mo1.getFedMapping().execute(getTID(), fr1);

			//drive output federated mapping
			MatrixObject out = ec.getMatrixObject(output);
			out.getDataCharacteristics().set(mo1.getNumColumns(), mo1.getNumRows(), (int) mo1.getBlocksize(), mo1.getNnz());
			out.setFedMapping(mo1.getFedMapping().copyWithNewID(fr1.getID()).transpose());
		}
		else if(instOpcode.equalsIgnoreCase("rev")) {
			//execute transpose at federated site
			FederatedRequest fr1 = FederationUtils.callInstruction(instString,
				output, new CPOperand[] {input1},
				new long[] {mo1.getFedMapping().getID()}, true);
			mo1.getFedMapping().execute(getTID(), fr1);

			if(mo1.isFederated(FederationMap.FType.ROW))
				mo1.getFedMapping().reverseFedMap();

			//derive output federated mapping
			MatrixObject out = ec.getMatrixObject(output);
			out.getDataCharacteristics().set(mo1.getNumRows(), mo1.getNumColumns(), (int) mo1.getBlocksize(), mo1.getNnz());
			out.setFedMapping(mo1.getFedMapping().copyWithNewID(fr1.getID()));
		}
		else if (instOpcode.equals("rdiag")) {
			RdiagResult result;
			// diag(diag(X))
			if (mo1.getNumColumns() == 1 && mo1.getNumRows() != 1) {
				result = rdiagV2M(mo1, r_op);
			} else {
				result = rdiagM2V(mo1, r_op);
			}

			FederationMap diagFedMap = result.getFedMap();
			Map<FederatedRange, int[]> dcs = result.getDcs();

			//update fed ranges
			for(int i = 0; i < diagFedMap.getFederatedRanges().length; i++) {
				int[] newRange = dcs.get(diagFedMap.getFederatedRanges()[i]);

				diagFedMap.getFederatedRanges()[i].setBeginDim(0,
					(diagFedMap.getFederatedRanges()[i].getBeginDims()[0] == 0 ||
						i == 0) ? 0 : diagFedMap.getFederatedRanges()[i - 1].getEndDims()[0]);
				diagFedMap.getFederatedRanges()[i].setEndDim(0,
					diagFedMap.getFederatedRanges()[i].getBeginDims()[0] + newRange[0]);
				diagFedMap.getFederatedRanges()[i].setBeginDim(1,
					(diagFedMap.getFederatedRanges()[i].getBeginDims()[1] == 0 ||
						i == 0) ? 0 : diagFedMap.getFederatedRanges()[i - 1].getEndDims()[1]);
				diagFedMap.getFederatedRanges()[i].setEndDim(1,
					diagFedMap.getFederatedRanges()[i].getBeginDims()[1] + newRange[1]);
			}

			//update output mapping and data characteristics
			MatrixObject rdiag = ec.getMatrixObject(output);
			rdiag.getDataCharacteristics()
				.set(diagFedMap.getMaxIndexInRange(0), diagFedMap.getMaxIndexInRange(1),
					(int) mo1.getBlocksize());
			rdiag.setFedMapping(diagFedMap);
		}
	}

	private class RdiagResult {
		FederationMap fedMap;
		Map<FederatedRange, int[]> dcs;

		public RdiagResult(FederationMap fedMap, Map<FederatedRange, int[]> dcs) {
			this.fedMap = fedMap;
			this.dcs = dcs;
		}

		public FederationMap getFedMap() {
			return fedMap;
		}

		public Map<FederatedRange, int[]> getDcs() {
			return dcs;
		}
	}

	private RdiagResult rdiagV2M (MatrixObject mo1, ReorgOperator r_op) {
		FederationMap fedMap = mo1.getFedMapping();
		boolean rowFed = mo1.isFederated(FederationMap.FType.ROW);

		long varID = FederationUtils.getNextFedDataID();
		Map<FederatedRange, int[]> dcs = new HashMap<>();
		FederationMap diagFedMap;

		diagFedMap = fedMap.mapParallel(varID, (range, data) -> {
			try {
				FederatedResponse response = data.executeFederatedOperation(new FederatedRequest(
					FederatedRequest.RequestType.EXEC_UDF, -1,
					new ReorgFEDInstruction.DiagMatrix(data.getVarID(),
						varID, r_op,
						rowFed ? (new int[] {range.getBeginDimsInt()[0], range.getEndDimsInt()[0]}) :
							new int[] {range.getBeginDimsInt()[1], range.getEndDimsInt()[1]},
						rowFed, (int) mo1.getNumRows()))).get();
				if(!response.isSuccessful())
					response.throwExceptionFromResponse();
				int[] subRangeCharacteristics = (int[]) response.getData()[0];
				synchronized(dcs) {
					dcs.put(range, subRangeCharacteristics);
				}
				return null;
			}
			catch(Exception e) {
				throw new DMLRuntimeException(e);
			}
		});
		return new RdiagResult(diagFedMap, dcs);
	}

	private RdiagResult rdiagM2V (MatrixObject mo1, ReorgOperator r_op) {
		FederationMap fedMap = mo1.getFedMapping();
		boolean rowFed = mo1.isFederated(FederationMap.FType.ROW);

		long varID = FederationUtils.getNextFedDataID();
		Map<FederatedRange, int[]> dcs = new HashMap<>();
		FederationMap diagFedMap;

		diagFedMap = fedMap.mapParallel(varID, (range, data) -> {
			try {
				FederatedResponse response = data.executeFederatedOperation(new FederatedRequest(
					FederatedRequest.RequestType.EXEC_UDF, -1,
					new ReorgFEDInstruction.Rdiag(data.getVarID(), varID, r_op,
						rowFed ? (new int[] {range.getBeginDimsInt()[0], range.getEndDimsInt()[0]}) :
							new int[] {range.getBeginDimsInt()[1], range.getEndDimsInt()[1]},
						rowFed))).get();
				if(!response.isSuccessful())
					response.throwExceptionFromResponse();
				int[] subRangeCharacteristics = (int[]) response.getData()[0];
				synchronized(dcs) {
					dcs.put(range, subRangeCharacteristics);
				}
				return null;
			}
			catch(Exception e) {
				throw new DMLRuntimeException(e);
			}
		});
		return new RdiagResult(diagFedMap, dcs);
	}

	public static class Rdiag extends FederatedUDF {

		private static final long serialVersionUID = -3466926635958851402L;
		private final long _outputID;
		private final ReorgOperator _r_op;
		private final int[] _slice;
		private final boolean _rowFed;

		private Rdiag(long input, long outputID, ReorgOperator r_op, int[] slice, boolean rowFed) {
			super(new long[] {input});
			_outputID = outputID;
			_r_op = r_op;
			_slice = slice;
			_rowFed = rowFed;
		}

		@Override
		public FederatedResponse execute(ExecutionContext ec, Data... data) {
			MatrixBlock mb = ((MatrixObject) data[0]).acquireReadAndRelease();
			MatrixBlock soresBlock;
			MatrixBlock res;

			soresBlock = _rowFed ?
				mb.slice(0, mb.getNumRows() - 1, _slice[0], _slice[1] - 1, new MatrixBlock()) :
				mb.slice(_slice[0], _slice[1] - 1);
			res = soresBlock.reorgOperations(_r_op, new MatrixBlock(), 0, 0, 0);

			MatrixObject mout = ExecutionContext.createMatrixObject(res);
			mout.setDiag(true);
			ec.setVariable(String.valueOf(_outputID), mout);

			return new FederatedResponse(FederatedResponse.ResponseType.SUCCESS, new int[]{res.getNumRows(), res.getNumColumns()});
		}

		@Override
		public List<Long> getOutputIds() {
			return new ArrayList<>(Arrays.asList(_outputID));
		}

		@Override
		public Pair<String, LineageItem> getLineageItem(ExecutionContext ec) {
			LineageItem[] liUdfInputs = Arrays.stream(getInputIDs())
					.mapToObj(id -> ec.getLineage().get(String.valueOf(id))).toArray(LineageItem[]::new);
			CPOperand r_op = new CPOperand(_r_op.fn.getClass().getSimpleName(), ValueType.STRING, DataType.SCALAR, true);
			CPOperand slice = new CPOperand(Arrays.toString(_slice), ValueType.STRING, DataType.SCALAR, true);
			CPOperand rowFed = new CPOperand(String.valueOf(_rowFed), ValueType.BOOLEAN, DataType.SCALAR, true);
			LineageItem[] otherInputs = LineageItemUtils.getLineage(ec, r_op, slice, rowFed);
			LineageItem[] liInputs = Stream.concat(Arrays.stream(liUdfInputs), Arrays.stream(otherInputs))
					.toArray(LineageItem[]::new);
			return Pair.of(String.valueOf(_outputID), 
					new LineageItem(getClass().getSimpleName(), liInputs));
		}
	}

	public static class DiagMatrix extends FederatedUDF {

		private static final long serialVersionUID = -3466926635958851402L;
		private final long _outputID;
		private final ReorgOperator _r_op;
		private final int _len;
		private final int[] _slice;
		private final boolean _rowFed;

		private DiagMatrix(long input, long outputID, ReorgOperator r_op, int[] slice, boolean rowFed, int len) {
			super(new long[] {input});
			_outputID = outputID;
			_r_op = r_op;
			_len = len;
			_rowFed = rowFed;
			_slice = slice;
		}

		@Override
		public FederatedResponse execute(ExecutionContext ec, Data... data) {
			MatrixBlock mb = ((MatrixObject) data[0]).acquireReadAndRelease();
			MatrixBlock res;

			MatrixBlock tmp = mb.reorgOperations(_r_op, new MatrixBlock(), 0, 0, 0);
			if(_rowFed) {
				res = new MatrixBlock(mb.getNumRows(), _len, 0.0);
				res.copy(0, res.getNumRows()-1, _slice[0], _slice[1]-1, tmp, false);
			} else {
				res = new MatrixBlock(_len, _slice[1], 0.0);
				res.copy(_slice[0], _slice[1]-1, 0, mb.getNumColumns() - 1, tmp, false);;
			}
			MatrixObject mout = ExecutionContext.createMatrixObject(res);
			mout.setDiag(true);
			ec.setVariable(String.valueOf(_outputID), mout);

			return new FederatedResponse(FederatedResponse.ResponseType.SUCCESS, new int[]{res.getNumRows(), res.getNumColumns()});
		}

		@Override
		public List<Long> getOutputIds() {
			return new ArrayList<>(Arrays.asList(_outputID));
		}

		@Override
		public Pair<String, LineageItem> getLineageItem(ExecutionContext ec) {
			LineageItem[] liUdfInputs = Arrays.stream(getInputIDs())
					.mapToObj(id -> ec.getLineage().get(String.valueOf(id))).toArray(LineageItem[]::new);
			CPOperand r_op = new CPOperand(_r_op.fn.getClass().getSimpleName(), ValueType.STRING, DataType.SCALAR, true);
			CPOperand len = new CPOperand(String.valueOf(_len), ValueType.INT32, DataType.SCALAR, true);
			CPOperand slice = new CPOperand(Arrays.toString(_slice), ValueType.STRING, DataType.SCALAR, true);
			CPOperand rowFed = new CPOperand(String.valueOf(_rowFed), ValueType.BOOLEAN, DataType.SCALAR, true);
			LineageItem[] otherInputs = LineageItemUtils.getLineage(ec, r_op, len, slice, rowFed);
			LineageItem[] liInputs = Stream.concat(Arrays.stream(liUdfInputs), Arrays.stream(otherInputs))
					.toArray(LineageItem[]::new);
			return Pair.of(String.valueOf(_outputID), 
					new LineageItem(getClass().getSimpleName(), liInputs));
		}
	}
}
