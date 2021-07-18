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

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.lops.Lop;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRange;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederatedUDF;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.AlignType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.functionobjects.And;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;

public class CtableFEDInstruction extends ComputationFEDInstruction {
	private final CPOperand _outDim1;
	private final CPOperand _outDim2;
	//private final boolean _isExpand;
	//private final boolean _ignoreZeros;

	private CtableFEDInstruction(CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out, String outputDim1, boolean dim1Literal, String outputDim2, boolean dim2Literal, boolean isExpand,
		boolean ignoreZeros, String opcode, String istr) {
		super(FEDType.Ctable, null, in1, in2, in3, out, opcode, istr);
		_outDim1 = new CPOperand(outputDim1, ValueType.FP64, DataType.SCALAR, dim1Literal);
		_outDim2 = new CPOperand(outputDim2, ValueType.FP64, DataType.SCALAR, dim2Literal);
		//_isExpand = isExpand;
		//_ignoreZeros = ignoreZeros;
	}

	public static CtableFEDInstruction parseInstruction(String inst) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(inst);
		InstructionUtils.checkNumFields(parts, 7);

		String opcode = parts[0];

		//handle opcode
		if(!(opcode.equalsIgnoreCase("ctable"))) {
			throw new DMLRuntimeException("Unexpected opcode in CtableFEDInstruction: " + inst);
		}

		//handle operands
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand in3 = new CPOperand(parts[3]);

		//handle known dimension information
		String[] dim1Fields = parts[4].split(Instruction.LITERAL_PREFIX);
		String[] dim2Fields = parts[5].split(Instruction.LITERAL_PREFIX);

		CPOperand out = new CPOperand(parts[6]);
		boolean ignoreZeros = Boolean.parseBoolean(parts[7]);

		// ctable does not require any operator, so we simply pass-in a dummy operator with null functionobject
		return new CtableFEDInstruction(in1,
			in2, in3, out, dim1Fields[0], Boolean.parseBoolean(dim1Fields[1]),
			dim2Fields[0], Boolean.parseBoolean(dim2Fields[1]), false, ignoreZeros, opcode, inst);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = ec.getMatrixObject(input1);
		MatrixObject mo2 = ec.getMatrixObject(input2);

		boolean reversed = false;
		if(!mo1.isFederated() && mo2.isFederated()) {
			mo1 = ec.getMatrixObject(input2);
			mo2 = ec.getMatrixObject(input1);
			reversed = true;
		}

		// get new output dims
		Long[] dims1 = getOutputDimension(mo1, input1, _outDim1, mo1.getFedMapping().getFederatedRanges());
		Long[] dims2 = getOutputDimension(mo2, input2, _outDim2, mo1.getFedMapping().getFederatedRanges());

		MatrixObject mo3 = input3 != null && input3.isMatrix() ? ec.getMatrixObject(input3) : null;

		boolean reversedWeights = mo3 != null && mo3.isFederated() && !(mo1.isFederated() || mo2.isFederated());
		if(reversedWeights) {
			mo3 = mo1;
			mo1 = ec.getMatrixObject(input3);
		}

		long dim1 = Collections.max(Arrays.asList(dims1), Long::compare);
		boolean fedOutput = dim1 % mo1.getFedMapping().getSize() == 0 && dims1.length == Arrays.stream(dims1).distinct().count();

		processRequest(ec, mo1, mo2, mo3, reversed, reversedWeights, fedOutput, dims1, dims2);
	}

	private void processRequest(ExecutionContext ec, MatrixObject mo1, MatrixObject mo2, MatrixObject mo3,
		boolean reversed, boolean reversedWeights, boolean fedOutput, Long[] dims1, Long[] dims2) {
		Future<FederatedResponse>[] ffr;

		FederatedRequest[] fr1 = mo1.getFedMapping().broadcastSliced(mo2, false, mo1.getUniqueID());
		FederatedRequest fr2, fr3;
		if(mo3 != null && mo1.isFederated() && mo3.isFederated()
		&& mo1.getFedMapping().isAligned(mo3.getFedMapping(), AlignType.FULL)) { // mo1 and mo3 federated and aligned
			if(!reversed)
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2, input3},
					new long[] {mo1.getFedMapping().getID(), fr1[0].getID(), mo3.getFedMapping().getID()});
			else
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2, input3},
					new long[] {fr1[0].getID(), mo1.getFedMapping().getID(), mo3.getFedMapping().getID()});

			fr3 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fr2.getID());
			FederatedRequest fr4 = mo1.getFedMapping().cleanup(getTID(), fr1[0].getID());
			ffr = mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3, fr4);
		}
		else if(mo3 == null) {
			if(!reversed)
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2},
					new long[] {mo1.getFedMapping().getID(), fr1[0].getID()});
			else
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2},
					new long[] {fr1[0].getID(), mo1.getFedMapping().getID()});

			fr3 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fr2.getID());
			FederatedRequest fr4 = mo1.getFedMapping().cleanup(getTID(), fr1[0].getID());
			ffr = mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3, fr4);

		} else {
			FederatedRequest[] fr4 = mo1.getFedMapping().broadcastSliced(mo3, false, mo1.getUniqueID());
			if(!reversed && !reversedWeights)
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2, input3},
					new long[] {mo1.getFedMapping().getID(), fr1[0].getID(), fr4[0].getID()});
			else if(reversed && !reversedWeights)
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2, input3},
					new long[] {fr1[0].getID(), mo1.getFedMapping().getID(), fr4[0].getID()});
			else
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2, input3},
					new long[] {fr1[0].getID(), fr4[0].getID(), mo1.getFedMapping().getID()});

			fr3 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fr2.getID());
			FederatedRequest fr5 = mo1.getFedMapping().cleanup(getTID(), fr1[0].getID(), fr4[0].getID());
			ffr = mo1.getFedMapping().execute(getTID(), true, fr1, fr4, fr2, fr3, fr5);
		}

		if(fedOutput && isFedOutput(ffr, dims1)) {
			MatrixObject out = ec.getMatrixObject(output);
			FederationMap newFedMap = modifyFedRanges(mo1.getFedMapping(), dims1, dims2);
			setFedOutput(mo1, out, newFedMap, dims1, fr2.getID());
		} else {
			ec.setMatrixOutput(output.getName(), aggResult(ffr));
		}
	}

	boolean isFedOutput(Future<FederatedResponse>[] ffr,  Long[] dims1) {
		boolean fedOutput = true;

		long fedSize = Collections.max(Arrays.asList(dims1), Long::compare) / ffr.length;
		try {
			MatrixBlock curr;
			MatrixBlock prev =(MatrixBlock) ffr[0].get().getData()[0];
			for(int i = 1; i < ffr.length && fedOutput; i++) {
				curr = (MatrixBlock) ffr[i].get().getData()[0];
				MatrixBlock sliced = curr.slice((int) (curr.getNumRows() - fedSize), curr.getNumRows() - 1);

				// no intersection
				if(curr.getNumRows() == (i+1) * prev.getNumRows() && curr.getNonZeros() <= prev.getLength()
					&& (curr.getNumRows() - sliced.getNumRows()) == i * prev.getNumRows()
					&& curr.getNonZeros() - sliced.getNonZeros() == 0)
					continue;

				// check intersect with AND and compare number of nnz
				MatrixBlock prevExtend = new MatrixBlock(curr.getNumRows(), curr.getNumColumns(), 0.0);
				prevExtend.copy(0, prev.getNumRows()-1, 0, prev.getNumColumns()-1, prev, true);

				MatrixBlock  intersect = curr.binaryOperationsInPlace(new BinaryOperator(And.getAndFnObject()), prevExtend);
				if(intersect.getNonZeros() != 0)
					fedOutput = false;
				prev = sliced;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return fedOutput;
	}


	private static void setFedOutput(MatrixObject mo1, MatrixObject out, FederationMap fedMap, Long[] dims1, long outId) {
		long fedSize = Collections.max(Arrays.asList(dims1), Long::compare) / dims1.length;

		long d1 = Collections.max(Arrays.asList(dims1), Long::compare);
		long d2 = Collections.max(Arrays.asList(dims1), Long::compare);

		// set output
		out.getDataCharacteristics().set(d1, d2, (int) mo1.getBlocksize(), mo1.getNnz());
		out.setFedMapping(fedMap.copyWithNewID(outId));

		long varID = FederationUtils.getNextFedDataID();
		out.getFedMapping().mapParallel(varID, (range, data) -> {
			try {
				FederatedResponse response = data.executeFederatedOperation(new FederatedRequest(
					FederatedRequest.RequestType.EXEC_UDF, -1,
					new SliceOutput(data.getVarID(), fedSize))).get();
				if(!response.isSuccessful())
					response.throwExceptionFromResponse();
			}
			catch(Exception e) {
				throw new DMLRuntimeException(e);
			}
			return null;
		});
	}

	private static MatrixBlock aggResult(Future<FederatedResponse>[] ffr) {
		MatrixBlock resultBlock = new MatrixBlock(1, 1, 0);
		int dim1 = 0, dim2 = 0;
		for(int i = 0; i < ffr.length; i++) {
			try {
				MatrixBlock mb = ((MatrixBlock) ffr[i].get().getData()[0]);
				dim1 = mb.getNumRows()  > dim1 ? mb.getNumRows() : dim1;
				dim2 = mb.getNumColumns()  > dim2 ? mb.getNumColumns() : dim2;

				// set next and prev to same output dimensions
				MatrixBlock prev = new MatrixBlock(dim1, dim2, 0.0);
				prev.copy(0, resultBlock.getNumRows()-1, 0, resultBlock.getNumColumns()-1, resultBlock, true);

				MatrixBlock next = new MatrixBlock(dim1, dim2, 0.0);
				next.copy(0, mb.getNumRows()-1, 0, mb.getNumColumns()-1, mb, true);

				// add worker results
				BinaryOperator plus = InstructionUtils.parseBinaryOperator("+");
				resultBlock = prev.binaryOperationsInPlace(plus, next);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		return resultBlock;
	}

	private static FederationMap modifyFedRanges(FederationMap fedMap, Long[] dims1, Long[] dims2) {
		IntStream.range(0, fedMap.getFederatedRanges().length).forEach(i -> {
			fedMap.getFederatedRanges()[i]
				.setBeginDim(0, i == 0 ? 0 : fedMap.getFederatedRanges()[i - 1].getEndDims()[0]);
			fedMap.getFederatedRanges()[i].setEndDim(0, dims1[i]);
			fedMap.getFederatedRanges()[i]
				.setBeginDim(1, i == 0 ? 0 : fedMap.getFederatedRanges()[i - 1].getBeginDims()[1]);
			fedMap.getFederatedRanges()[i].setEndDim(1, dims2[i]);
		});
		return fedMap;
	}

	private Long[] getOutputDimension(MatrixObject in, CPOperand inOp, CPOperand outOp, FederatedRange[] federatedRanges) {
		Long[] fedDims = new Long[federatedRanges.length];

		if(!in.isFederated()) {
			//slice
			MatrixBlock mb = in.acquireReadAndRelease();
			IntStream.range(0, federatedRanges.length).forEach(i -> {
				MatrixBlock sliced = mb
					.slice(federatedRanges[i].getBeginDimsInt()[0], federatedRanges[i].getEndDimsInt()[0] - 1);
				fedDims[i] = (long) sliced.max();
			});
			return fedDims;
		}

		String maxInstString = constructMaxInstString(inOp.getName(), outOp.getName());

		// get max per worker
		FederationMap map = in.getFedMapping();
		FederatedRequest fr1 = FederationUtils.callInstruction(maxInstString, outOp,
			new CPOperand[]{inOp}, new long[]{in.getFedMapping().getID()});
		FederatedRequest fr2 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fr1.getID());
		FederatedRequest fr3 = map.cleanup(getTID(), fr1.getID());
		Future<FederatedResponse>[] tmp = map.execute(getTID(), fr1, fr2, fr3);

		return computeOutputDims(tmp);
	}

	private static Long[] computeOutputDims(Future<FederatedResponse>[] tmp) {
		Long[] fedDims = new Long[tmp.length];
		for(int i = 0; i < tmp.length; i ++)
			try {
				fedDims[i] = ((ScalarObject) tmp[i].get().getData()[0]).getLongValue();
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		return fedDims;
	}

	private String constructMaxInstString(String in, String out) {
		String maxInstrString = instString.replace("ctable", "uamax");
		String[] instParts = maxInstrString.split(Lop.OPERAND_DELIMITOR);
		String[] maxInstParts = new String[] {instParts[0], instParts[1],
			InstructionUtils.concatOperandParts(in, DataType.MATRIX.name(), (ValueType.FP64).name()),
			InstructionUtils.concatOperandParts(out, DataType.SCALAR.name(), (ValueType.FP64).name()), "16"};
		return String.join(Lop.OPERAND_DELIMITOR, maxInstParts);
	}

	private static class SliceOutput extends FederatedUDF {

		private static final long serialVersionUID = -2808597461054603816L;
		private final long _fedSize;

		protected SliceOutput(long input, long fedSize) {
			super(new long[] {input});
			_fedSize = fedSize;
		}

		public FederatedResponse execute(ExecutionContext ec, Data... data) {
			MatrixObject mo = (MatrixObject) data[0];
			MatrixBlock mb = mo.acquireReadAndRelease();

			MatrixBlock sliced = mb.slice((int) (mb.getNumRows()-_fedSize), mb.getNumRows()-1);
			mo.acquireModify(sliced);
			mo.release();

			return new FederatedResponse(FederatedResponse.ResponseType.SUCCESS, new Object[] {});
		}
		@Override
		public Pair<String, LineageItem> getLineageItem(ExecutionContext ec) {
			return null;
		}
	}
}