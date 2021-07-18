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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.lops.LeftIndex;
import org.apache.sysds.lops.Lop;
import org.apache.sysds.lops.RightIndex;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.caching.FrameObject;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedData;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRange;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.VariableCPInstruction;
import org.apache.sysds.runtime.util.IndexRange;

public final class IndexingFEDInstruction extends UnaryFEDInstruction {
	protected final CPOperand rowLower, rowUpper, colLower, colUpper;

	protected IndexingFEDInstruction(CPOperand in, CPOperand rl, CPOperand ru, CPOperand cl, CPOperand cu,
		CPOperand out, String opcode, String istr) {
		super(FEDInstruction.FEDType.MatrixIndexing, null, in, out, opcode, istr);
		rowLower = rl;
		rowUpper = ru;
		colLower = cl;
		colUpper = cu;
	}

	protected IndexingFEDInstruction(CPOperand lhsInput, CPOperand rhsInput, CPOperand rl, CPOperand ru, CPOperand cl,
		CPOperand cu, CPOperand out, String opcode, String istr) {
		super(FEDInstruction.FEDType.MatrixIndexing, null, lhsInput, rhsInput, out, opcode, istr);
		rowLower = rl;
		rowUpper = ru;
		colLower = cl;
		colUpper = cu;
	}

	protected IndexRange getIndexRange(ExecutionContext ec) {
		return new IndexRange( // rl, ru, cl, ru
			(int) (ec.getScalarInput(rowLower).getLongValue() - 1),
			(int) (ec.getScalarInput(rowUpper).getLongValue() - 1),
			(int) (ec.getScalarInput(colLower).getLongValue() - 1),
			(int) (ec.getScalarInput(colUpper).getLongValue() - 1));
	}

	public static IndexingFEDInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if(opcode.equalsIgnoreCase(RightIndex.OPCODE)) {
			if(parts.length == 7) {
				CPOperand in, rl, ru, cl, cu, out;
				in = new CPOperand(parts[1]);
				rl = new CPOperand(parts[2]);
				ru = new CPOperand(parts[3]);
				cl = new CPOperand(parts[4]);
				cu = new CPOperand(parts[5]);
				out = new CPOperand(parts[6]);

				if(in.getDataType() != Types.DataType.MATRIX && in.getDataType() != Types.DataType.FRAME)
					throw new DMLRuntimeException("Can index only on matrices, frames in federated.");

				return new IndexingFEDInstruction(in, rl, ru, cl, cu, out, opcode, str);
			}
			else {
				throw new DMLRuntimeException("Invalid number of operands in instruction: " + str);
			}
		}
		else if(opcode.equalsIgnoreCase(LeftIndex.OPCODE)) {
			if ( parts.length == 8 ) {
				CPOperand lhsInput, rhsInput, rl, ru, cl, cu, out;
				lhsInput = new CPOperand(parts[1]);
				rhsInput = new CPOperand(parts[2]);
				rl = new CPOperand(parts[3]);
				ru = new CPOperand(parts[4]);
				cl = new CPOperand(parts[5]);
				cu = new CPOperand(parts[6]);
				out = new CPOperand(parts[7]);

				if((lhsInput.getDataType() != Types.DataType.MATRIX && lhsInput.getDataType() != Types.DataType.FRAME) &&
					(rhsInput.getDataType() != Types.DataType.MATRIX && rhsInput.getDataType() != Types.DataType.FRAME))
					throw new DMLRuntimeException("Can index only on matrices, frames, and lists.");

				return new IndexingFEDInstruction(lhsInput, rhsInput, rl, ru, cl, cu, out, opcode, str);
			}
			else {
				throw new DMLRuntimeException("Invalid number of operands in instruction: " + str);
			}
		}
		else {
			throw new DMLRuntimeException("Unknown opcode while parsing a MatrixIndexingFEDInstruction: " + str);
		}
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		if(getOpcode().equalsIgnoreCase(RightIndex.OPCODE))
			rightIndexing(ec);
		else
			leftIndexing(ec);
	}

	private void rightIndexing(ExecutionContext ec)
	{
		//get input and requested index range
		CacheableData<?> in = ec.getCacheableData(input1);
		IndexRange ixrange = getIndexRange(ec);

		//prepare output federation map (copy-on-write)
		FederationMap fedMap = in.getFedMapping().filter(ixrange);

		//modify federated ranges in place
		String[] instStrings = new String[fedMap.getSize()];

		//create new frame schema
		List<Types.ValueType> schema = new ArrayList<>();
		// replace old reshape values for each worker
		int i = 0;
		for(org.apache.commons.lang3.tuple.Pair<FederatedRange, FederatedData> e : fedMap.getMap()) {
			FederatedRange range = e.getKey();
			long rs = range.getBeginDims()[0], re = range.getEndDims()[0],
				cs = range.getBeginDims()[1], ce = range.getEndDims()[1];
			long rsn = (ixrange.rowStart >= rs) ? (ixrange.rowStart - rs) : 0;
			long ren = (ixrange.rowEnd >= rs && ixrange.rowEnd < re) ? (ixrange.rowEnd - rs) : (re - rs - 1);
			long csn = (ixrange.colStart >= cs) ? (ixrange.colStart - cs) : 0;
			long cen = (ixrange.colEnd >= cs && ixrange.colEnd < ce) ? (ixrange.colEnd - cs) : (ce - cs - 1);

			range.setBeginDim(0, Math.max(rs - ixrange.rowStart, 0));
			range.setBeginDim(1, Math.max(cs - ixrange.colStart, 0));
			range.setEndDim(0, (ixrange.rowEnd >= re ? re-ixrange.rowStart : ixrange.rowEnd-ixrange.rowStart + 1));
			range.setEndDim(1, (ixrange.colEnd >= ce ? ce-ixrange.colStart : ixrange.colEnd-ixrange.colStart + 1));

			long[] newIx = new long[]{rsn, ren, csn, cen};

			// change 4 indices in instString
			instStrings[i] = modifyIndices(newIx, 3, 7);
			
			if(input1.isFrame()) {
				//modify frame schema
				if(in.isFederated(FederationMap.FType.ROW))
					schema = Arrays.asList(((FrameObject) in).getSchema((int) csn, (int) cen));
				else
					Collections.addAll(schema, ((FrameObject) in).getSchema((int) csn, (int) cen));
			}
			i++;
		}
		FederatedRequest[] fr1 = FederationUtils.callInstruction(instStrings,
			output, new CPOperand[] {input1}, new long[] {fedMap.getID()});
		fedMap.execute(getTID(), true, fr1, new FederatedRequest[0]);

		if(input1.isFrame()) {
			FrameObject out = ec.getFrameObject(output);
			out.setSchema(schema.toArray(new Types.ValueType[0]));
			out.getDataCharacteristics().setDimension(fedMap.getMaxIndexInRange(0), fedMap.getMaxIndexInRange(1));
			out.setFedMapping(fedMap.copyWithNewID(fr1[0].getID()));
		} else {
			MatrixObject out = ec.getMatrixObject(output);
			out.getDataCharacteristics().set(fedMap.getMaxIndexInRange(0), fedMap.getMaxIndexInRange(1),
				(int) ((MatrixObject)in).getBlocksize());
			out.setFedMapping(fedMap.copyWithNewID(fr1[0].getID()));
		}
	}

	private void leftIndexing(ExecutionContext ec)
	{
		//get input and requested index range
		CacheableData<?> in1 = ec.getCacheableData(input1);
		CacheableData<?> in2 = ec.getCacheableData(input2);
		IndexRange ixrange = getIndexRange(ec);

		//check bounds
		if( ixrange.rowStart < 0 || ixrange.rowStart >= in1.getNumRows() || ixrange.rowEnd >= in1.getNumRows()
			|| ixrange.colStart < 0 || ixrange.colStart >= in1.getNumColumns() || ixrange.colEnd >= in1.getNumColumns() ) {
			throw new DMLRuntimeException("Invalid values for matrix indexing: ["+(ixrange.rowStart+1)+":"+(ixrange.rowEnd+1)+","
				+ (ixrange.colStart+1)+":"+(ixrange.colEnd+1)+"] " + "must be within matrix dimensions ["+in1.getNumRows()+","+in1.getNumColumns()+"].");
		}
		if( (ixrange.rowEnd-ixrange.rowStart+1) != in2.getNumRows() || (ixrange.colEnd-ixrange.colStart+1) != in2.getNumColumns()) {
			throw new DMLRuntimeException("Invalid values for matrix indexing: " +
				"dimensions of the source matrix ["+in2.getNumRows()+"x" + in2.getNumColumns() + "] " +
				"do not match the shape of the matrix specified by indices [" +
				(ixrange.rowStart+1) +":" + (ixrange.rowEnd+1) + ", " + (ixrange.colStart+1) + ":" + (ixrange.colEnd+1) + "].");
		}

		FederationMap fedMap = in1.getFedMapping();

		String[] instStrings = new String[fedMap.getSize()];
		int[][] sliceIxs = new int[fedMap.getSize()][];
		FederatedRange[] ranges = new FederatedRange[fedMap.getSize()];

		// replace old reshape values for each worker
		int i = 0, prev = 0, from = fedMap.getSize();
		for(org.apache.commons.lang3.tuple.Pair<FederatedRange, FederatedData> e : fedMap.getMap()) {
			FederatedRange range = e.getKey();
			long rs = range.getBeginDims()[0], re = range.getEndDims()[0],
				cs = range.getBeginDims()[1], ce = range.getEndDims()[1];
			long rsn = (ixrange.rowStart >= rs) ? (ixrange.rowStart - rs) : 0;
			long ren = (ixrange.rowEnd >= rs && ixrange.rowEnd < re) ? (ixrange.rowEnd - rs) : (re - rs - 1);
			long csn = (ixrange.colStart >= cs) ? (ixrange.colStart - cs) : 0;
			long cen = (ixrange.colEnd >= cs && ixrange.colEnd < ce) ? (ixrange.colEnd - cs) : (ce - cs - 1);

			long[] newIx = new long[]{(int) rsn, (int) ren, (int) csn, (int) cen};

			// find ranges where to apply  leftIndex
			long to;
			if(in1.isFederated(FederationMap.FType.ROW) && (to = (prev + ren - rsn)) >= 0 &&
				to < in2.getNumRows() && ixrange.rowStart <= re) {
				sliceIxs[i] = new int[] { prev, (int) to, 0, (int) in2.getNumColumns()-1};
				prev = (int) (to + 1);

				instStrings[i] = modifyIndices(newIx, 4, 8);
				ranges[i] = range;
				from = Math.min(i, from);
			}
			else if(in1.isFederated(FederationMap.FType.COL) && (to = (prev + cen - csn)) >= 0 &&
				to < in2.getNumColumns() && ixrange.colStart <= ce) {
				sliceIxs[i] = new int[] {0, (int) in2.getNumRows() - 1, prev, (int) to};
				prev = (int) (to + 1);

				instStrings[i] = modifyIndices(newIx, 4, 8);
				ranges[i] = range;
				from = Math.min(i, from);
			}
			else
				// TODO shallow copy, add more advanced update in place for federated
				instStrings[i] = createCopyInstString();

			i++;
		}

		sliceIxs = Arrays.stream(sliceIxs).filter(Objects::nonNull).toArray(int[][] :: new);

		FederatedRequest[] fr1 = fedMap.broadcastSliced(in2, input2.isFrame(), sliceIxs, in1.getUniqueID());
		FederatedRequest[] fr2 = FederationUtils.callInstruction(instStrings, output, new CPOperand[]{input1, input2},
			new long[]{fedMap.getID(), fr1[0].getID()});
		FederatedRequest fr3 = fedMap.cleanup(getTID(), fr1[0].getID());

		//execute federated instruction and cleanup intermediates
		if(sliceIxs.length == fedMap.getSize())
			fedMap.execute(getTID(), true, fr2, fr1, fr3);
		else {
			// get index of cpvar request
			for(i = 0; i < fr2.length; i++)
				if(i < from || i >= from + sliceIxs.length)
					break;
			fedMap.execute(getTID(), true, ranges, (fr2[i]), Arrays.copyOfRange(fr2, from, from + sliceIxs.length), fr1, fr3);
		}

		if(input1.isFrame()) {
			FrameObject out = ec.getFrameObject(output);
			out.setSchema(((FrameObject) in1).getSchema());
			out.getDataCharacteristics().set(in1.getDataCharacteristics());
			out.setFedMapping(fedMap.copyWithNewID(fr2[0].getID()));
		} else {
			MatrixObject out = ec.getMatrixObject(output);
			out.getDataCharacteristics().set(in1.getDataCharacteristics());;
			out.setFedMapping(fedMap.copyWithNewID(fr2[0].getID()));
		}
	}

	private String modifyIndices(long[] newIx, int from, int to) {
		// change 4 indices in instString
		String[] instParts = instString.split(Lop.OPERAND_DELIMITOR);
		for(int j = from; j < to; j++)
			instParts[j] = InstructionUtils.createLiteralOperand(String.valueOf(newIx[j-from]+1), ValueType.INT64);
		return String.join(Lop.OPERAND_DELIMITOR, instParts);
	}

	private String createCopyInstString() {
		String[] instParts = instString.split(Lop.OPERAND_DELIMITOR);
		return VariableCPInstruction.prepareCopyInstruction(instParts[2], instParts[8]).toString();
	}
}
