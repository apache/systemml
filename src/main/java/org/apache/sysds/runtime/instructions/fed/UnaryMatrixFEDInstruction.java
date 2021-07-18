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
import java.util.concurrent.Future;

import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.functionobjects.Builtin;
import org.apache.sysds.runtime.functionobjects.ValueFunction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.data.LibCommonsMath;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.UnaryOperator;

public class UnaryMatrixFEDInstruction extends UnaryFEDInstruction {

	protected UnaryMatrixFEDInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String instr) {
		super(FEDType.Unary, op, in, out, opcode, instr);
	}
	
	public static boolean isValidOpcode(String opcode) {
		return !LibCommonsMath.isSupportedUnaryOperation(opcode);
	}

	public static UnaryMatrixFEDInstruction parseInstruction(String str) {
		CPOperand in = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);

		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		
		if(parts.length == 5 && (opcode.equalsIgnoreCase("exp") || opcode.equalsIgnoreCase("log") || opcode.startsWith("ucum"))) {
			in.split(parts[1]);
			out.split(parts[2]);
			ValueFunction func = Builtin.getBuiltinFnObject(opcode);
			if( Arrays.asList(new String[]{"ucumk+","ucum*","ucumk+*","ucummin","ucummax","exp","log","sigmoid"}).contains(opcode) ){
				UnaryOperator op = new UnaryOperator(func,Integer.parseInt(parts[3]),Boolean.parseBoolean(parts[4]));
				return new UnaryMatrixFEDInstruction(op, in, out, opcode, str);
			}
			else
				return new UnaryMatrixFEDInstruction(null, in, out, opcode, str);
		}
		opcode = parseUnaryInstruction(str, in, out);
		return new UnaryMatrixFEDInstruction(InstructionUtils.parseUnaryOperator(opcode), in, out, opcode, str);
	}
	
	@Override 
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = ec.getMatrixObject(input1);
		if(getOpcode().startsWith("ucum") && mo1.isFederated(FederationMap.FType.ROW))
			processCumulativeInstruction(ec, mo1);
		else {
			//federated execution on arbitrary row/column partitions
			//(only assumption for sparse-unsafe: fed mapping covers entire matrix)
			FederatedRequest fr1 = FederationUtils.callInstruction(instString, output,
				new CPOperand[] {input1}, new long[] {mo1.getFedMapping().getID()});
			mo1.getFedMapping().execute(getTID(), true, fr1);

			setOutputFedMapping(ec, mo1, fr1.getID());
		}
	}

	public void processCumulativeInstruction(ExecutionContext ec, MatrixObject mo1) {
		String opcode = getOpcode();
		MatrixObject out;
		if(opcode.equalsIgnoreCase("ucumk+*")) {
			FederatedRequest fr1 = FederationUtils.callInstruction(instString, output,
				new CPOperand[] {input1}, new long[] {mo1.getFedMapping().getID()});
			FederatedRequest fr2 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fr1.getID());
			Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), true, fr1, fr2);
			out = setOutputFedMapping(ec, mo1, fr1.getID());

			MatrixBlock scalingValues = getScalars(mo1, tmp);
			setScalingValues(ec, mo1, out, scalingValues);
		}
		else {
			String colAgg = opcode.replace("ucum", "uac");
			String agg2 = opcode.replace(opcode.contains("ucumk")? "ucumk" :"ucum", "");

			double init = opcode.equalsIgnoreCase("ucumk+") ? 0.0:
				opcode.equalsIgnoreCase("ucum*") ? 1.0 :
				opcode.equalsIgnoreCase("ucummin") ? Double.MAX_VALUE : -Double.MAX_VALUE;

			Future<FederatedResponse>[] tmp = modifyAndGetInstruction(colAgg, mo1);
			MatrixBlock scalingValues = getResultBlock(tmp, (int)mo1.getNumColumns(), opcode, init);

			out = ec.getMatrixObject(output);
			setScalingValues(agg2, ec, mo1, out, scalingValues, init);
		}
		processCumulative(out);
	}

	private Future<FederatedResponse>[] modifyAndGetInstruction(String newInst, MatrixObject mo1) {
		String modifiedInstString = InstructionUtils.replaceOperand(instString, 1, newInst);

		FederatedRequest fr1 = FederationUtils.callInstruction(modifiedInstString, output,
			new CPOperand[] {input1}, new long[] {mo1.getFedMapping().getID()});
		FederatedRequest fr2 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fr1.getID());
		return mo1.getFedMapping().execute(getTID(), true, fr1, fr2);
	}

	private void processCumulative(MatrixObject out) {
		String modifiedInstString = InstructionUtils.replaceOperand(instString, 2, InstructionUtils.createOperand(output));

		FederatedRequest fr4 = FederationUtils.callInstruction(modifiedInstString, output, out.getFedMapping().getID(),
			new CPOperand[] {output}, new long[] {out.getFedMapping().getID()});
		out.getFedMapping().execute(getTID(), true, fr4);

		out.setFedMapping(out.getFedMapping().copyWithNewID(fr4.getID()));

		// modify fed ranges since ucumk+* output is always nx1
		if(getOpcode().equalsIgnoreCase("ucumk+*")) {
			out.getDataCharacteristics().set(out.getNumRows(), 1L, (int) out.getBlocksize());
			for(int i = 0; i < out.getFedMapping().getFederatedRanges().length; i++)
				out.getFedMapping().getFederatedRanges()[i].setEndDim(1, 1);
		} else {
			out.getDataCharacteristics().set(out.getNumRows(), out.getNumColumns(), (int) out.getBlocksize());
		}
	}

	private static MatrixBlock getResultBlock(Future<FederatedResponse>[] tmp, int cols, String opcode, double init) {
		//TODO perf simple rbind, as the first row (init) is anyway not transferred
		
		//collect row vectors into local matrix
		MatrixBlock res = new MatrixBlock(tmp.length, cols, init);
		for(int i = 0; i < tmp.length-1; i++)
			try {
				res.copy(i+1, i+1, 0, cols-1, ((MatrixBlock) tmp[i].get().getData()[0]), true);
			}
			catch(Exception e) {
				throw new DMLRuntimeException("Federated Get data failed with exception on UnaryMatrixFEDInstruction", e);
			}

		//local cumulative aggregate
		return res.unaryOperations(
			new UnaryOperator(Builtin.getBuiltinFnObject(opcode)),
			new MatrixBlock());
	}

	private MatrixBlock getScalars(MatrixObject mo1, Future<FederatedResponse>[] tmp) {
		MatrixBlock[] aggRes = getAggMatrices(mo1);
		MatrixBlock prod = aggRes[0];
		MatrixBlock firstValues = aggRes[1];
		for(int i = 0; i < tmp.length; i++)
			try {
				MatrixBlock curr = ((MatrixBlock) tmp[i].get().getData()[0]);
				prod.setValue(i, 0, curr.getValue(curr.getNumRows()-1, 0));
			}
			catch(Exception e) {
				throw new DMLRuntimeException("Federated Get data failed with exception on UnaryMatrixFEDInstruction", e);
			}

		// aggregate sumprod to get scalars
		MatrixBlock a = new MatrixBlock(tmp.length, 1, 0.0);
		a.copy(1, a.getNumRows()-1, 0, 0,
			prod.unaryOperations(new UnaryOperator(Builtin.getBuiltinFnObject("ucumk+*")), new MatrixBlock())
				.slice(0, prod.getNumRows()-2), true);

		// compute  B11 = B11 + B12 ⊙ a
		MatrixBlock B = firstValues.slice(0, firstValues.getNumRows()-1,1, 1)
			.binaryOperations(InstructionUtils.parseBinaryOperator("*"), a, new MatrixBlock());
		return B.binaryOperationsInPlace(InstructionUtils.parseBinaryOperator("+"), firstValues.slice(0,firstValues.getNumRows()-1,0,0));
	}

	private MatrixBlock[] getAggMatrices(MatrixObject mo1) {
		Future<FederatedResponse>[] tmp = modifyAndGetInstruction("ucum*", mo1);

		// slice and return prod and first value
		MatrixBlock prod = new MatrixBlock(tmp.length, 2, 0.0);
		MatrixBlock firstValues = new MatrixBlock(tmp.length, 2, 0.0);
		for(int i = 0; i < tmp.length; i++)
			try {
				MatrixBlock curr = ((MatrixBlock) tmp[i].get().getData()[0]);
				prod.setValue(i, 1, curr.getValue(curr.getNumRows()-1, 1));
				firstValues.copy(i, i, 0,1, curr.slice(0, 0), true);
			}
			catch(Exception e) {
				throw new DMLRuntimeException("Federated Get data failed with exception on UnaryMatrixFEDInstruction", e);
			}
		return new MatrixBlock[] {prod, firstValues};
	}

	private void setScalingValues(ExecutionContext ec, MatrixObject mo1, MatrixObject out, MatrixBlock scalingValues) {
		MatrixBlock condition = new MatrixBlock((int) mo1.getNumRows(), (int) mo1.getNumColumns(), 1.0);
		MatrixBlock mb2 = new MatrixBlock((int) mo1.getNumRows(), (int) mo1.getNumColumns(), 0.0);

		for(int i = 0; i < scalingValues.getNumRows()-1; i++) {
			int step = (int) mo1.getFedMapping().getFederatedRanges()[i + 1].getBeginDims()[0];
			condition.setValue(step, 0, 0.0);
			mb2.setValue(step, 0, scalingValues.getValue(i + 1, 0));
		}

		MatrixObject cond = ExecutionContext.createMatrixObject(condition);
		long condID = FederationUtils.getNextFedDataID();
		ec.setVariable(String.valueOf(condID), cond);

		MatrixObject mo2 = ExecutionContext.createMatrixObject(mb2);
		long varID2 = FederationUtils.getNextFedDataID();
		ec.setVariable(String.valueOf(varID2), mo2);

		CPOperand opCond = new CPOperand(String.valueOf(condID), ValueType.FP64, DataType.MATRIX);
		CPOperand op2 = new CPOperand(String.valueOf(varID2), ValueType.FP64, DataType.MATRIX);

		String ternaryInstString = InstructionUtils.constructTernaryString(instString, opCond, input1, op2, output);

		FederatedRequest[] fr1 = mo1.getFedMapping().broadcastSliced(cond, false, mo1.getUniqueID());
		FederatedRequest[] fr2 = mo1.getFedMapping().broadcastSliced(mo2, false, mo1.getUniqueID());
		FederatedRequest fr3 = FederationUtils.callInstruction(ternaryInstString, output,
			new CPOperand[] {input1, opCond, op2}, new long[] {mo1.getFedMapping().getID(), fr1[0].getID(), fr2[0].getID()});
		//TODO perf no need to execute here, we can piggyback the requests onto the final cumagg
		mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);

		out.setFedMapping(mo1.getFedMapping().copyWithNewID(fr3.getID()));

		ec.removeVariable(opCond.getName());
		ec.removeVariable(op2.getName());
	}

	private void setScalingValues(String opcode, ExecutionContext ec, MatrixObject mo1, MatrixObject out, MatrixBlock scalingValues, double init) {
		//TODO perf improvement (currently this creates a sliced broadcast in the size of the original matrix
		//but sparse w/ strategically placed offsets, but would need to be dense for dense prod/cumsum)
		
		//allocated large matrix of init value and placed offset rows in first row of every partition
		MatrixBlock mb2 = new MatrixBlock((int) mo1.getNumRows(), (int) mo1.getNumColumns(), init);
		for(int i = 1; i < scalingValues.getNumRows(); i++) {
			int step = (int) mo1.getFedMapping().getFederatedRanges()[i].getBeginDims()[0];
			mb2.copy(step, step, 0, (int)(mo1.getNumColumns()-1), scalingValues.slice(i, i), true);
		}

		MatrixObject mo2 = ExecutionContext.createMatrixObject(mb2);
		long varID2 = FederationUtils.getNextFedDataID();
		ec.setVariable(String.valueOf(varID2), mo2);
		CPOperand op2 = new CPOperand(String.valueOf(varID2), ValueType.FP64, DataType.MATRIX);

		String modifiedInstString = InstructionUtils.constructBinaryInstString(instString, opcode, input1, op2, output);

		FederatedRequest[] fr1 = mo1.getFedMapping().broadcastSliced(mo2, false, mo1.getUniqueID());
		FederatedRequest fr2 = FederationUtils.callInstruction(modifiedInstString, output,
			new CPOperand[] {input1, op2}, new long[] {mo1.getFedMapping().getID(), fr1[0].getID()});
		mo1.getFedMapping().execute(getTID(), true, fr1, fr2);

		out.setFedMapping(mo1.getFedMapping().copyWithNewID(fr2.getID()));

		ec.removeVariable(op2.getName());
	}

	private MatrixObject setOutputFedMapping(ExecutionContext ec, MatrixObject fedMapObj, long fedOutputID) {
		MatrixObject out = ec.getMatrixObject(output);
		out.getDataCharacteristics().set(fedMapObj.getDataCharacteristics());
		out.setFedMapping(fedMapObj.getFedMapping().copyWithNewID(fedOutputID));
		return out;
	}
}
