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

import java.util.Objects;
import java.util.concurrent.Future;

import com.sun.tools.javac.util.List;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.operators.TernaryOperator;

public class TernaryFEDInstruction extends ComputationFEDInstruction {

	private TernaryFEDInstruction(TernaryOperator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
		String opcode, String str, FederatedOutput fedOut) {
		super(FEDInstruction.FEDType.Ternary, op, in1, in2, in3, out, opcode, str, fedOut);
	}

	public static TernaryFEDInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		CPOperand operand1 = new CPOperand(parts[1]);
		CPOperand operand2 = new CPOperand(parts[2]);
		CPOperand operand3 = new CPOperand(parts[3]);
		CPOperand outOperand = new CPOperand(parts[4]);
		int numThreads = parts.length>5 ? Integer.parseInt(parts[5]) : 1;
		FederatedOutput fedOut = parts.length>7 ? FederatedOutput.valueOf(parts[6]) : FederatedOutput.NONE;
		TernaryOperator op = InstructionUtils.parseTernaryOperator(opcode, numThreads);
		return new TernaryFEDInstruction(op, operand1, operand2, operand3, outOperand, opcode, str, fedOut);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = input1.isMatrix() ? ec.getMatrixObject(input1.getName()) : null;
		MatrixObject mo2 = input2.isMatrix() ? ec.getMatrixObject(input2.getName()) : null;
		MatrixObject mo3 = input3 != null && input3.isMatrix() ? ec.getMatrixObject(input3.getName()) : null;

		long matrixInputsCount = List.of(mo1, mo2, mo3).stream().filter(Objects::nonNull).count();

		if(matrixInputsCount == 3)
			processMatrixInput(ec, mo1, mo2, mo3);
		else if(matrixInputsCount == 1) {
			CPOperand in = mo1 == null ? mo2 == null ? input3 : input2 : input1;
			mo1 = mo1 == null ? mo2 == null ? mo3 : mo2 : mo1;
			processMatrixScalarInput(ec, mo1, in);
		}
		else {
			if(mo1 != null && mo2 != null) {
				if(input3 != null && !input3.isLiteral())
					instString = InstructionUtils.replaceOperand(instString, 4,
						InstructionUtils.createLiteralOperand(ec.getScalarInput(input3).getStringValue(), Types.ValueType.FP64));
				process2MatrixScalarInput(ec, mo1, mo2, input1, input2);
			}
			else if(mo2 != null && mo3 != null) {
				if(!input1.isLiteral())
					instString = InstructionUtils.replaceOperand(instString, 2,
						InstructionUtils.createLiteralOperand(ec.getScalarInput(input1).getStringValue(), Types.ValueType.FP64));
				process2MatrixScalarInput(ec, mo2, mo3, input2, input3);
			}
			else if(mo1 != null && mo3 != null) {
				if(!input2.isLiteral())
					instString = InstructionUtils.replaceOperand(instString, 3,
						InstructionUtils.createLiteralOperand(ec.getScalarInput(input2).getStringValue(), Types.ValueType.FP64));
				process2MatrixScalarInput(ec, mo1, mo3, input1, input3);
			}
		}
	}

	private void processMatrixScalarInput(ExecutionContext ec, MatrixObject mo1, CPOperand in) {
		FederatedRequest fr1 = FederationUtils.callInstruction(instString, output, new CPOperand[] {in}, new long[] {mo1.getFedMapping().getID()});
		sendFederatedRequests(ec, mo1, fr1.getID(), fr1);
	}

	private void process2MatrixScalarInput(ExecutionContext ec, MatrixObject mo1, MatrixObject mo2, CPOperand in1, CPOperand in2) {
		FederatedRequest[] fr1 = null;
		CPOperand[] varOldIn;
		boolean cleanupIn = true;
		long[] varNewIn;
		varOldIn = new CPOperand[] {in1, in2};
		if(mo1.isFederated()) {
			if(mo2.isFederated() && mo1.getFedMapping().isAligned(mo2.getFedMapping(), false))
				varNewIn = new long[]{mo1.getFedMapping().getID(), mo2.getFedMapping().getID()};
			else {
				fr1 = mo1.getFedMapping().broadcastSliced(mo2, false, mo1.getUniqueID());
				varNewIn = new long[]{mo1.getFedMapping().getID(), fr1[0].getID()};
			}
		} else {
			cleanupIn = false;
			mo1 = ec.getMatrixObject(in2);
			fr1 = mo1.getFedMapping().broadcastSliced(ec.getMatrixObject(in1), false, mo1.getUniqueID());
			varNewIn = new long[]{fr1[0].getID(), mo1.getFedMapping().getID()};
		}
		FederatedRequest fr2 = FederationUtils.callInstruction(instString, output, varOldIn, varNewIn);

		// 2 aligned inputs
		if(fr1 == null) {
			sendFederatedRequests(ec, mo1, fr2.getID(), fr2);
		} else {
			if(cleanupIn) {
				FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1[0].getID());
				sendFederatedRequests(ec, mo1, fr2.getID(), fr1, fr2, fr3);
			}
			else
				sendFederatedRequests(ec, mo1, fr2.getID(), fr1, fr2);
		}
	}

	/**
	 * Send federated requests and retrieve output if federated output flag is set.
	 * @param ec execution context
	 * @param fedMapObj matrix object with federated mapping where federated requests are sent to.
	 * @param fedOutputID ID of federated output
	 * @param federatedRequests federated requests for processing instruction
	 */
	private void sendFederatedRequests(ExecutionContext ec, MatrixObject fedMapObj, long fedOutputID,
		FederatedRequest... federatedRequests){
		sendFederatedRequests(ec, fedMapObj, fedOutputID, null, null, federatedRequests);
	}

	/**
	 * Send federated requests and retrieve output if federated output flag is set.
	 * @param ec execution context
	 * @param fedMapObj matrix object with federated mapping where federated requests are sent to.
	 * @param fedOutputID ID of federated output
	 * @param federatedSlices federated requests for broadcasting slices before processing instruction
	 * @param federatedRequests federated requests for processing instruction
	 */
	private void sendFederatedRequests(ExecutionContext ec, MatrixObject fedMapObj, long fedOutputID,
		FederatedRequest[] federatedSlices, FederatedRequest... federatedRequests){
		sendFederatedRequests(ec, fedMapObj, fedOutputID, federatedSlices, null, federatedRequests);
	}

	/**
	 * Send federated requests and retrieve output if federated output flag is set.
	 * @param ec execution context
	 * @param fedMapObj matrix object with federated mapping where federated requests are sent to.
	 * @param fedOutputID ID of federated output
	 * @param federatedSlices1 federated requests for broadcasting slices before processing instruction
	 * @param federatedSlices2 federated requests for broadcasting slices before processing instruction
	 * @param federatedRequests federated requests for processing instruction
	 */
	private void sendFederatedRequests(ExecutionContext ec, MatrixObject fedMapObj, long fedOutputID,
		FederatedRequest[] federatedSlices1, FederatedRequest[] federatedSlices2, FederatedRequest... federatedRequests){
		if ( !_fedOut.isForcedLocal() ){
			fedMapObj.getFedMapping().execute(getTID(), true, federatedSlices1, federatedSlices2, federatedRequests);
			setOutputFedMapping(ec, fedMapObj, fedOutputID);
		}
		else
			processAndRetrieve(ec, fedMapObj, fedOutputID, federatedSlices1, federatedSlices2, federatedRequests);
	}

	/**
	 * Process instruction and get output from federated workers.
	 * @param ec execution context
	 * @param fedMapObj matrix object with federated mapping where federated requests are sent to.
	 * @param fedOutputID ID of federated output
	 * @param federatedSlices1 federated requests for broadcasting slices before processing instruction
	 * @param federatedSlices2 federated requests for broadcasting slices before processing instruction
	 * @param federatedRequests federated requests for processing instruction
	 */
	private void processAndRetrieve(ExecutionContext ec, MatrixObject fedMapObj, long fedOutputID,
		FederatedRequest[] federatedSlices1, FederatedRequest[] federatedSlices2, FederatedRequest... federatedRequests){
		FederatedRequest getRequest = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fedOutputID);
		Future<FederatedResponse>[] executionResponse = fedMapObj.getFedMapping().execute(
			getTID(), true, federatedSlices1, federatedSlices2, collectRequests(federatedRequests, getRequest));
		ec.setMatrixOutput(output.getName(), FederationUtils.bind(executionResponse,
			fedMapObj.isFederated(FederationMap.FType.COL)));
	}

	/**
	 * Collect federated requests into a single array of federated requests.
	 * The federated requests are added in the same order as the parameters of this method.
	 * @param fedRequests array of federated requests
	 * @param fedRequest1 federated request to occur after array
	 * @return federated requests collected in a single array
	 */
	private static FederatedRequest[] collectRequests(FederatedRequest[] fedRequests, FederatedRequest fedRequest1){
		FederatedRequest[] allRequests = new FederatedRequest[fedRequests.length + 1];
		for ( int i = 0; i < fedRequests.length; i++ )
			allRequests[i] = fedRequests[i];
		allRequests[allRequests.length-1] = fedRequest1;
		return allRequests;
	}

	private void processMatrixInput(ExecutionContext ec, MatrixObject mo1, MatrixObject mo2, MatrixObject mo3) {

		// check aligned matrices
		RetAlignedValues retAlignedValues = getAlignedInputs(ec, mo1, mo2, mo3);

		FederatedRequest[] fr2;
		FederatedRequest fr3, fr4;

		// all 3 inputs fed aligned on the one worker
		if(retAlignedValues._allAligned) {
			fr3 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2, input3},
				new long[] {mo1.getFedMapping().getID(), mo2.getFedMapping().getID(), mo3.getFedMapping().getID()});
			sendFederatedRequests(ec, mo1, fr3.getID(), fr3);
		}
		// 2 fed aligned inputs
		else if(retAlignedValues._twoAligned) {
			fr3 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2, input3}, retAlignedValues._vars);
			fr4 = mo1.getFedMapping().cleanup(getTID(), retAlignedValues._fr[0].getID());
			sendFederatedRequests(ec, mo1, fr3.getID(), retAlignedValues._fr, fr3, fr4);
		}
		// 1 fed input or not aligned
		else {
			if(!mo1.isFederated())
				if(mo2.isFederated()) {
					mo1 = mo2;
					mo2 = ec.getMatrixObject(input1);
				}
				else {
					mo1 = mo3;
					mo3 = ec.getMatrixObject(input1);
				}

			FederatedRequest[] fr1 = mo1.getFedMapping().broadcastSliced(mo2, false, mo1.getUniqueID());
			fr2 = mo1.getFedMapping().broadcastSliced(mo3, false, mo1.getUniqueID());

			long[] vars = new long[] {mo1.getFedMapping().getID(), fr1[0].getID(), fr2[0].getID()};
			if(!ec.getMatrixObject(input1).isFederated())
				vars = ec.getMatrixObject(input2).isFederated() ? new long[] {fr1[0].getID(), mo1.getFedMapping().getID(), fr2[0].getID()} : new long[] {fr1[0].getID(), fr2[0].getID(),
					mo1.getFedMapping().getID()};

			fr3 = FederationUtils.callInstruction(instString, output, new CPOperand[] {input1, input2, input3}, vars);
			fr4 = mo1.getFedMapping().cleanup(getTID(), fr1[0].getID(), fr2[0].getID());
			sendFederatedRequests(ec, mo1, fr3.getID(), fr1, fr2, fr3, fr4);
		}
	}

	/**
	 * Check alignment of matrices and return aligned federated data.
	 * @param ec execution context
	 * @param mo1 first input matrix
	 * @param mo2 second input matrix
	 * @param mo3 third input matrix
	 * @return aligned federated data
	 */
	private RetAlignedValues getAlignedInputs(ExecutionContext ec, MatrixObject mo1, MatrixObject mo2, MatrixObject mo3) {
		long[] vars = new long[0];
		FederatedRequest[] fr = new FederatedRequest[0];
		boolean twoAligned = false, allAligned = false;
		if(mo1.isFederated() && !mo1.isFederated(FederationMap.FType.BROADCAST) && mo2.isFederated() && mo1.getFedMapping().isAligned(mo2.getFedMapping(), false)) {
			twoAligned = true;
			fr = mo1.getFedMapping().broadcastSliced(mo3, false, mo1.getUniqueID());
			vars = new long[] {mo1.getFedMapping().getID(), mo2.getFedMapping().getID(), fr[0].getID()};
		}
		if(mo1.isFederated() && !mo1.isFederated(FederationMap.FType.BROADCAST) && mo3.isFederated() && mo1.getFedMapping().isAligned(mo3.getFedMapping(), false)) {
			allAligned = twoAligned;
			twoAligned = true;
			fr = mo1.getFedMapping().broadcastSliced(mo2, false, mo1.getUniqueID());
			vars = new long[] {mo1.getFedMapping().getID(), fr[0].getID(), mo3.getFedMapping().getID()};
		}
		if(!mo1.isFederated(FederationMap.FType.BROADCAST) && mo2.isFederated() && mo3.isFederated() && mo2.getFedMapping().isAligned(mo3.getFedMapping(), false) && !allAligned) {
			twoAligned = true;
			mo1 = mo2;
			mo2 = mo3;
			mo3 = ec.getMatrixObject(input1);
			fr = mo1.getFedMapping().broadcastSliced(mo3, false, mo1.getUniqueID());
			vars = new long[] {fr[0].getID(), mo1.getFedMapping().getID(), mo2.getFedMapping().getID()};
		}

		return new RetAlignedValues(twoAligned, allAligned, vars, fr);
	}

	private static final class RetAlignedValues {
		public boolean _twoAligned;
		public boolean _allAligned;
		public long[] _vars;
		public FederatedRequest[] _fr;

		public RetAlignedValues(boolean twoAligned, boolean allAligned, long[] vars, FederatedRequest[] fr) {
			_twoAligned = twoAligned;
			_allAligned = allAligned;
			_vars = vars;
			_fr = fr;
		}
	}

	/**
	 * Set fed mapping of output. The data characteristics are not set.
	 * @param ec execution context
	 * @param fedMapObj federated matrix object from which federated mapping is derived
	 * @param fedOutputID ID for the fed mapping of output
	 */
	private void setOutputFedMapping(ExecutionContext ec, MatrixObject fedMapObj, long fedOutputID) {
		MatrixObject out = ec.getMatrixObject(output);
		out.setFedMapping(fedMapObj.getFedMapping().copyWithNewID(fedOutputID));
	}
}
