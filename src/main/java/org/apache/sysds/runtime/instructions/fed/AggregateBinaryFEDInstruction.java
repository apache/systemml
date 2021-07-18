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

import java.util.concurrent.Future;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRange;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.AlignType;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;

public class AggregateBinaryFEDInstruction extends BinaryFEDInstruction {
	// private static final Log LOG = LogFactory.getLog(AggregateBinaryFEDInstruction.class.getName());
	
	public AggregateBinaryFEDInstruction(Operator op, CPOperand in1,
		CPOperand in2, CPOperand out, String opcode, String istr) {
		super(FEDType.AggregateBinary, op, in1, in2, out, opcode, istr);
	}

	public AggregateBinaryFEDInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out,
		String opcode, String istr, FederatedOutput fedOut) {
		super(FEDType.AggregateBinary, op, in1, in2, out, opcode, istr, fedOut);
	}
	
	public static AggregateBinaryFEDInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		if(!opcode.equalsIgnoreCase("ba+*"))
			throw new DMLRuntimeException("AggregateBinaryInstruction.parseInstruction():: Unknown opcode " + opcode);
		
		InstructionUtils.checkNumFields(parts, 5);
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[3]);
		int k = Integer.parseInt(parts[4]);
		FederatedOutput fedOut = FederatedOutput.valueOf(parts[5]);
		return new AggregateBinaryFEDInstruction(
			InstructionUtils.getMatMultOperator(k), in1, in2, out, opcode, str, fedOut);
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = ec.getMatrixObject(input1);
		MatrixObject mo2 = ec.getMatrixObject(input2);

		//TODO cleanup unnecessary redundancy
		//#1 federated matrix-vector multiplication
		if(mo1.isFederated(FType.COL) && mo2.isFederated(FType.ROW)
			&& mo1.getFedMapping().isAligned(mo2.getFedMapping(), AlignType.COL_T) ) {
			FederatedRequest fr1 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2},
				new long[]{mo1.getFedMapping().getID(), mo2.getFedMapping().getID()}, true);

			if ( _fedOut.isForcedFederated() ){
				mo1.getFedMapping().execute(getTID(), fr1);
				setPartialOutput(mo1.getFedMapping(), mo1, mo2, fr1.getID(), ec);
			}
			else {
				FederatedRequest fr2 = new FederatedRequest(RequestType.GET_VAR, fr1.getID());
				FederatedRequest fr3 = mo2.getFedMapping().cleanup(getTID(), fr1.getID(), fr2.getID());
				//execute federated operations and aggregate
				Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3);
				MatrixBlock ret = FederationUtils.aggAdd(tmp);
				ec.setMatrixOutput(output.getName(), ret);
			}
		}
		else if(mo1.isFederated(FType.ROW) || mo1.isFederated(FType.PART)) { // MV + MM
			//construct commands: broadcast rhs, fed mv, retrieve results
			FederatedRequest fr1 = mo1.getFedMapping().broadcast(mo2, mo1.getUniqueID());
			FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2},
				new long[]{mo1.getFedMapping().getID(), fr1.getID()}, true);
			if( mo2.getNumColumns() == 1 && mo2.getNumRows() != mo1.getNumColumns()) { //MV
				if ( _fedOut.isForcedFederated() ){
					FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1.getID());
					mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3);
					if ( mo1.isFederated(FType.PART) )
						setPartialOutput(mo1.getFedMapping(), mo1, mo2, fr2.getID(), ec);
					else
						setOutputFedMapping(mo1.getFedMapping(), mo1, mo2, fr2.getID(), ec);
				}
				else {
					FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
					FederatedRequest fr4 = mo1.getFedMapping().cleanup(getTID(), fr1.getID(), fr2.getID());
					//execute federated operations and aggregate
					Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3, fr4);
					MatrixBlock ret;
					if ( mo1.isFederated(FType.PART) )
						ret = FederationUtils.aggAdd(tmp);
					else
						ret = FederationUtils.bind(tmp, false);
					ec.setMatrixOutput(output.getName(), ret);
				}
			}
			else { //MM
				//execute federated operations and aggregate
				if ( !_fedOut.isForcedLocal() ){
					FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1.getID());
					mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);
					if ( mo1.isFederated(FType.PART) || mo2.isFederated(FType.PART) )
						setPartialOutput(mo1.getFedMapping(), mo1, mo2, fr2.getID(), ec);
					else
						setOutputFedMapping(mo1.getFedMapping(), mo1, mo2, fr2.getID(), ec);
				}
				else {
					FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
					FederatedRequest fr4 = mo1.getFedMapping().cleanup(getTID(), fr1.getID(), fr2.getID());
					//execute federated operations and aggregate
					Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3, fr4);
					MatrixBlock ret;
					if ( mo1.isFederated(FType.PART) )
						ret = FederationUtils.aggAdd(tmp);
					else
						ret = FederationUtils.bind(tmp, false);
					ec.setMatrixOutput(output.getName(), ret);
				}
			}
		}
		//#2 vector - federated matrix multiplication
		else if (mo2.isFederated(FType.ROW)) {// VM + MM
			if ( mo1.isFederated(FType.COL) && isAggBinaryFedAligned(mo1,mo2) ){
				FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
					new CPOperand[]{input1, input2},
					new long[]{mo1.getFedMapping().getID(), mo2.getFedMapping().getID()}, true);
				if ( _fedOut.isForcedFederated() ){
					// Partial aggregates (set fedmapping to the partial aggs)
					mo2.getFedMapping().execute(getTID(), true, fr2);
					setPartialOutput(mo2.getFedMapping(), mo1, mo2, fr2.getID(), ec);
				}
				else {
					FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
					//execute federated operations and aggregate
					Future<FederatedResponse>[] tmp = mo2.getFedMapping().execute(getTID(), fr2, fr3);
					MatrixBlock ret = FederationUtils.aggAdd(tmp);
					ec.setMatrixOutput(output.getName(), ret);
				}
			}
			else {
				//construct commands: broadcast rhs, fed mv, retrieve results
				FederatedRequest[] fr1 = mo2.getFedMapping().broadcastSliced(mo1, true, mo1.getUniqueID());
				FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
					new CPOperand[]{input1, input2},
					new long[]{fr1[0].getID(), mo2.getFedMapping().getID()}, true);
				if ( _fedOut.isForcedFederated() ){
					// Partial aggregates (set fedmapping to the partial aggs)
					FederatedRequest fr3 = mo2.getFedMapping().cleanup(getTID(), fr1[0].getID());
					mo2.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);
					setPartialOutput(mo2.getFedMapping(), mo1, mo2, fr2.getID(), ec);
				}
				else {
					FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
					FederatedRequest fr4 = mo2.getFedMapping().cleanup(getTID(), fr1[0].getID(), fr2.getID());
					//execute federated operations and aggregate
					Future<FederatedResponse>[] tmp = mo2.getFedMapping().execute(getTID(), fr1, fr2, fr3, fr4);
					MatrixBlock ret = FederationUtils.aggAdd(tmp);
					ec.setMatrixOutput(output.getName(), ret);
				}
			}
		}
		//#3 col-federated matrix vector multiplication
		else if (mo1.isFederated(FType.COL)) {// VM + MM
			//construct commands: broadcast rhs, fed mv, retrieve results
			FederatedRequest[] fr1 = mo1.getFedMapping().broadcastSliced(mo2, true, mo1.getUniqueID());
			FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2},
				new long[]{mo1.getFedMapping().getID(), fr1[0].getID()}, true);
			if ( _fedOut.isForcedFederated() ){
				// Partial aggregates (set fedmapping to the partial aggs)
				FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1[0].getID());
				mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);
				setPartialOutput(mo1.getFedMapping(), mo1, mo2, fr2.getID(), ec);
			}
			else {
				FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
				FederatedRequest fr4 = mo1.getFedMapping().cleanup(getTID(), fr1[0].getID(), fr2.getID());
				//execute federated operations and aggregate
				Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3, fr4);
				MatrixBlock ret = FederationUtils.aggAdd(tmp);
				ec.setMatrixOutput(output.getName(), ret);
			}
		}
		else { //other combinations
			throw new DMLRuntimeException("Federated AggregateBinary not supported with the "
				+ "following federated objects: "+mo1.isFederated()+":"+mo1.getFedMapping()
				+" "+mo2.isFederated()+":"+mo2.getFedMapping());
		}
	}

	/**
	 * Checks alignment of dimensions for the federated aggregate binary processing without broadcast.
	 * If the begin and end ranges of mo1 has cols equal to the rows of the begin and end ranges of mo2,
	 * the two inputs are aligned for the processing of the federated aggregate binary instruction without broadcasting.
	 * @param mo1 input matrix object 1
	 * @param mo2 input matrix object 2
	 * @return true if the two inputs are aligned for aggregate binary processing without broadcasting
	 */
	private static boolean isAggBinaryFedAligned(MatrixObject mo1, MatrixObject mo2){
		FederatedRange[] mo1FederatedRanges = mo1.getFedMapping().getFederatedRanges();
		FederatedRange[] mo2FederatedRanges = mo2.getFedMapping().getFederatedRanges();
		for ( int i = 0; i < mo1FederatedRanges.length; i++ ){
			FederatedRange mo1FedRange = mo1FederatedRanges[i];
			FederatedRange mo2FedRange = mo2FederatedRanges[i];

			if ( mo1FedRange.getBeginDims()[1] != mo2FedRange.getBeginDims()[0]
				|| mo1FedRange.getEndDims()[1] != mo2FedRange.getEndDims()[0])
				return false;
		}
		return true;
	}

	/**
	 * Sets the output with a federated mapping of overlapping partial aggregates.
	 * @param federationMap federated map from which the federated metadata is retrieved
	 * @param mo1 matrix object with number of rows used to set the number of rows of the output
	 * @param mo2 matrix object with number of columns used to set the number of columns of the output
	 * @param outputID ID of the output
	 * @param ec execution context
	 */
	private void setPartialOutput(FederationMap federationMap, MatrixObject mo1, MatrixObject mo2,
		long outputID, ExecutionContext ec){
		MatrixObject out = ec.getMatrixObject(output);
		out.getDataCharacteristics().set(mo1.getNumRows(), mo2.getNumColumns(), (int)mo1.getBlocksize());
		FederationMap outputFedMap = federationMap
			.copyWithNewIDAndRange(mo1.getNumRows(), mo2.getNumColumns(), outputID);
		out.setFedMapping(outputFedMap);
	}

	/**
	 * Sets the output with a federated map copied from federationMap input given.
	 * @param federationMap federation map to be set in output
	 * @param mo1 matrix object with number of rows used to set the number of rows of the output
	 * @param mo2 matrix object with number of columns used to set the number of columns of the output
	 * @param outputID ID of the output
	 * @param ec execution context
	 */
	private void setOutputFedMapping(FederationMap federationMap, MatrixObject mo1, MatrixObject mo2,
		long outputID, ExecutionContext ec){
		MatrixObject out = ec.getMatrixObject(output);
		out.getDataCharacteristics().set(mo1.getNumRows(), mo2.getNumColumns(), (int)mo1.getBlocksize());
		out.setFedMapping(federationMap.copyWithNewID(outputID, mo2.getNumColumns()));
	}
}
