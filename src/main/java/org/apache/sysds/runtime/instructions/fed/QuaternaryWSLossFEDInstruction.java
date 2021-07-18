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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.AlignType;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.QuaternaryOperator;

import java.util.ArrayList;
import java.util.concurrent.Future;

public class QuaternaryWSLossFEDInstruction extends QuaternaryFEDInstruction {

	/**
	 * This Instruction performs a Weighted Sigmoid Loss function as follows:
	 *
	 * Z = sum(W * (X - (U %*% t(V))) ^ 2)
	 *
	 * @param operator Weighted Sigmoid Loss
	 * @param in1 X
	 * @param in2 U
	 * @param in3 V
	 * @param in4 W
	 * @param out Z
	 * @param opcode
	 * @param instruction_str
	 */
	protected QuaternaryWSLossFEDInstruction(Operator operator, CPOperand in1, CPOperand in2, CPOperand in3,
		CPOperand in4, CPOperand out, String opcode, String instruction_str) {
		super(FEDType.Quaternary, operator, in1, in2, in3, in4, out, opcode, instruction_str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		QuaternaryOperator qop = (QuaternaryOperator) _optr;

		MatrixObject X = ec.getMatrixObject(input1);
		MatrixObject U = ec.getMatrixObject(input2);
		MatrixObject V = ec.getMatrixObject(input3);

		MatrixObject W = null;
		if(qop.hasFourInputs()) {
			W = ec.getMatrixObject(_input4);
		}

		if(X.isFederated()) {
			FederationMap fedMap = X.getFedMapping();
			ArrayList<FederatedRequest[]> frSliced = new ArrayList<>(); // FederatedRequests of broadcastSliced
			FederatedRequest frB = null; // FederatedRequest for broadcast
			long[] varNewIn = new long[qop.hasFourInputs() ? 4 : 3];
			varNewIn[0] = fedMap.getID();

			if(X.isFederated(FType.ROW)) { // row partitined X
				if(U.isFederated(FType.ROW) && fedMap.isAligned(U.getFedMapping(), AlignType.ROW)) {
					// U federated and aligned
					varNewIn[1] = U.getFedMapping().getID();
				}
				else {
					FederatedRequest[] tmpFrS = fedMap.broadcastSliced(U, false, X.getUniqueID());
					varNewIn[1] = tmpFrS[0].getID();
					frSliced.add(tmpFrS);
				}
				frB = fedMap.broadcast(V, X.getUniqueID());
				varNewIn[2] = frB.getID();
			}
			else if(X.isFederated(FType.COL)) { // col partitioned X
				frB = fedMap.broadcast(U, X.getUniqueID());
				varNewIn[1] = frB.getID();
				if(V.isFederated() && fedMap.isAligned(V.getFedMapping(), AlignType.COL, AlignType.COL_T)) {
					// V federated and aligned
					varNewIn[2] = V.getFedMapping().getID();
				}
				else {
					FederatedRequest[] tmpFrS = fedMap.broadcastSliced(V, true, X.getUniqueID());
					varNewIn[2] = tmpFrS[0].getID();
					frSliced.add(tmpFrS);
				}
			}
			else {
				throw new DMLRuntimeException("Federated WSLoss only supported for ROW or COLUMN partitioned "
					+ "federated data.");
			}

			// broadcast matrix W if there is a fourth input
			if(W != null) {
				if(W.isFederated() && fedMap.isAligned(W.getFedMapping(), AlignType.FULL)) {
					// W federated and aligned
					varNewIn[3] = W.getFedMapping().getID();
				}
				else {
					FederatedRequest[] tmpFrS = fedMap.broadcastSliced(W, false, X.getUniqueID());
					varNewIn[3] = tmpFrS[0].getID();
					frSliced.add(tmpFrS);
				}
			}

			FederatedRequest frComp = FederationUtils.callInstruction(instString, output,
				qop.hasFourInputs() ? new CPOperand[] {input1, input2, input3, _input4}
				: new CPOperand[]{input1, input2, input3}, varNewIn);

			// get partial results from federated workers
			FederatedRequest frGet = new FederatedRequest(RequestType.GET_VAR, frComp.getID());

			ArrayList<FederatedRequest> frC = new ArrayList<>();
			frC.add(fedMap.cleanup(getTID(), frComp.getID()));
			for(FederatedRequest[] frS : frSliced)
				frC.add(fedMap.cleanup(getTID(), frS[0].getID()));
			frC.add(fedMap.cleanup(getTID(), frB.getID()));

			FederatedRequest[] frAll = ArrayUtils.addAll(new FederatedRequest[]{frB, frComp, frGet},
				frC.toArray(new FederatedRequest[0]));

			// execute federated instructions
			Future<FederatedResponse>[] response = frSliced.isEmpty() ?
				fedMap.execute(getTID(), true, frAll) : fedMap.executeMultipleSlices(
					getTID(), true, frSliced.toArray(new FederatedRequest[0][]), frAll);

			// aggregate partial results from federated responses
			AggregateUnaryOperator aop = InstructionUtils.parseBasicAggregateUnaryOperator("uak+");
			ec.setVariable(output.getName(), FederationUtils.aggScalar(aop, response));
		}
		else {
			throw new DMLRuntimeException("Unsupported federated inputs (X, U, V, W) = (" + X.isFederated() + ", "
				+ U.isFederated() + ", " + V.isFederated() + ", " + (W != null ? W.isFederated() : "none") + ")");
		}
	}
}
