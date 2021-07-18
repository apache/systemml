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
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.lops.WeightedDivMM.WDivMMType;
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
import org.apache.sysds.runtime.instructions.cp.DoubleObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.QuaternaryOperator;

import java.util.ArrayList;
import java.util.concurrent.Future;

public class QuaternaryWDivMMFEDInstruction extends QuaternaryFEDInstruction
{
	/**
	 * This instruction performs:
	 *
	 * Z = X * (U %*% t(V));
	 * Z = (X / (U %*% t(V) + eps)) %*% V;
	 * and many more
	 *
	 * @param operator        Weighted Div Matrix Multiplication Federated Instruction.
	 * @param in1             X
	 * @param in2             U
	 * @param in3             V
	 * @param in4             W (=epsilon or MX matrix)
	 * @param out             The Federated Result Z
	 * @param opcode          ...
	 * @param instruction_str ...
	 */
	protected QuaternaryWDivMMFEDInstruction(Operator operator,
		CPOperand in1, CPOperand in2, CPOperand in3, CPOperand in4, CPOperand out, String opcode, String instruction_str)
	{
		super(FEDType.Quaternary, operator, in1, in2, in3, in4, out, opcode, instruction_str);
	}

	@Override
	public void processInstruction(ExecutionContext ec)
	{
		QuaternaryOperator qop = (QuaternaryOperator) _optr;
		final WDivMMType wdivmm_type = qop.wtype3;
		MatrixObject X = ec.getMatrixObject(input1);
		MatrixObject U = ec.getMatrixObject(input2);
		MatrixObject V = ec.getMatrixObject(input3);
		ScalarObject eps = null;
		MatrixObject MX = null;

		if(qop.hasFourInputs()) {
			if(wdivmm_type == WDivMMType.MULT_MINUS_4_LEFT || wdivmm_type == WDivMMType.MULT_MINUS_4_RIGHT) {
				MX = ec.getMatrixObject(_input4);
			}
			else {
				eps = (_input4.getDataType() == DataType.SCALAR) ?
					ec.getScalarInput(_input4) :
					new DoubleObject(ec.getMatrixInput(_input4.getName()).quickGetValue(0, 0));
			}
		}

		if(X.isFederated()) {
			FederationMap fedMap = X.getFedMapping();
			ArrayList<FederatedRequest[]> frSliced = new ArrayList<>();
			ArrayList<FederatedRequest> frB = new ArrayList<>(); // FederatedRequests of broadcasts
			long[] varNewIn = new long[qop.hasFourInputs() ? 4 : 3];
			varNewIn[0] = fedMap.getID();

			if(X.isFederated(FType.ROW)) { // row partitioned X
				if(U.isFederated(FType.ROW) && fedMap.isAligned(U.getFedMapping(), AlignType.ROW)) {
					// U federated and aligned
					varNewIn[1] = U.getFedMapping().getID();
				}
				else {
					FederatedRequest[] tmpFrS = fedMap.broadcastSliced(U, false, X.getUniqueID());
					varNewIn[1] = tmpFrS[0].getID();
					frSliced.add(tmpFrS);
				}
				FederatedRequest tmpFr = fedMap.broadcast(V, X.getUniqueID());
				varNewIn[2] = tmpFr.getID();
				frB.add(tmpFr);
			}
			else if(X.isFederated(FType.COL)) { // col paritioned X
				FederatedRequest tmpFr = fedMap.broadcast(U, X.getUniqueID());
				varNewIn[1] = tmpFr.getID();
				frB.add(tmpFr);
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
				throw new DMLRuntimeException("Federated WDivMM only supported for ROW or COLUMN partitioned "
					+ "federated data.");
			}

			// broadcast matrix MX if there is a fourth matrix input
			if(MX != null) {
				if(MX.isFederated() && fedMap.isAligned(MX.getFedMapping(), AlignType.FULL)) {
					varNewIn[3] = MX.getFedMapping().getID();
				}
				else {
					FederatedRequest[] tmpFrS = fedMap.broadcastSliced(MX, false, X.getUniqueID());
					varNewIn[3] = tmpFrS[0].getID();
					frSliced.add(tmpFrS);
				}
			}

			// broadcast scalar epsilon if there is a fourth scalar input
			if(eps != null) {
				FederatedRequest tmpFr = fedMap.broadcast(eps);
				varNewIn[3] = tmpFr.getID();
				frB.add(tmpFr);
				// change the is_literal flag from true to false because when broadcasted it is no literal anymore
				instString = instString.replace("true", "false");
			}

			FederatedRequest frComp = FederationUtils.callInstruction(instString, output,
				qop.hasFourInputs() ? new CPOperand[]{input1, input2, input3, _input4}
				: new CPOperand[]{input1, input2, input3}, varNewIn);

			// get partial results from federated workers
			FederatedRequest frGet = new FederatedRequest(RequestType.GET_VAR, frComp.getID());

			ArrayList<FederatedRequest> frC = new ArrayList<>();
			frC.add(fedMap.cleanup(getTID(), frComp.getID()));
			for(FederatedRequest[] frS : frSliced)
				frC.add(fedMap.cleanup(getTID(), frS[0].getID()));
			for(FederatedRequest fr : frB)
				frC.add(fedMap.cleanup(getTID(), fr.getID()));

			FederatedRequest[] frAll = ArrayUtils.addAll(ArrayUtils.addAll(
				frB.toArray(new FederatedRequest[0]), frComp, frGet),
				frC.toArray(new FederatedRequest[0]));

			// execute federated instructions
			Future<FederatedResponse>[] response = frSliced.isEmpty() ?
				fedMap.execute(getTID(), true, frAll) : fedMap.executeMultipleSlices(
					getTID(), true, frSliced.toArray(new FederatedRequest[0][]), frAll);

			if((wdivmm_type.isLeft() && X.isFederated(FType.ROW))
				|| (wdivmm_type.isRight() && X.isFederated(FType.COL))) {
				// aggregate partial results from federated responses
				AggregateUnaryOperator aop = InstructionUtils.parseBasicAggregateUnaryOperator("uak+");
				ec.setMatrixOutput(output.getName(), FederationUtils.aggMatrix(aop, response, fedMap));
			}
			else if(wdivmm_type.isLeft() || wdivmm_type.isRight() || wdivmm_type.isBasic()) {
				// bind partial results from federated responses
				ec.setMatrixOutput(output.getName(), FederationUtils.bind(response, false));
			}
			else {
				throw new DMLRuntimeException("Federated WDivMM only supported for BASIC, LEFT or RIGHT variants.");
			}
		}
		else {
			throw new DMLRuntimeException("Unsupported federated inputs (X, U, V) = ("
				+ X.isFederated() + ", " + U.isFederated() + ", " + V.isFederated() + ")");
		}
	}
}

