/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.	See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.instructions.fed;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.AlignType;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.DoubleObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.QuaternaryOperator;

import java.util.ArrayList;
import java.util.concurrent.Future;

public class QuaternaryWCeMMFEDInstruction extends QuaternaryFEDInstruction
{
	// input1 ... X
	// input2 ... U
	// input3 ... V
	// _input4 ... W (=epsilon)
	protected QuaternaryWCeMMFEDInstruction(Operator operator,
		CPOperand in1, CPOperand in2, CPOperand in3, CPOperand in4,
		CPOperand out, String opcode, String instruction_str)
	{
		super(FEDType.Quaternary, operator, in1, in2, in3, in4, out, opcode, instruction_str);
	}

	@Override
	public void processInstruction(ExecutionContext ec)
	{
		QuaternaryOperator qop = (QuaternaryOperator) _optr;
		MatrixObject X = ec.getMatrixObject(input1);
		MatrixObject U = ec.getMatrixObject(input2);
		MatrixObject V = ec.getMatrixObject(input3);
		ScalarObject eps = null;

		if(qop.hasFourInputs()) {
			eps = (_input4.getDataType() == DataType.SCALAR) ?
				ec.getScalarInput(_input4) :
				new DoubleObject(ec.getMatrixInput(_input4.getName()).quickGetValue(0, 0));
		}

		if(X.isFederated()) {
			FederationMap fedMap = X.getFedMapping();
			FederatedRequest[] frSliced = null;
			ArrayList<FederatedRequest> frB = new ArrayList<>(); // FederatedRequests of broadcasts
			long[] varNewIn = new long[eps != null ? 4 : 3];
			varNewIn[0] = fedMap.getID();
			
			if(X.isFederated(FType.ROW)) { // row partitioned X
				if(U.isFederated(FType.ROW) && fedMap.isAligned(U.getFedMapping(), AlignType.ROW)) {
					varNewIn[1] = U.getFedMapping().getID();
				}
				else {
					frSliced = fedMap.broadcastSliced(U, false, X.getUniqueID());
					varNewIn[1] = frSliced[0].getID();
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
					varNewIn[2] = V.getFedMapping().getID();
				}
				else {
					frSliced = fedMap.broadcastSliced(V, true, X.getUniqueID());
					varNewIn[2] = frSliced[0].getID();
				}
			}
			else {
				throw new DMLRuntimeException("Federated WCeMM only supported for ROW or COLUMN partitioned "
					+ "federated data.");
			}

			// broadcast the scalar epsilon if there are four inputs
			if(eps != null) {
				FederatedRequest tmpFr = fedMap.broadcast(eps);
				varNewIn[3] = tmpFr.getID();
				frB.add(tmpFr);
				// change the is_literal flag from true to false because when broadcasted it is no literal anymore
				instString = instString.replace("true", "false");
			}

			FederatedRequest frComp = FederationUtils.callInstruction(instString, output,
				eps == null ? new CPOperand[]{input1, input2, input3}
					: new CPOperand[]{input1, input2, input3, _input4}, varNewIn);

			FederatedRequest frGet = new FederatedRequest(RequestType.GET_VAR, frComp.getID());
			
			ArrayList<FederatedRequest> frC = new ArrayList<>(); // FederatedRequests for cleanup
			frC.add(fedMap.cleanup(getTID(), frComp.getID()));
			if(frSliced != null)
				frC.add(fedMap.cleanup(getTID(), frSliced[0].getID()));
			for(FederatedRequest fr : frB)
				frC.add(fedMap.cleanup(getTID(), fr.getID()));

			FederatedRequest[] frAll = ArrayUtils.addAll(ArrayUtils.addAll(
				frB.toArray(new FederatedRequest[0]), frComp, frGet),
				frC.toArray(new FederatedRequest[0]));

			// execute federated instructions
			Future<FederatedResponse>[] response = frSliced == null ?
				fedMap.execute(getTID(), true, frAll) : fedMap.execute(getTID(), true, frSliced, frAll);
			
			//aggregate partial results from federated responses
			AggregateUnaryOperator aop = InstructionUtils.parseBasicAggregateUnaryOperator("uak+");
			ec.setVariable(output.getName(), FederationUtils.aggScalar(aop, response));
		}
		else {
			throw new DMLRuntimeException("Unsupported federated inputs (X, U, V) = ("
				+ X.isFederated() + ", " + U.isFederated() + ", " + V.isFederated() + ")");
		}
	}
}
