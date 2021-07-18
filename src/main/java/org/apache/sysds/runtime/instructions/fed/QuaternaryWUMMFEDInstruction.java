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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.AlignType;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.operators.Operator;

public class QuaternaryWUMMFEDInstruction extends QuaternaryFEDInstruction {

	/**
	 * This instruction performs:
	 *
	 * Z = X / exp(U %*% t(V));
	 *
	 *
	 * @param operator        Weighted Unary Matrix Multiplication Federated Instruction.
	 * @param in1             X
	 * @param in2             U
	 * @param in3             V
	 * @param out             The Federated Result Z
	 * @param opcode          ...
	 * @param instruction_str ...
	 */
	protected QuaternaryWUMMFEDInstruction(Operator operator, CPOperand in1, CPOperand in2, CPOperand in3,
		CPOperand out, String opcode, String instruction_str) {
		super(FEDType.Quaternary, operator, in1, in2, in3, out, opcode, instruction_str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject X = ec.getMatrixObject(input1);
		MatrixObject U = ec.getMatrixObject(input2);
		MatrixObject V = ec.getMatrixObject(input3);

		if(X.isFederated()) {
			FederationMap fedMap = X.getFedMapping();
			FederatedRequest[] frSliced = null; // FederatedRequest for broadcastSliced
			FederatedRequest frB = null; // FederatedRequest for broadcast
			long[] varNewIn = new long[3];
			varNewIn[0] = fedMap.getID();

			if(X.isFederated(FType.ROW)) { // row partitioned X
				if(U.isFederated(FType.ROW) && fedMap.isAligned(U.getFedMapping(), AlignType.ROW)) {
					// U federated and aligned
					varNewIn[1] = U.getFedMapping().getID();
				}
				else {
					frSliced = fedMap.broadcastSliced(U, false, X.getUniqueID());
					varNewIn[1] = frSliced[0].getID();
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
					frSliced = fedMap.broadcastSliced(V, true, X.getUniqueID());
					varNewIn[2] = frSliced[0].getID();
				}
			}
			else {
				throw new DMLRuntimeException("Federated WUMM only supported for ROW or COLUMN partitioned "
					+ "federated data.");
			}

			FederatedRequest frComp = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2, input3}, varNewIn);

			ArrayList<FederatedRequest> frC = new ArrayList<>();
			if(frSliced != null)
				frC.add(fedMap.cleanup(getTID(), frSliced[0].getID()));
			frC.add(fedMap.cleanup(getTID(), frB.getID()));

			FederatedRequest[] frAll = ArrayUtils.addAll(new FederatedRequest[]{frB, frComp},
				frC.toArray(new FederatedRequest[0]));

			// execute federated instructions
			if(frSliced == null)
				fedMap.execute(getTID(), true, frAll);
			else
				fedMap.execute(getTID(), true, frSliced, frAll);

			// derive output federated mapping
			MatrixObject out = ec.getMatrixObject(output);
			out.setFedMapping(fedMap.copyWithNewID(frComp.getID()));
			setOutputDataCharacteristics(X, U, V, ec);
		}
		else {
			throw new DMLRuntimeException("Unsupported federated inputs (X, U, V) = (" 
				+ X.isFederated() + ", " + U.isFederated() + ", " + V.isFederated() + ")");
		}
	}
}
