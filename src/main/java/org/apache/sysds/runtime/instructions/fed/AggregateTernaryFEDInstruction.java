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
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.AlignType;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.cp.AggregateTernaryCPInstruction;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.DoubleObject;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;

public class AggregateTernaryFEDInstruction extends FEDInstruction {
	// private static final Log LOG = LogFactory.getLog(AggregateTernaryFEDInstruction.class.getName());

	public final AggregateTernaryCPInstruction _ins;

	protected AggregateTernaryFEDInstruction(AggregateTernaryCPInstruction ins) {
		super(FEDType.AggregateTernary, ins.getOperator(), ins.getOpcode(), ins.getInstructionString());
		_ins = ins;
	}

	public static AggregateTernaryFEDInstruction parseInstruction(AggregateTernaryCPInstruction ins) {
		return new AggregateTernaryFEDInstruction(ins);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = ec.getMatrixObject(_ins.input1);
		MatrixObject mo2 = ec.getMatrixObject(_ins.input2);
		MatrixObject mo3 = _ins.input3.isLiteral() ? null : ec.getMatrixObject(_ins.input3);
		if(mo3 != null && mo1.isFederated() && mo2.isFederated() && mo3.isFederated()
				&& mo1.getFedMapping().isAligned(mo2.getFedMapping(), mo1.isFederated(FType.ROW) ? AlignType.ROW : AlignType.COL)
				&& mo2.getFedMapping().isAligned(mo3.getFedMapping(), mo1.isFederated(FType.ROW) ? AlignType.ROW : AlignType.COL)) {
			FederatedRequest fr1 = FederationUtils.callInstruction(_ins.getInstructionString(), _ins.getOutput(),
				new CPOperand[] {_ins.input1, _ins.input2, _ins.input3},
				new long[] {mo1.getFedMapping().getID(), mo2.getFedMapping().getID(), mo3.getFedMapping().getID()});
			FederatedRequest fr2 = new FederatedRequest(RequestType.GET_VAR, fr1.getID());
			FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1.getID());
			Future<FederatedResponse>[] response = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3);
			
			if(_ins.output.getDataType().isScalar()) {
				AggregateUnaryOperator aop = InstructionUtils.parseBasicAggregateUnaryOperator("uak+");
				ec.setScalarOutput(_ins.output.getName(), FederationUtils.aggScalar(aop, response, mo1.getFedMapping()));
			}
			else {
				AggregateUnaryOperator aop = InstructionUtils.parseBasicAggregateUnaryOperator(_ins.getOpcode().equals("fed_tak+*") ? "uak+" : "uack+");
				ec.setMatrixOutput(_ins.output.getName(), FederationUtils.aggMatrix(aop, response, mo1.getFedMapping()));
			}
		}
		else if(mo1.isFederated() && mo2.isFederated() && mo1.getFedMapping().isAligned(mo2.getFedMapping(), false) &&
			mo3 == null) {
			FederatedRequest fr1 = mo1.getFedMapping().broadcast(ec.getScalarInput(_ins.input3));
			FederatedRequest fr2 = FederationUtils.callInstruction(_ins.getInstructionString(),
				_ins.getOutput(),
				new CPOperand[] {_ins.input1, _ins.input2, _ins.input3},
				new long[] {mo1.getFedMapping().getID(), mo2.getFedMapping().getID(), fr1.getID()});
			FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
			FederatedRequest fr4 = mo2.getFedMapping().cleanup(getTID(), fr1.getID(), fr2.getID());
			Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3, fr4);

			if(_ins.output.getDataType().isScalar()) {
				double sum = 0;
				for(Future<FederatedResponse> fr : tmp)
					try {
						sum += ((ScalarObject) fr.get().getData()[0]).getDoubleValue();
					}
					catch(Exception e) {
						throw new DMLRuntimeException("Federated Get data failed with exception on TernaryFedInstruction", e);
					}

				ec.setScalarOutput(_ins.output.getName(), new DoubleObject(sum));
			}
			else {
				throw new DMLRuntimeException("Not Implemented Federated Ternary Variation");
			}
		} else if(mo1.isFederated() && _ins.input3.isMatrix() && mo3 != null) {
			FederatedRequest[] fr1 = mo1.getFedMapping().broadcastSliced(mo3, false, mo1.getUniqueID());
			FederatedRequest[] fr2 = mo1.getFedMapping().broadcastSliced(mo2, false, mo1.getUniqueID());
			FederatedRequest fr3 = FederationUtils.callInstruction(_ins.getInstructionString(),
				_ins.getOutput(),
				new CPOperand[] {_ins.input1, _ins.input2, _ins.input3},
				new long[] {mo1.getFedMapping().getID(), fr2[0].getID(), fr1[0].getID()});
			FederatedRequest fr4 = new FederatedRequest(RequestType.GET_VAR, fr3.getID());
			FederatedRequest fr5 = mo2.getFedMapping().cleanup(getTID(), fr1[0].getID(), fr2[0].getID());
			Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr1, fr2[0], fr3, fr4, fr5);

			if(_ins.output.getDataType().isScalar()) {
				double sum = 0;
				for(Future<FederatedResponse> fr : tmp)
					try {
						sum += ((ScalarObject) fr.get().getData()[0]).getDoubleValue();
					}
					catch(Exception e) {
						throw new DMLRuntimeException("Federated Get data failed with exception on TernaryFedInstruction", e);
					}

				ec.setScalarOutput(_ins.output.getName(), new DoubleObject(sum));
			}
			else {
				throw new DMLRuntimeException("Not Implemented Federated Ternary Variation");
			}
		}
		else {
			if(mo3 == null)
				throw new DMLRuntimeException("Federated AggregateTernary not supported with the "
					+ "following federated objects: " + mo1.isFederated() + ":" + mo1.getFedMapping() + " "
					+ mo2.isFederated() + ":" + mo2.getFedMapping());
			else
				throw new DMLRuntimeException("Federated AggregateTernary not supported with the "
					+ "following federated objects: " + mo1.isFederated() + ":" + mo1.getFedMapping() + " "
					+ mo2.isFederated() + ":" + mo2.getFedMapping() + mo3.isFederated() + ":" + mo3.getFedMapping());
		}

	}
}
