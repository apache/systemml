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

import org.apache.sysds.lops.MapMultChain.ChainType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.AlignType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

import java.util.concurrent.Future;

public class MMChainFEDInstruction extends UnaryFEDInstruction {
	
	public MMChainFEDInstruction(CPOperand in1, CPOperand in2, CPOperand in3, 
		CPOperand out, ChainType type, int k, String opcode, String istr) {
		super(FEDType.MMChain, null, in1, in2, in3, out, opcode, istr);
		_type = type;
	}
	
	private final ChainType _type;

	public ChainType getMMChainType() {
		return _type;
	}

	public static MMChainFEDInstruction parseInstruction ( String str ) {
		//parse instruction parts (without exec type)
		String[] parts = InstructionUtils.getInstructionPartsWithValueType( str );
		InstructionUtils.checkNumFields( parts, 5, 6 );
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		
		if( parts.length==6 ) {
			CPOperand out= new CPOperand(parts[3]);
			ChainType type = ChainType.valueOf(parts[4]);
			int k = Integer.parseInt(parts[5]);
			return new MMChainFEDInstruction(in1, in2, null, out, type, k, opcode, str);
		}
		else { //parts.length==7
			CPOperand in3 = new CPOperand(parts[3]);
			CPOperand out = new CPOperand(parts[4]);
			ChainType type = ChainType.valueOf(parts[5]);
			int k = Integer.parseInt(parts[6]);
			return new MMChainFEDInstruction(in1, in2, in3, out, type, k, opcode, str);
		}
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = ec.getMatrixObject(input1);
		MatrixObject mo2 = ec.getMatrixObject(input2);
		MatrixObject mo3 = _type.isWeighted() ? ec.getMatrixObject(input3) : null;
		
		if( !mo1.isFederated() )
			throw new DMLRuntimeException("Federated MMChain: Federated main input expected, "
				+ "but invoked w/ "+mo1.isFederated()+" "+mo2.isFederated());

		// broadcast vector mo2
		FederatedRequest fr1 = mo1.getFedMapping().broadcast(mo2, mo1.getUniqueID());

		if(_type.isWeighted() && mo3.isFederated()
			&& mo1.getFedMapping().isAligned(mo3.getFedMapping(), AlignType.ROW)) {
				//construct commands: execute, get and aggregate, cleanup
				FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
					new CPOperand[]{input1, input2, input3},
					new long[]{mo1.getFedMapping().getID(), fr1.getID(), mo3.getFedMapping().getID()});
				FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
				FederatedRequest fr4 = mo1.getFedMapping()
					.cleanup(getTID(), fr1.getID(), fr2.getID());

				//execute federated operations and aggregate
				Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3, fr4);
				MatrixBlock ret = FederationUtils.aggAdd(tmp);
				ec.setMatrixOutput(output.getName(), ret);
		}
		else if( !_type.isWeighted() ) { //XtXv
			//construct commands: execute, get and aggregate, cleanup
			FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2}, new long[]{mo1.getFedMapping().getID(), fr1.getID()});
			FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
			FederatedRequest fr4 = mo1.getFedMapping()
				.cleanup(getTID(), fr1.getID(), fr2.getID());
			
			//execute federated operations and aggregate
			Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3, fr4);
			MatrixBlock ret = FederationUtils.aggAdd(tmp);
			ec.setMatrixOutput(output.getName(), ret);
		}
		else { //XtwXv | XtXvy
			//construct commands: broadcast vector mo3, execute, get and aggregate, cleanup
			FederatedRequest[] fr0 = mo1.getFedMapping().broadcastSliced(mo3, false, mo1.getUniqueID());
			FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2, input3},
				new long[]{mo1.getFedMapping().getID(), fr1.getID(), fr0[0].getID()});
			FederatedRequest fr3 = new FederatedRequest(RequestType.GET_VAR, fr2.getID());
			FederatedRequest fr4 = mo1.getFedMapping()
				.cleanup(getTID(), fr0[0].getID(), fr1.getID(), fr2.getID());

			//execute federated operations and aggregate
			Future<FederatedResponse>[] tmp = mo1.getFedMapping().execute(getTID(), fr0, fr1, fr2, fr3, fr4);
			MatrixBlock ret = FederationUtils.aggAdd(tmp);
			ec.setMatrixOutput(output.getName(), ret);
		}
	}
}
