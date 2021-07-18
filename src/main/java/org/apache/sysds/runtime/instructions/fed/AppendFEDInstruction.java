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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.functionobjects.OffsetColumnIndex;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataUtils;

public class AppendFEDInstruction extends BinaryFEDInstruction {
	protected boolean _cbind; // otherwise rbind

	protected AppendFEDInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, boolean cbind,
		String opcode, String istr) {
		super(FEDType.Append, op, in1, in2, out, opcode, istr);
		_cbind = cbind;
	}

	public static AppendFEDInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 6, 5, 4);

		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[parts.length - 2]);
		boolean cbind = Boolean.parseBoolean(parts[parts.length - 1]);

		Operator op = new ReorgOperator(OffsetColumnIndex.getOffsetColumnIndexFnObject(-1));
		return new AppendFEDInstruction(op, in1, in2, out, cbind, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		// get inputs
		MatrixObject mo1 = ec.getMatrixObject(input1.getName());
		MatrixObject mo2 = ec.getMatrixObject(input2.getName());
		DataCharacteristics dc1 = mo1.getDataCharacteristics();
		DataCharacteristics dc2 = mo2.getDataCharacteristics();

		// check input dimensions
		if(_cbind && mo1.getNumRows() != mo2.getNumRows()) {
			StringBuilder sb = new StringBuilder();
			sb.append("Append-cbind is not possible for federated input matrices ");
			sb.append(input1.getName()).append(" and ").append(input2.getName());
			sb.append(" with different number of rows: ");
			sb.append(mo1.getNumRows()).append(" vs ").append(mo2.getNumRows());
			throw new DMLRuntimeException(sb.toString());
		}
		else if(!_cbind && mo1.getNumColumns() != mo2.getNumColumns()) {
			StringBuilder sb = new StringBuilder();
			sb.append("Append-rbind is not possible for federated input matrices ");
			sb.append(input1.getName()).append(" and ").append(input2.getName());
			sb.append(" with different number of columns: ");
			sb.append(mo1.getNumColumns()).append(" vs ").append(mo2.getNumColumns());
			throw new DMLRuntimeException(sb.toString());
		}

		//prepare output
		MatrixObject out = ec.getMatrixObject(output);
		MetaDataUtils.updateAppendDataCharacteristics(dc1, dc2, out.getDataCharacteristics(), _cbind);
		
		// federated/federated
		if( mo1.isFederated() && mo2.isFederated() 
			&& mo1.getFedMapping().getType()==mo2.getFedMapping().getType() ) 
		{
			long id = FederationUtils.getNextFedDataID();
			long roff = _cbind ? 0 : dc1.getRows();
			long coff = _cbind ? dc1.getCols() : 0;
			out.setFedMapping(mo1.getFedMapping().identCopy(getTID(), id)
				.bind(roff, coff, mo2.getFedMapping().identCopy(getTID(), id)));
		}
		// federated/local, local/federated cbind
		else if( (mo1.isFederated(FType.ROW) || mo2.isFederated(FType.ROW)) && _cbind ) {
			MatrixObject moFed = mo1.isFederated(FType.ROW) ? mo1 : mo2;
			MatrixObject moLoc = mo1.isFederated(FType.ROW) ? mo2 : mo1;
			
			//construct commands: broadcast lhs, fed append, clean broadcast
			FederatedRequest[] fr1 = moFed.getFedMapping().broadcastSliced(moLoc, false, moFed.getUniqueID());
			FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2}, mo1.isFederated(FType.ROW) ?
				new long[]{ moFed.getFedMapping().getID(), fr1[0].getID()} :
				new long[]{ fr1[0].getID(), moFed.getFedMapping().getID()});
			FederatedRequest fr3 = moFed.getFedMapping().cleanup(getTID(), fr1[0].getID());
			
			//execute federated operations and set output
			moFed.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);
			out.setFedMapping(moFed.getFedMapping().copyWithNewID(fr2.getID(), out.getNumColumns()));
		}
		// federated/local, local/federated rbind
		else if( (mo1.isFederated(FType.ROW) || mo2.isFederated(FType.ROW)) && !_cbind) {
			long id = FederationUtils.getNextFedDataID();
			long roff = _cbind ? 0 : dc1.getRows();
			long coff = _cbind ? dc1.getCols() : 0;
			FederationMap fed1 = mo1.isFederated(FType.ROW) ?
				mo1.getFedMapping() : FederationUtils.federateLocalData(mo1);
			FederationMap fed2 = mo2.isFederated(FType.ROW) ?
				mo2.getFedMapping() : FederationUtils.federateLocalData(mo2);
			out.setFedMapping(fed1.identCopy(getTID(), id)
				.bind(roff, coff, fed2.identCopy(getTID(), id)));
		}
		else {
			throw new DMLRuntimeException("Unsupported federated append: "
				+ (mo1.isFederated() ? mo1.getFedMapping().getType().name():"LOCAL") + " "
				+ (mo2.isFederated() ? mo2.getFedMapping().getType().name():"LOCAL") + " " + _cbind);
		}
	}
}
