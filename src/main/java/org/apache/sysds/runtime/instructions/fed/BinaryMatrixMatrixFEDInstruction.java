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
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.AlignType;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;

public class BinaryMatrixMatrixFEDInstruction extends BinaryFEDInstruction
{
	protected BinaryMatrixMatrixFEDInstruction(Operator op,
		CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr, FederatedOutput fedOut) {
		super(FEDType.Binary, op, in1, in2, out, opcode, istr, fedOut);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = ec.getMatrixObject(input1);
		MatrixObject mo2 = ec.getMatrixObject(input2);
		
		//canonicalization for federated lhs
		if( !mo1.isFederated() && mo2.isFederated()
			&& mo1.getDataCharacteristics().equalDims(mo2.getDataCharacteristics())
			&& ((BinaryOperator)_optr).isCommutative() ) {
			mo1 = ec.getMatrixObject(input2);
			mo2 = ec.getMatrixObject(input1);
		}

		//execute federated operation on mo1 or mo2
		FederatedRequest fr2 = null;
		if( mo2.isFederated() && !mo2.isFederated(FType.BROADCAST)) {
			if(mo1.isFederated() && mo1.getFedMapping().isAligned(mo2.getFedMapping(),
					mo1.isFederated(FType.ROW) ? AlignType.ROW : AlignType.COL)) {
				fr2 = FederationUtils.callInstruction(instString, output,
					new CPOperand[]{input1, input2},
					new long[]{mo1.getFedMapping().getID(), mo2.getFedMapping().getID()}, true);
				mo1.getFedMapping().execute(getTID(), true, fr2);
			}
			else if ( !mo1.isFederated() ){
				FederatedRequest[] fr1 = mo2.getFedMapping().broadcastSliced(mo1, false, mo2.getUniqueID());
				fr2 = FederationUtils.callInstruction(instString, output,
					new CPOperand[]{input1, input2},
					new long[]{fr1[0].getID(), mo2.getFedMapping().getID()}, true);
				mo2.getFedMapping().execute(getTID(), true, fr1, fr2);
			}
			else {
				throw new DMLRuntimeException("Matrix-matrix binary operations with a "
					+ "federated right input are only supported for special cases yet.");
			}
		}
		else { // matrix-matrix binary operations -> lhs fed input -> fed output
			if(mo1.isFederated(FType.FULL)) {
				// full federated (row and col)
				if(mo1.getFedMapping().getSize() == 1) {
					// only one partition (MM on a single fed worker)
					FederatedRequest fr1 = mo1.getFedMapping().broadcast(mo2, mo1.getUniqueID());
					fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[]{input1, input2},
					new long[]{mo1.getFedMapping().getID(), fr1.getID()}, true);
					FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1.getID());
					//execute federated instruction and cleanup intermediates
					mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);
				}
				else {
					throw new DMLRuntimeException("Matrix-matrix binary operations with a full partitioned federated input with multiple partitions are not supported yet.");
				}
			}
			else if((mo1.isFederated(FType.ROW) && mo2.getNumRows() == 1)      //matrix-rowVect
				|| (mo1.isFederated(FType.COL) && mo2.getNumColumns() == 1)) { //matrix-colVect
				// MV row partitioned row vector, MV col partitioned col vector
				FederatedRequest fr1 = mo1.getFedMapping().broadcast(mo2, mo1.getUniqueID());
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[]{input1, input2},
				new long[]{mo1.getFedMapping().getID(), fr1.getID()}, true);
				FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1.getID());
				//execute federated instruction and cleanup intermediates
				mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);
			}
			else if((mo1.isFederated(FType.ROW) ^ mo1.isFederated(FType.COL))
			 	|| (mo1.isFederated(FType.FULL) && mo1.getFedMapping().getSize() == 1)) {
				// row partitioned MM or col partitioned MM
				FederatedRequest[] fr1 = mo1.getFedMapping().broadcastSliced(mo2, false, mo1.getUniqueID());
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[]{input1, input2},
					new long[]{mo1.getFedMapping().getID(), fr1[0].getID()}, true);
				FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1[0].getID());
				//execute federated instruction and cleanup intermediates
				mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);
			}
			else if ( mo1.isFederated(FType.PART) && !mo2.isFederated() ){
				FederatedRequest fr1 = mo1.getFedMapping().broadcast(mo2, mo1.getUniqueID());
				fr2 = FederationUtils.callInstruction(instString, output, new CPOperand[]{input1, input2},
					new long[]{mo1.getFedMapping().getID(), fr1.getID()}, true);
				FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1.getID());
				//execute federated instruction and cleanup intermediates
				mo1.getFedMapping().execute(getTID(), true, fr1, fr2, fr3);
			}
			else {
				throw new DMLRuntimeException("Matrix-matrix binary operations are only supported with a row partitioned or column partitioned federated input yet.");
			}
		}

		if ( mo1.isFederated(FType.PART) && !mo2.isFederated() )
			setOutputFedMappingPart(mo1, mo2, fr2.getID(), ec);
		else if ( mo1.isFederated() )
			setOutputFedMapping(mo1, fr2.getID(), ec);
		else if ( mo2.isFederated() )
			setOutputFedMapping(mo2, fr2.getID(), ec);
		else throw new DMLRuntimeException("Input is not federated, so the output FedMapping cannot be set!");
	}

	/**
	 * Sets the output with a federation map of overlapping partial aggregates with metadata copied from mo1.
	 * @param mo1 matrix object with number of rows used to set output number of rows and retrieve federated map
	 * @param mo2 matrix object with number of columns used to set output number of columns
	 * @param outputID ID of output
	 * @param ec execution context
	 */
	private void setOutputFedMappingPart(MatrixObject mo1, MatrixObject mo2, long outputID, ExecutionContext ec){
		MatrixObject out = ec.getMatrixObject(output);
		out.getDataCharacteristics().set(mo1.getNumRows(), mo2.getNumColumns(), (int)mo1.getBlocksize());
		FederationMap outputFedMap = mo1.getFedMapping()
			.copyWithNewIDAndRange(mo1.getNumRows(), mo2.getNumColumns(), outputID);
		out.setFedMapping(outputFedMap);
	}

	/**
	 * Set data characteristics and fed mapping for output.
	 * @param moFederated federated matrix object from which data characteristics and fed mapping are derived
	 * @param outputFedmappingID ID for the fed mapping of output
	 * @param ec execution context
	 */
	private void setOutputFedMapping(MatrixObject moFederated, long outputFedmappingID, ExecutionContext ec){
		MatrixObject out = ec.getMatrixObject(output);
		out.getDataCharacteristics().set(moFederated.getDataCharacteristics());
		out.setFedMapping(moFederated.getFedMapping().copyWithNewID(outputFedmappingID));
	}
}
