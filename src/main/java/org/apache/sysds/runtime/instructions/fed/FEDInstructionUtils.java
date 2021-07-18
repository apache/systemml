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

import org.apache.sysds.runtime.codegen.SpoofCellwise;
import org.apache.sysds.runtime.codegen.SpoofMultiAggregate;
import org.apache.sysds.runtime.codegen.SpoofOuterProduct;
import org.apache.sysds.runtime.codegen.SpoofRowwise;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.caching.FrameObject;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.caching.TensorObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.AggregateBinaryCPInstruction;
import org.apache.sysds.runtime.instructions.cp.AggregateTernaryCPInstruction;
import org.apache.sysds.runtime.instructions.cp.AggregateUnaryCPInstruction;
import org.apache.sysds.runtime.instructions.cp.BinaryCPInstruction;
import org.apache.sysds.runtime.instructions.cp.BinaryFrameScalarCPInstruction;
import org.apache.sysds.runtime.instructions.cp.CtableCPInstruction;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.IndexingCPInstruction;
import org.apache.sysds.runtime.instructions.cp.MMChainCPInstruction;
import org.apache.sysds.runtime.instructions.cp.MMTSJCPInstruction;
import org.apache.sysds.runtime.instructions.cp.MultiReturnParameterizedBuiltinCPInstruction;
import org.apache.sysds.runtime.instructions.cp.ParameterizedBuiltinCPInstruction;
import org.apache.sysds.runtime.instructions.cp.QuaternaryCPInstruction;
import org.apache.sysds.runtime.instructions.cp.ReorgCPInstruction;
import org.apache.sysds.runtime.instructions.cp.TernaryCPInstruction;
import org.apache.sysds.runtime.instructions.cp.SpoofCPInstruction;
import org.apache.sysds.runtime.instructions.cp.UnaryCPInstruction;
import org.apache.sysds.runtime.instructions.cp.UnaryMatrixCPInstruction;
import org.apache.sysds.runtime.instructions.cp.VariableCPInstruction;
import org.apache.sysds.runtime.instructions.cp.VariableCPInstruction.VariableOperationCode;
import org.apache.sysds.runtime.instructions.fed.FEDInstruction.FederatedOutput;
import org.apache.sysds.runtime.instructions.spark.AggregateUnarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.AppendGAlignedSPInstruction;
import org.apache.sysds.runtime.instructions.spark.AppendGSPInstruction;
import org.apache.sysds.runtime.instructions.spark.BinaryMatrixBVectorSPInstruction;
import org.apache.sysds.runtime.instructions.spark.BinaryMatrixMatrixSPInstruction;
import org.apache.sysds.runtime.instructions.spark.BinaryMatrixScalarSPInstruction;
import org.apache.sysds.runtime.instructions.spark.BinarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.BinaryTensorTensorBroadcastSPInstruction;
import org.apache.sysds.runtime.instructions.spark.BinaryTensorTensorSPInstruction;
import org.apache.sysds.runtime.instructions.spark.CentralMomentSPInstruction;
import org.apache.sysds.runtime.instructions.spark.MapmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.QuantilePickSPInstruction;
import org.apache.sysds.runtime.instructions.spark.QuantileSortSPInstruction;
import org.apache.sysds.runtime.instructions.spark.QuaternarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.SpoofSPInstruction;
import org.apache.sysds.runtime.instructions.spark.UnarySPInstruction;
import org.apache.sysds.runtime.instructions.spark.WriteSPInstruction;

public class FEDInstructionUtils {
	
	private static String[] PARAM_BUILTINS = new String[]{
		"replace", "rmempty", "lowertri", "uppertri", "transformdecode", "transformapply", "tokenize"};
	
	// private static final Log LOG = LogFactory.getLog(FEDInstructionUtils.class.getName());

	// This is currently a rather simplistic to our solution of replacing instructions with their correct federated
	// counterpart, since we do not propagate the information that a matrix is federated, therefore we can not decide
	// to choose a federated instruction earlier.

	/**
	 * Check and replace CP instructions with federated instructions if the instruction match criteria.
	 *
	 * @param inst The instruction to analyse
	 * @param ec The Execution Context
	 * @return The potentially modified instruction
	 */
	public static Instruction checkAndReplaceCP(Instruction inst, ExecutionContext ec) {
		FEDInstruction fedinst = null;
		if (inst instanceof AggregateBinaryCPInstruction) {
			AggregateBinaryCPInstruction instruction = (AggregateBinaryCPInstruction) inst;
			if( instruction.input1.isMatrix() && instruction.input2.isMatrix() ) {
				MatrixObject mo1 = ec.getMatrixObject(instruction.input1);
				MatrixObject mo2 = ec.getMatrixObject(instruction.input2);
				if (((mo1.isFederated(FType.ROW) || mo1.isFederated(FType.COL)) && !mo1.isFederated(FType.BROADCAST))
					|| (mo2.isFederated(FType.ROW) && !mo2.isFederated(FType.BROADCAST))) {
					fedinst = AggregateBinaryFEDInstruction.parseInstruction(
						InstructionUtils.concatOperands(inst.getInstructionString(), FederatedOutput.NONE.name()));
				}
			}
		}
		else if( inst instanceof MMChainCPInstruction) {
			MMChainCPInstruction linst = (MMChainCPInstruction) inst;
			MatrixObject mo = ec.getMatrixObject(linst.input1);
			if( mo.isFederated(FType.ROW) )
				fedinst = MMChainFEDInstruction.parseInstruction(linst.getInstructionString());
		}
		else if( inst instanceof MMTSJCPInstruction ) {
			MMTSJCPInstruction linst = (MMTSJCPInstruction) inst;
			MatrixObject mo = ec.getMatrixObject(linst.input1);
			if( (mo.isFederated(FType.ROW) && linst.getMMTSJType().isLeft()) ||
				(mo.isFederated(FType.COL) && linst.getMMTSJType().isRight()))
				fedinst = TsmmFEDInstruction.parseInstruction(linst.getInstructionString());
		}
		else if (inst instanceof UnaryCPInstruction && ! (inst instanceof IndexingCPInstruction)) {
			UnaryCPInstruction instruction = (UnaryCPInstruction) inst;
			if(inst instanceof ReorgCPInstruction && (inst.getOpcode().equals("r'") || inst.getOpcode().equals("rdiag")
				|| inst.getOpcode().equals("rev"))) {
				ReorgCPInstruction rinst = (ReorgCPInstruction) inst;
				CacheableData<?> mo = ec.getCacheableData(rinst.input1);

				if((mo instanceof MatrixObject || mo instanceof FrameObject) && mo.isFederated() )
					fedinst = ReorgFEDInstruction.parseInstruction(
						InstructionUtils.concatOperands(rinst.getInstructionString(),FederatedOutput.NONE.name()));
			}
			else if(instruction.input1 != null && instruction.input1.isMatrix()
				&& ec.containsVariable(instruction.input1)) {

				MatrixObject mo1 = ec.getMatrixObject(instruction.input1);
				if(instruction.getOpcode().equalsIgnoreCase("cm") && mo1.isFederated())
					fedinst = CentralMomentFEDInstruction.parseInstruction(inst.getInstructionString());
				else if(inst.getOpcode().equalsIgnoreCase("qsort") && mo1.isFederated()) {
					if(mo1.getFedMapping().getFederatedRanges().length == 1)
						fedinst = QuantileSortFEDInstruction.parseInstruction(inst.getInstructionString());
				}
				else if(inst.getOpcode().equalsIgnoreCase("rshape") && mo1.isFederated())
					fedinst = ReshapeFEDInstruction.parseInstruction(inst.getInstructionString());
				else if(inst instanceof AggregateUnaryCPInstruction  && mo1.isFederated() &&
					((AggregateUnaryCPInstruction) instruction).getAUType() == AggregateUnaryCPInstruction.AUType.DEFAULT)
					fedinst = AggregateUnaryFEDInstruction.parseInstruction(
						InstructionUtils.concatOperands(inst.getInstructionString(),FederatedOutput.NONE.name()));
				else if(inst instanceof UnaryMatrixCPInstruction && mo1.isFederated()) {
					if(UnaryMatrixFEDInstruction.isValidOpcode(inst.getOpcode()) &&
						!(inst.getOpcode().equalsIgnoreCase("ucumk+*") && mo1.isFederated(FType.COL)))
						fedinst = UnaryMatrixFEDInstruction.parseInstruction(inst.getInstructionString());
				}
			}
		}
		else if (inst instanceof BinaryCPInstruction) {
			BinaryCPInstruction instruction = (BinaryCPInstruction) inst;
			if( (instruction.input1.isMatrix() && ec.getMatrixObject(instruction.input1).isFederated()
				&& !ec.getMatrixObject(instruction.input1).isFederated(FType.BROADCAST))
				|| (instruction.input2.isMatrix() && ec.getMatrixObject(instruction.input2).isFederated()
				&& !ec.getMatrixObject(instruction.input2).isFederated(FType.BROADCAST))) {
				if(instruction.getOpcode().equals("append") )
					fedinst = AppendFEDInstruction.parseInstruction(inst.getInstructionString());
				else if(instruction.getOpcode().equals("qpick"))
					fedinst = QuantilePickFEDInstruction.parseInstruction(inst.getInstructionString());
				else if("cov".equals(instruction.getOpcode()) && (ec.getMatrixObject(instruction.input1).isFederated(FType.ROW) ||
					ec.getMatrixObject(instruction.input2).isFederated(FType.ROW)))
					fedinst = CovarianceFEDInstruction.parseInstruction(inst.getInstructionString());
				else
					fedinst = BinaryFEDInstruction.parseInstruction(
						InstructionUtils.concatOperands(inst.getInstructionString(),FederatedOutput.NONE.name()));
			} else if(inst.getOpcode().equals("_map") && inst instanceof BinaryFrameScalarCPInstruction && !inst.getInstructionString().contains("UtilFunctions")
				&& instruction.input1.isFrame() && ec.getFrameObject(instruction.input1).isFederated()) {
				fedinst = BinaryFrameScalarFEDInstruction.parseInstruction(InstructionUtils.concatOperands(inst.getInstructionString(),FederatedOutput.NONE.name()));
			}
		}
		else if( inst instanceof ParameterizedBuiltinCPInstruction ) {
			ParameterizedBuiltinCPInstruction pinst = (ParameterizedBuiltinCPInstruction) inst;
			if( ArrayUtils.contains(PARAM_BUILTINS, pinst.getOpcode()) && pinst.getTarget(ec).isFederated() )
				fedinst = ParameterizedBuiltinFEDInstruction.parseInstruction(pinst.getInstructionString());
		}
		else if (inst instanceof MultiReturnParameterizedBuiltinCPInstruction) {
			MultiReturnParameterizedBuiltinCPInstruction minst = (MultiReturnParameterizedBuiltinCPInstruction) inst;
			if(minst.getOpcode().equals("transformencode") && minst.input1.isFrame()) {
				CacheableData<?> fo = ec.getCacheableData(minst.input1);
				if(fo.isFederated()) {
					fedinst = MultiReturnParameterizedBuiltinFEDInstruction
						.parseInstruction(minst.getInstructionString());
				}
			}
		}
		else if(inst instanceof IndexingCPInstruction) {
			// matrix and frame indexing
			IndexingCPInstruction minst = (IndexingCPInstruction) inst;
			if((minst.input1.isMatrix() || minst.input1.isFrame())
				&& ec.getCacheableData(minst.input1).isFederated()) {
				fedinst = IndexingFEDInstruction.parseInstruction(minst.getInstructionString());
			}
		}
		else if(inst instanceof TernaryCPInstruction) {
			TernaryCPInstruction tinst = (TernaryCPInstruction) inst;
			if((tinst.input1.isMatrix() && ec.getCacheableData(tinst.input1).isFederated()
				&& !ec.getCacheableData(tinst.input1).isFederated(FType.BROADCAST))
				|| (tinst.input2.isMatrix() && ec.getCacheableData(tinst.input2).isFederated()
				&& !ec.getCacheableData(tinst.input2).isFederated(FType.BROADCAST))
				|| (tinst.input3.isMatrix() && ec.getCacheableData(tinst.input3).isFederated()
				&& !ec.getCacheableData(tinst.input3).isFederated(FType.BROADCAST))) {
				fedinst = TernaryFEDInstruction.parseInstruction(tinst.getInstructionString());
			}
		}
		else if(inst instanceof VariableCPInstruction ){
			VariableCPInstruction ins = (VariableCPInstruction) inst;
			if(ins.getVariableOpcode() == VariableOperationCode.Write
				&& ins.getInput1().isMatrix()
				&& ins.getInput3().getName().contains("federated")){
				fedinst = VariableFEDInstruction.parseInstruction(ins);
			}
			else if(ins.getVariableOpcode() == VariableOperationCode.CastAsFrameVariable
				&& ins.getInput1().isMatrix()
				&& ec.getCacheableData(ins.getInput1()).isFederated()){
				fedinst = VariableFEDInstruction.parseInstruction(ins);
			}
			else if(ins.getVariableOpcode() == VariableOperationCode.CastAsMatrixVariable
				&& ins.getInput1().isFrame()
				&& ec.getCacheableData(ins.getInput1()).isFederated()){
				fedinst = VariableFEDInstruction.parseInstruction(ins);
			}
		}
		else if(inst instanceof AggregateTernaryCPInstruction){
			AggregateTernaryCPInstruction ins = (AggregateTernaryCPInstruction) inst;
			if(ins.input1.isMatrix() && ec.getCacheableData(ins.input1).isFederated() && ins.input2.isMatrix() &&
				ec.getCacheableData(ins.input2).isFederated()) {
				fedinst = AggregateTernaryFEDInstruction.parseInstruction(ins);
			}
		}
		else if(inst instanceof QuaternaryCPInstruction) {
			QuaternaryCPInstruction instruction = (QuaternaryCPInstruction) inst;
			Data data = ec.getVariable(instruction.input1);
			if(data instanceof MatrixObject && ((MatrixObject) data).isFederated())
				fedinst = QuaternaryFEDInstruction.parseInstruction(instruction.getInstructionString());
		}
		else if(inst instanceof SpoofCPInstruction) {
			SpoofCPInstruction ins = (SpoofCPInstruction) inst;
			Class<?> scla = ins.getOperatorClass().getSuperclass();
			if(((scla == SpoofCellwise.class || scla == SpoofMultiAggregate.class || scla == SpoofOuterProduct.class)
					&& SpoofFEDInstruction.isFederated(ec, ins.getInputs(), scla))
				|| (scla == SpoofRowwise.class && SpoofFEDInstruction.isFederated(ec, FType.ROW, ins.getInputs(), scla))) {
				fedinst = SpoofFEDInstruction.parseInstruction(ins.getInstructionString());
			}
		}
		else if(inst instanceof CtableCPInstruction) {
			CtableCPInstruction cinst = (CtableCPInstruction) inst;
			if(inst.getOpcode().equalsIgnoreCase("ctable")
				&& ( ec.getCacheableData(cinst.input1).isFederated(FType.ROW)
				|| (cinst.input2.isMatrix() && ec.getCacheableData(cinst.input2).isFederated(FType.ROW))
				|| (cinst.input3.isMatrix() && ec.getCacheableData(cinst.input3).isFederated(FType.ROW))))
				fedinst = CtableFEDInstruction.parseInstruction(cinst.getInstructionString());
		}

		//set thread id for federated context management
		if( fedinst != null ) {
			fedinst.setTID(ec.getTID());
			return fedinst;
		}

		return inst;
	}

	public static Instruction checkAndReplaceSP(Instruction inst, ExecutionContext ec) {
		FEDInstruction fedinst = null;
		if (inst instanceof MapmmSPInstruction) {
			// FIXME does not yet work for MV multiplication. SPARK execution mode not supported for federated l2svm
			MapmmSPInstruction instruction = (MapmmSPInstruction) inst;
			Data data = ec.getVariable(instruction.input1);
			if (data instanceof MatrixObject && ((MatrixObject) data).isFederated()) {
				// TODO correct FED instruction string
				fedinst = new AggregateBinaryFEDInstruction(instruction.getOperator(),
					instruction.input1, instruction.input2, instruction.output, "ba+*", "FED...");
			}
		}
		else if (inst instanceof UnarySPInstruction) {
			if (inst instanceof CentralMomentSPInstruction) {
				CentralMomentSPInstruction instruction = (CentralMomentSPInstruction) inst;
				Data data = ec.getVariable(instruction.input1);
				if (data instanceof MatrixObject && ((MatrixObject) data).isFederated())
					fedinst = CentralMomentFEDInstruction.parseInstruction(inst.getInstructionString());
			} else if (inst instanceof QuantileSortSPInstruction) {
				QuantileSortSPInstruction instruction = (QuantileSortSPInstruction) inst;
				Data data = ec.getVariable(instruction.input1);
				if (data instanceof MatrixObject && ((MatrixObject) data).isFederated())
					fedinst = QuantileSortFEDInstruction.parseInstruction(inst.getInstructionString());
			}
			else if (inst instanceof AggregateUnarySPInstruction) {
				AggregateUnarySPInstruction instruction = (AggregateUnarySPInstruction) inst;
				Data data = ec.getVariable(instruction.input1);
				if(data instanceof MatrixObject && ((MatrixObject) data).isFederated())
					fedinst = AggregateUnaryFEDInstruction.parseInstruction(
						InstructionUtils.concatOperands(inst.getInstructionString(),FederatedOutput.NONE.name()));
			}
		}
		else if(inst instanceof BinarySPInstruction) {
			if(inst instanceof QuantilePickSPInstruction) {
				QuantilePickSPInstruction instruction = (QuantilePickSPInstruction) inst;
				Data data = ec.getVariable(instruction.input1);
				if(data instanceof MatrixObject && ((MatrixObject) data).isFederated())
					fedinst = QuantilePickFEDInstruction.parseInstruction(inst.getInstructionString());
			}
			else if (inst instanceof AppendGAlignedSPInstruction) {
				// TODO other Append Spark instructions
				AppendGAlignedSPInstruction instruction = (AppendGAlignedSPInstruction) inst;
				Data data = ec.getVariable(instruction.input1);
				if (data instanceof MatrixObject && ((MatrixObject) data).isFederated()) {
					fedinst = AppendFEDInstruction.parseInstruction(instruction.getInstructionString());
				}
			}
			else if (inst instanceof AppendGSPInstruction) {
				AppendGSPInstruction instruction = (AppendGSPInstruction) inst;
				Data data = ec.getVariable(instruction.input1);
				if(data instanceof MatrixObject && ((MatrixObject) data).isFederated()) {
					fedinst = AppendFEDInstruction.parseInstruction(instruction.getInstructionString());
				}
			}
			else if (inst instanceof BinaryMatrixScalarSPInstruction
				|| inst instanceof BinaryMatrixMatrixSPInstruction
				|| inst instanceof BinaryMatrixBVectorSPInstruction
				|| inst instanceof BinaryTensorTensorSPInstruction
				|| inst instanceof BinaryTensorTensorBroadcastSPInstruction) {
				BinarySPInstruction instruction = (BinarySPInstruction) inst;
				Data data = ec.getVariable(instruction.input1);
				if((data instanceof MatrixObject && ((MatrixObject)data).isFederated())
					|| (data instanceof TensorObject && ((TensorObject)data).isFederated())) {
					fedinst = BinaryFEDInstruction.parseInstruction(
						InstructionUtils.concatOperands(inst.getInstructionString(),FederatedOutput.NONE.name()));
				}
			}
		}
		else if (inst instanceof WriteSPInstruction) {
			WriteSPInstruction instruction = (WriteSPInstruction) inst;
			Data data = ec.getVariable(instruction.input1);
			if (data instanceof MatrixObject && ((MatrixObject) data).isFederated()) {
				// Write spark instruction can not be executed for federated matrix objects (tries to get rdds which do
				// not exist), therefore we replace the instruction with the VariableCPInstruction.
				return VariableCPInstruction.parseInstruction(instruction.getInstructionString());
			}
		}
		else if(inst instanceof QuaternarySPInstruction) {
			QuaternarySPInstruction instruction = (QuaternarySPInstruction) inst;
			Data data = ec.getVariable(instruction.input1);
			if(data instanceof MatrixObject && ((MatrixObject) data).isFederated())
				fedinst = QuaternaryFEDInstruction.parseInstruction(instruction.getInstructionString());
		}
		else if(inst instanceof SpoofSPInstruction) {
			SpoofSPInstruction ins = (SpoofSPInstruction) inst;
			Class<?> scla = ins.getOperatorClass().getSuperclass();
			if(((scla == SpoofCellwise.class || scla == SpoofMultiAggregate.class || scla == SpoofOuterProduct.class)
					&& SpoofFEDInstruction.isFederated(ec, ins.getInputs(), scla))
				|| (scla == SpoofRowwise.class && SpoofFEDInstruction.isFederated(ec, FType.ROW, ins.getInputs(), scla))) {
				fedinst = SpoofFEDInstruction.parseInstruction(inst.getInstructionString());
			}
		}
		//set thread id for federated context management
		if( fedinst != null ) {
			fedinst.setTID(ec.getTID());
			return fedinst;
		}

		return inst;
	}
}
