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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRange;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederatedUDF;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.functionobjects.COV;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CM_COV_Object;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.DoubleObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.COVOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;

public class CovarianceFEDInstruction extends BinaryFEDInstruction {
	private CovarianceFEDInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode,
		String istr) {
		super(FEDInstruction.FEDType.AggregateBinary, op, in1, in2, out, opcode, istr);
	}

	private CovarianceFEDInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
		String opcode, String istr) {
		super(FEDInstruction.FEDType.AggregateBinary, op, in1, in2, in3, out, opcode, istr);
	}


	public static CovarianceFEDInstruction parseInstruction(String str) {
		CPOperand in1 = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);
		CPOperand in2 = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);
		CPOperand in3 = null;
		CPOperand out = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);

		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if( !opcode.equalsIgnoreCase("cov") ) {
			throw new DMLRuntimeException("CovarianceCPInstruction.parseInstruction():: Unknown opcode " + opcode);
		}

		COVOperator cov = new COVOperator(COV.getCOMFnObject());
		if ( parts.length == 4 ) {
			parseBinaryInstruction(str, in1, in2, out);
			return new CovarianceFEDInstruction(cov, in1, in2, out, opcode, str);
		} else if ( parts.length == 5 ) {
			in3 = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);
			parseBinaryInstruction(str, in1, in2, in3, out);
			return new CovarianceFEDInstruction(cov, in1, in2, in3, out, opcode, str);
		}
		else {
			throw new DMLRuntimeException("Invalid number of arguments in Instruction: " + str);
		}
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject mo1 = ec.getMatrixObject(input1);
		MatrixObject mo2 = ec.getMatrixObject(input2);
		MatrixObject weights = input3 != null ? ec.getMatrixObject(input3) : null;

		if(mo1.isFederated() && mo2.isFederated() && !mo1.getFedMapping().isAligned(mo2.getFedMapping(), false))
			throw new DMLRuntimeException("Not supported matrix-matrix binary operation: covariance.");

		boolean moAligned = mo1.isFederated() && mo2.isFederated() && mo1.getFedMapping().isAligned(mo2.getFedMapping(), false);
		boolean weightsAligned = weights == null || (weights.isFederated() && mo2.isFederated() && weights.getFedMapping()
			.isAligned(mo2.getFedMapping(), false));

		// all aligned
		if(moAligned && weightsAligned)
			processAlignedFedCov(ec, mo1, mo2, weights);
		// weights are not aligned, broadcast
		else if(moAligned)
			processFedCovWeights(ec, mo1, mo2, weights);
		else
			processCov(ec, mo1, mo2);
	}

	private void processAlignedFedCov(ExecutionContext ec, MatrixObject mo1, MatrixObject mo2, MatrixObject mo3) {
		FederatedRequest fr1;
		if(mo3 == null)
			fr1 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2}, new long[]{mo1.getFedMapping().getID(), mo2.getFedMapping().getID()});
		else
			fr1 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2, input3}, new long[]{mo1.getFedMapping().getID(), mo2.getFedMapping().getID(), mo3.getFedMapping().getID()});

		FederatedRequest fr2 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fr1.getID());
		FederatedRequest fr3 = mo1.getFedMapping().cleanup(getTID(), fr1.getID());
		Future<FederatedResponse>[] covTmp = mo1.getFedMapping().execute(getTID(), fr1, fr2, fr3);

		//means
		Future<FederatedResponse>[] meanTmp1 = processMean(mo1, 0);
		Future<FederatedResponse>[] meanTmp2 = processMean(mo2, 1);

		ImmutableTriple<Double[], Double[], Double[]> res = getResponses(covTmp, meanTmp1, meanTmp2);

		double result = aggCov(res.left, res.middle, res.right, mo1.getFedMapping().getFederatedRanges());
		ec.setVariable(output.getName(), new DoubleObject(result));
	}

	private void processFedCovWeights(ExecutionContext ec, MatrixObject mo1, MatrixObject mo2, MatrixObject mo3) {

		FederatedRequest[] fr2 = mo1.getFedMapping().broadcastSliced(mo3, false, mo1.getUniqueID());
		FederatedRequest fr1 = FederationUtils.callInstruction(instString, output,
			new CPOperand[]{input1, input2}, new long[]{mo1.getFedMapping().getID(), mo2.getFedMapping().getID()});
		FederatedRequest fr3 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, fr1.getID());
		FederatedRequest fr4 = mo1.getFedMapping().cleanup(getTID(), fr1.getID(), fr2[0].getID());
		Future<FederatedResponse>[] covTmp = mo1.getFedMapping().execute(getTID(), fr1, fr2[0], fr3, fr4);

		//means
		Future<FederatedResponse>[] meanTmp1 = processMean(mo1, 0);
		Future<FederatedResponse>[] meanTmp2 = processMean(mo2, 1);

		ImmutableTriple<Double[], Double[], Double[]> res = getResponses(covTmp, meanTmp1, meanTmp2);

		double result = aggCov(res.left, res.middle, res.right, mo1.getFedMapping().getFederatedRanges());
		ec.setVariable(output.getName(), new DoubleObject(result));
	}

	private void processCov(ExecutionContext ec, MatrixObject mo1, MatrixObject mo2) {
		MatrixBlock mb;
		MatrixObject mo;
		COVOperator cop = ((COVOperator)_optr);

		if(!mo1.isFederated() && mo2.isFederated()) {
			mo = mo2;
			mb = ec.getMatrixInput(input1.getName());
		}
		else {
			mo = mo1;
			mb = ec.getMatrixInput(input2.getName());
		}

		FederationMap fedMapping = mo.getFedMapping();
		List<CM_COV_Object> globalCmobj = new ArrayList<>();
		long varID = FederationUtils.getNextFedDataID();
		fedMapping.mapParallel(varID, (range, data) -> {

			FederatedResponse response;
			try {
				if(input3 == null) {
					response = data.executeFederatedOperation(new FederatedRequest(FederatedRequest.RequestType.EXEC_UDF, -1,
						new CovarianceFEDInstruction.COVFunction(data.getVarID(),
							mb.slice(range.getBeginDimsInt()[0], range.getEndDimsInt()[0] - 1),
							cop))).get();
				}
				// with weights
				else {
					MatrixBlock wtBlock = ec.getMatrixInput(input2.getName());
					response = data.executeFederatedOperation(new FederatedRequest(FederatedRequest.RequestType.EXEC_UDF, -1,
						new CovarianceFEDInstruction.COVWeightsFunction(data.getVarID(),
							mb.slice(range.getBeginDimsInt()[0], range.getEndDimsInt()[0] - 1),
							cop, wtBlock))).get();
				}

				if(!response.isSuccessful())
					response.throwExceptionFromResponse();
				synchronized(globalCmobj) {
					globalCmobj.add((CM_COV_Object) response.getData()[0]);
				}
			}
			catch(Exception e) {
				throw new DMLRuntimeException(e);
			}
			return null;
		});

		Optional<CM_COV_Object> res = globalCmobj.stream().reduce((arg0, arg1) -> (CM_COV_Object) cop.fn.execute(arg0, arg1));
		try {
			ec.setScalarOutput(output.getName(), new DoubleObject(res.get().getRequiredResult(cop)));
		}
		catch(Exception e) {
			throw new DMLRuntimeException(e);
		}
	}

	private static ImmutableTriple<Double[], Double[], Double[]> getResponses(Future<FederatedResponse>[] covFfr, Future<FederatedResponse>[] mean1Ffr, Future<FederatedResponse>[] mean2Ffr) {
		Double[] cov = new Double[covFfr.length];
		Double[] mean1 = new Double[mean1Ffr.length];
		Double[] mean2 = new Double[mean2Ffr.length];
		IntStream.range(0, covFfr.length).forEach(i -> {
			try {
				cov[i] = ((ScalarObject) covFfr[i].get().getData()[0]).getDoubleValue();
				mean1[i] = ((ScalarObject) mean1Ffr[1].get().getData()[0]).getDoubleValue();
				mean2[i] = ((ScalarObject) mean2Ffr[2].get().getData()[0]).getDoubleValue();
			}
			catch(Exception e) {
				throw new DMLRuntimeException("CovarianceFEDInstruction: incorrect means or cov.");
			}
		});

		return new ImmutableTriple<>(cov, mean1, mean2);
	}

	private static double aggCov(Double[] covValues, Double[] mean1, Double[] mean2, FederatedRange[] ranges) {
		double cov = covValues[0];
		long size1 = ranges[0].getSize();
		double mean = (mean1[0] + mean2[0]) / 2;

		for(int i = 0; i < covValues.length - 1; i++) {
			long size2 = ranges[i+1].getSize();
			double nextMean = (mean1[i+1] + mean2[i+1]) / 2;
			double newMean = (size1 * mean + size2 * nextMean) / (size1 + size2);

			cov = (size1 * cov + size2 * covValues[i+1] + size1 * (mean - newMean) * (mean - newMean)
				+ size2 * (nextMean - newMean) * (nextMean - newMean)) / (size1 + size2);

			mean = newMean;
			size1 = size1 + size2;
		}
		return cov;
	}

	private Future<FederatedResponse>[] processMean(MatrixObject mo1, int var){
		String[] parts = instString.split("°");
		String meanInstr = instString.replace(getOpcode(), getOpcode().replace("cov", "uamean"));
		meanInstr = meanInstr.replace((var == 0 ? parts[2] : parts[3]) + "°", "");
		meanInstr = meanInstr.replace(parts[4], parts[4].replace("FP64", "STRING°16"));
		Future<FederatedResponse>[] meanTmp = null;

		//create federated commands for aggregation
		FederatedRequest meanFr1 = FederationUtils.callInstruction(meanInstr, output,
			new CPOperand[]{var == 0 ? input2 : input1}, new long[]{mo1.getFedMapping().getID()});
		FederatedRequest meanFr2 = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, meanFr1.getID());
		FederatedRequest meanFr3 = mo1.getFedMapping().cleanup(getTID(), meanFr1.getID());
		meanTmp = mo1.getFedMapping().execute(getTID(), meanFr1, meanFr2, meanFr3);
		return meanTmp;
	}

	private static class COVFunction extends FederatedUDF {

		private static final long serialVersionUID = -501036588060113499L;
		private final MatrixBlock _mo2;
		private final COVOperator _op;

		public COVFunction (long input, MatrixBlock mo2, COVOperator op) {
			super(new long[] {input});
			_op = op;
			_mo2 = mo2;
		}

		@Override
		public FederatedResponse execute(ExecutionContext ec, Data... data) {
			MatrixBlock mb = ((MatrixObject) data[0]).acquireReadAndRelease();
			return new FederatedResponse(FederatedResponse.ResponseType.SUCCESS, mb.covOperations(_op, _mo2));
		}

		@Override public Pair<String, LineageItem> getLineageItem(ExecutionContext ec) {
			return null;
		}
	}

	private static class COVWeightsFunction extends FederatedUDF {
		private static final long serialVersionUID = -1768739786192949573L;
		private final COVOperator _op;
		private final MatrixBlock _mo2;
		private final MatrixBlock _weights;

		protected COVWeightsFunction(long input, MatrixBlock mo2, COVOperator op, MatrixBlock weights) {
			super(new long[] {input});
			_mo2 = mo2;
			_op = op;
			_weights = weights;
		}

		@Override
		public FederatedResponse execute(ExecutionContext ec, Data... data) {
			MatrixBlock mb = ((MatrixObject) data[0]).acquireReadAndRelease();
			return new FederatedResponse(FederatedResponse.ResponseType.SUCCESS, mb.covOperations(_op, _mo2, _weights));
		}

		@Override public Pair<String, LineageItem> getLineageItem(ExecutionContext ec) {
			return null;
		}
	}
}
