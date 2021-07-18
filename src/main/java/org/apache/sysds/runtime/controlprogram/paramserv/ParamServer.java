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

package org.apache.sysds.runtime.controlprogram.paramserv;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.parser.DMLProgram;
import org.apache.sysds.parser.DataIdentifier;
import org.apache.sysds.parser.Statement;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.FunctionProgramBlock;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.stat.Timing;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.functionobjects.Divide;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.*;
import org.apache.sysds.runtime.lineage.Lineage;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixValue;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.matrix.operators.ScalarOperator;
import org.apache.sysds.runtime.util.IndexRange;
import org.apache.sysds.utils.Statistics;

import org.dmg.pmml.Model;
import scala.tools.nsc.doc.model.Object;


import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.lang.Math;


public abstract class ParamServer
{
	protected static final Log LOG = LogFactory.getLog(ParamServer.class.getName());
	protected static final boolean ACCRUE_BSP_GRADIENTS = true;

	// worker input queues and global model
	protected Map<Integer, BlockingQueue<ListObject>> _modelMap;
	private ListObject _model;

	//aggregation service
	protected ExecutionContext _ec;
	private Statement.PSUpdateType _updateType;
	private Statement.PSFrequency _freq;

	private FunctionCallCPInstruction _inst;
	private String _outputName;
	private boolean[] _finishedStates;  // Workers' finished states
	private ListObject _accGradients = null;

	private boolean _validationPossible;
	private FunctionCallCPInstruction _valInst;
	private String _lossOutput;
	private String _accuracyOutput;

	private int _syncCounter = 0;
	private int _epochCounter = 0 ;
	private int _numBatchesPerEpoch;
	private boolean _modelAvg;

	private int _numWorkers;
	private ListObject _accModel = null;
	private Object sum;
	private BinaryOperator _op2;
	private MatrixObject AvgModel;


	protected ParamServer() {}

	protected ParamServer(ListObject model, String aggFunc, Statement.PSUpdateType updateType,
						  Statement.PSFrequency freq, ExecutionContext ec, int workerNum, String valFunc,
						  int numBatchesPerEpoch, MatrixObject valFeatures, MatrixObject valLabels, boolean modelAvg)
	{

		// init worker queues and global model
		_modelMap = new HashMap<>(workerNum);
		IntStream.range(0, workerNum).forEach(i -> {
			// Create a single element blocking queue for workers to receive the broadcasted model
			_modelMap.put(i, new ArrayBlockingQueue<>(1));
		});
		_model = model;
		_modelAvg = modelAvg;

		// init aggregation service
		_ec = ec;
		_updateType = updateType;
		_freq = freq;
		_finishedStates = new boolean[workerNum];
		setupAggFunc(_ec, aggFunc);

		if(valFunc != null && numBatchesPerEpoch > 0 && valFeatures != null && valLabels != null) {
			setupValFunc(_ec, valFunc, valFeatures, valLabels);
		}
		_numBatchesPerEpoch = numBatchesPerEpoch;
		_numWorkers = workerNum;

		// broadcast initial model
		broadcastModel(true);
	}

	protected void setupAggFunc(ExecutionContext ec, String aggFunc) {
		String[] cfn = DMLProgram.splitFunctionKey(aggFunc);
		String ns = cfn[0];
		String fname = cfn[1];
		boolean opt = !ec.getProgram().containsFunctionProgramBlock(ns, fname, false);
		FunctionProgramBlock func = ec.getProgram().getFunctionProgramBlock(ns, fname, opt);
		ArrayList<DataIdentifier> inputs = func.getInputParams();
		ArrayList<DataIdentifier> outputs = func.getOutputParams();

		// Check the output of the aggregation function
		if (outputs.size() != 1) {
			throw new DMLRuntimeException(String.format("The output of the '%s' function should provide one list containing the updated model.", aggFunc));
		}
		if (outputs.get(0).getDataType() != DataType.LIST) {
			throw new DMLRuntimeException(String.format("The output of the '%s' function should be type of list.", aggFunc));
		}
		_outputName = outputs.get(0).getName();

		CPOperand[] boundInputs = inputs.stream()
				.map(input -> new CPOperand(input.getName(), input.getValueType(), input.getDataType()))
				.toArray(CPOperand[]::new);
		ArrayList<String> outputNames = outputs.stream().map(DataIdentifier::getName)
				.collect(Collectors.toCollection(ArrayList::new));
		_inst = new FunctionCallCPInstruction(ns, fname, opt, boundInputs,
				func.getInputParamNames(), outputNames, "aggregate function");
	}

	protected void setupValFunc(ExecutionContext ec, String valFunc, MatrixObject valFeatures, MatrixObject valLabels) {
		String[] cfn = DMLProgram.splitFunctionKey(valFunc);
		String ns = cfn[0];
		String fname = cfn[1];
		boolean opt = !ec.getProgram().containsFunctionProgramBlock(ns, fname, false);
		FunctionProgramBlock func = ec.getProgram().getFunctionProgramBlock(ns, fname, opt);
		ArrayList<DataIdentifier> inputs = func.getInputParams();
		ArrayList<DataIdentifier> outputs = func.getOutputParams();

		// Check the output of the validate function
		if (outputs.size() != 2) {
			throw new DMLRuntimeException(String.format("The output of the '%s' function should provide the loss and the accuracy in that order", valFunc));
		}
		if (outputs.get(0).getDataType() != DataType.SCALAR || outputs.get(1).getDataType() != DataType.SCALAR) {
			throw new DMLRuntimeException(String.format("The outputs of the '%s' function should both be scalars", valFunc));
		}
		_lossOutput = outputs.get(0).getName();
		_accuracyOutput = outputs.get(1).getName();

		CPOperand[] boundInputs = inputs.stream()
				.map(input -> new CPOperand(input.getName(), input.getValueType(), input.getDataType()))
				.toArray(CPOperand[]::new);
		ArrayList<String> outputNames = outputs.stream().map(DataIdentifier::getName)
				.collect(Collectors.toCollection(ArrayList::new));
		_valInst = new FunctionCallCPInstruction(ns, fname, opt, boundInputs,
				func.getInputParamNames(), outputNames, "validate function");

		// write validation data to execution context. hyper params are already in ec
		_ec.setVariable(Statement.PS_VAL_FEATURES, valFeatures);
		_ec.setVariable(Statement.PS_VAL_LABELS, valLabels);

		_validationPossible = true;
	}

	public abstract void push(int workerID, ListObject value);

	public abstract ListObject pull(int workerID);

	public ListObject getResult() {
		// All the model updating work has terminated,
		// so we could return directly the result model
		return _model;
	}

	protected synchronized void updModel_avgModel(int workerID, ListObject params){
		if (_modelAvg == true){
			updateAverageModel(workerID, params);}
		else if (_modelAvg == false)
			updateGlobalModel(workerID, params);

}
	protected  void updateAverageModel(int workerID, ListObject models) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Successfully pulled the gradients [size:%d kb] of worker_%d.",
						models.getDataSize() / 1024, workerID));
			}

			switch(_updateType) {
				case BSP: {
					setFinishedState(workerID);
					_accModel = ParamservUtils.accrueModels(_accModel, models, true);

					if (allFinished()) {
						averageGlobalModel(_accModel);
						_accModel = null;

						// This if has grown to be quite complex its function is rather simple. Validate at the end of each epoch
						// In the BSP batch case that occurs after the sync counter reaches the number of batches and in the
						// BSP epoch case every time
						if (_numBatchesPerEpoch != -1 &&
								(_freq == Statement.PSFrequency.EPOCH ||
										(_freq == Statement.PSFrequency.BATCH && ++_syncCounter % _numBatchesPerEpoch == 0))) {

							if(LOG.isInfoEnabled())
								LOG.info("[+] PARAMSERV: completed EPOCH " + _epochCounter);

							time_epoch();
							if(_validationPossible)
								validate();
							_epochCounter++;
							_syncCounter = 0;

						}
						// Broadcast the updated model
						resetFinishedStates();

						broadcastModel(true);
						if (LOG.isDebugEnabled())
							LOG.debug("Global Averaging parameter is broadcasted successfully ");
					}
					break;
				}
				case ASP:
					throw new DMLRuntimeException("Unsupported update: " + _updateType.name()+"in the case of averaging model");
				default:
					throw new DMLRuntimeException("Unsupported update: " + _updateType.name());
			}
		}
		catch (Exception e) {
			throw new DMLRuntimeException("Aggregation or validation service failed: ", e);
		}
	}
	private void averageGlobalModel(ListObject accModel) {
		Timing tAgg = DMLScript.STATISTICS ? new Timing(true) : null;
		_model = averageModel(_ec,accModel, _model);

		if (DMLScript.STATISTICS && tAgg != null)
			Statistics.accPSAggregationTime((long) tAgg.stop());
	}
	/*********************************************************************************************************************
	 * A service method for averaging model with models
	 *
	 * @param ec execution context
	 * @param accModels list of models
	 * @param model old model
	 * @return new model (accModel)
	 */

	public static  ListObject averageModel(ExecutionContext ec, ListObject accModels,ListObject model) {

		Timing tAgg = DMLScript.STATISTICS ? new Timing(true) : null;

		ec.setVariable(Statement.PS_MODEL, model);
		ec.setVariable(Statement.PS_MODELS, accModels);
		boolean par = true;
		// first we accumulate all the models
		if (accModels == null)
			return ParamservUtils.copyList(model, true);
			IntStream range = IntStream.range(0, accModels.getLength());
		(par ? range.parallel() : range).forEach(i -> {

			MatrixBlock mb1 = ((MatrixObject) model.getData().get(i)).acquireReadAndRelease();
			MatrixBlock mb2 = ((MatrixObject) accModels.getData().get(i)).acquireReadAndRelease();
			mb2.binaryOperationsInPlace(new BinaryOperator(Plus.getPlusFnObject()), mb1);


		});


		ListObject tempModel = accModels;
		IntStream range1 = IntStream.range(0, tempModel.getLength());
		ListObject finalTempModel = tempModel;

		//second we calculate the averaging model
		(par ? range1.parallel() : range1).forEach(i -> {
					MatrixBlock mbTemp = ((MatrixObject) finalTempModel.getData().get(i)).acquireReadAndRelease();
		         	ScalarOperator sop = InstructionUtils.parseScalarBinaryOperator("/", false);
			        sop = sop.setConstant(accModels.getLength());// division would be done for averaging
					mbTemp.scalarOperations(sop, new MatrixBlock());

		});
	//	ParamservUtils.cleanupListObject(model);

		return accModels;
	}

	private void updateGlobalModel(ListObject gradients) {
		Timing tAgg = DMLScript.STATISTICS ? new Timing(true) : null;
		_model = updateLocalModel(_ec, gradients, _model);
		if (DMLScript.STATISTICS && tAgg != null)
			Statistics.accPSAggregationTime((long) tAgg.stop());
	}

	/**
	 * A service method for updating model with gradients
	 *
	 * @param ec execution context
	 * @param gradients list of gradients
	 * @param model old model
	 * @return new model
	 */
	protected ListObject updateLocalModel(ExecutionContext ec, ListObject gradients, ListObject model) {
		// Populate the variables table with the gradients and model
		ec.setVariable(Statement.PS_GRADIENTS, gradients);
		ec.setVariable(Statement.PS_MODEL, model);

		// Invoke the aggregate function
		_inst.processInstruction(ec);

		// Get the new model
		ListObject newModel = ec.getListObject(_outputName);

		// Clean up the list according to the data referencing status
		ParamservUtils.cleanupListObject(ec, Statement.PS_MODEL, newModel.getStatus());
		ParamservUtils.cleanupListObject(ec, Statement.PS_GRADIENTS);
		return newModel;
	}

	private boolean allFinished() {
		return !ArrayUtils.contains(_finishedStates, false);
	}

	private void resetFinishedStates() {
		Arrays.fill(_finishedStates, false);
	}

	private void setFinishedState(int workerID) {
		_finishedStates[workerID] = true;
	}

	/**
	 * Broadcast the model for all workers
	 */
	private void broadcastModel(boolean par) {
		IntStream stream = IntStream.range(0, _modelMap.size());
		(par ? stream.parallel() : stream).forEach(workerID -> {
			try {
				broadcastModel(workerID);
			} catch (InterruptedException e) {
				throw new DMLRuntimeException("Paramserv func: some error occurred when broadcasting model", e);
			}
		});
	}

	private void broadcastModel(int workerID) throws InterruptedException {
		Timing tBroad = DMLScript.STATISTICS ? new Timing(true) : null;
		//broadcast copy of model to specific worker, cleaned up by worker
		_modelMap.get(workerID).put(ParamservUtils.copyList(_model, false));
		if (DMLScript.STATISTICS && tBroad != null)
			Statistics.accPSModelBroadcastTime((long) tBroad.stop());
	}

	/**
	 * Prints the time the epoch took to complete
	 */
	private void time_epoch() {
		if (DMLScript.STATISTICS) {
			//TODO double check correctness with multiple, potentially concurrent paramserv invocation
			Statistics.accPSExecutionTime((long) Statistics.getPSExecutionTimer().stop());
			double current_total_execution_time = Statistics.getPSExecutionTime();
			double current_total_validation_time = Statistics.getPSValidationTime();
			double time_to_epoch = current_total_execution_time - current_total_validation_time;

			if (LOG.isInfoEnabled())
				if(_validationPossible)
					LOG.info("[+] PARAMSERV: epoch timer (excl. validation): " + time_to_epoch / 1000 + " secs.");
				else
					LOG.info("[+] PARAMSERV: epoch timer: " + time_to_epoch / 1000 + " secs.");
		}
	}

	/**
	 * Checks the current model against the validation set
	 */
	private void validate() {
		Timing tValidate = DMLScript.STATISTICS ? new Timing(true) : null;
		_ec.setVariable(Statement.PS_MODEL, _model);
		// Invoke the validation function
		_valInst.processInstruction(_ec);

		// Get the validation results
		double loss = ((DoubleObject) _ec.getVariable(_lossOutput)).getDoubleValue();
		double accuracy = ((DoubleObject) _ec.getVariable(_accuracyOutput)).getDoubleValue();

		// cleanup
		ParamservUtils.cleanupListObject(_ec, Statement.PS_MODEL);

		// Log validation results
		if (LOG.isInfoEnabled())
			LOG.info("[+] PARAMSERV: validation-loss: " + loss + " validation-accuracy: " + accuracy);

		if(tValidate != null)
			Statistics.accPSValidationTime((long) tValidate.stop());
	}

	public FunctionCallCPInstruction getAggInst() {
		return _inst;
	}

	protected  void updateGlobalModel(int workerID, ListObject gradients) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Successfully pulled the gradients [size:%d kb] of worker_%d.",
						gradients.getDataSize() / 1024, workerID));
			}

			switch(_updateType) {
				case BSP: {
					setFinishedState(workerID);

					// Accumulate the intermediate gradients
					if( ACCRUE_BSP_GRADIENTS )

						_accGradients = ParamservUtils.accrueGradients(_accGradients, gradients, true);
					else
						updateGlobalModel(gradients);

					if (allFinished()) {
						// Update the global model with accrued gradients
						if( ACCRUE_BSP_GRADIENTS ) {
							updateGlobalModel(_accGradients);
							_accGradients = null;
						}
						// This if has grown to be quite complex its function is rather simple. Validate at the end of each epoch
						// In the BSP batch case that occurs after the sync counter reaches the number of batches and in the
						// BSP epoch case every time
						if (_numBatchesPerEpoch != -1 &&
								(_freq == Statement.PSFrequency.EPOCH ||
										(_freq == Statement.PSFrequency.BATCH && ++_syncCounter % _numBatchesPerEpoch == 0))) {

							if(LOG.isInfoEnabled())
								LOG.info("[+] PARAMSERV: completed EPOCH " + _epochCounter);

							time_epoch();

							if(_validationPossible)
								validate();

							_epochCounter++;
							_syncCounter = 0;
						}

						// Broadcast the updated model
						resetFinishedStates();
						broadcastModel(true);
						if (LOG.isDebugEnabled())
							LOG.debug("Global parameter is broadcasted successfully.");
					}
					break;
				}
				case ASP: {
					updateGlobalModel(gradients);
					// This works similarly to the one for BSP, but divides the sync counter by
					// the number of workers, creating "Pseudo Epochs"
					if (_numBatchesPerEpoch != -1 &&
							((_freq == Statement.PSFrequency.EPOCH && ((float) ++_syncCounter % _numWorkers) == 0) ||
									(_freq == Statement.PSFrequency.BATCH && ((float) ++_syncCounter / _numWorkers) % (float) _numBatchesPerEpoch == 0))) {

						if(LOG.isInfoEnabled())
							LOG.info("[+] PARAMSERV: completed PSEUDO EPOCH (ASP) " + _epochCounter);

						time_epoch();

						if(_validationPossible)
							validate();

						_epochCounter++;
						_syncCounter = 0;
					}

					broadcastModel(workerID);
					break;
				}
				default:
					throw new DMLRuntimeException("Unsupported update: " + _updateType.name());
			}
		}
		catch (Exception e) {
			throw new DMLRuntimeException("Aggregation or validation service failed: ", e);
		}
	}
}
