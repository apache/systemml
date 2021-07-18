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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.parser.DMLProgram;
import org.apache.sysds.parser.DataIdentifier;
import org.apache.sysds.parser.Statement;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.FunctionProgramBlock;
import org.apache.sysds.runtime.controlprogram.caching.LazyWriteBuffer;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.stat.Timing;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.FunctionCallCPInstruction;

public abstract class PSWorker implements Serializable 
{
	private static final long serialVersionUID = -3510485051178200118L;

	// thread pool for asynchronous accrue gradients on epoch scheduling
	// Note: we use a non-static variable to obtain the live maintenance thread pool
	// which is important in scenarios w/ multiple scripts in a single JVM (e.g., tests)
	protected ExecutorService _tpool = LazyWriteBuffer.getUtilThreadPool();
	
	protected int _workerID;
	protected int _epochs;
	protected long _batchSize;
	protected ExecutionContext _ec;
	protected ParamServer _ps;
	protected DataIdentifier _output;
	protected FunctionCallCPInstruction _inst;
	protected MatrixObject _features;
	protected MatrixObject _labels;
	protected String _updFunc;
	protected Statement.PSFrequency _freq;
	protected boolean _modelAvg;

	protected PSWorker() {}

	protected PSWorker(int workerID, String updFunc, Statement.PSFrequency freq, int epochs, long batchSize, ExecutionContext ec, ParamServer ps,boolean modelAvg) {
		_workerID = workerID;
		_updFunc = updFunc;
		_freq = freq;
		_epochs = epochs;
		_batchSize = batchSize;
		_ec = ec;
		_ps = ps;
		_modelAvg = modelAvg;
		setupUpdateFunction(updFunc, ec);
	}

	protected void setupUpdateFunction(String updFunc, ExecutionContext ec) {
		// Get the update function
		String[] cfn = DMLProgram.splitFunctionKey(updFunc);
		String ns = cfn[0];
		String fname = cfn[1];
		boolean opt = !ec.getProgram().containsFunctionProgramBlock(ns, fname, false);
		FunctionProgramBlock func = ec.getProgram().getFunctionProgramBlock(ns, fname, opt);
		ArrayList<DataIdentifier> inputs = func.getInputParams();
		ArrayList<DataIdentifier> outputs = func.getOutputParams();
		CPOperand[] boundInputs = inputs.stream()
			.map(input -> new CPOperand(input.getName(), input.getValueType(), input.getDataType()))
			.toArray(CPOperand[]::new);
		ArrayList<String> outputNames = outputs.stream().map(DataIdentifier::getName)
			.collect(Collectors.toCollection(ArrayList::new));
		_inst = new FunctionCallCPInstruction(ns, fname, opt, boundInputs,
			func.getInputParamNames(), outputNames, "update function");

		// Check the inputs of the update function
		checkInput(false, inputs, DataType.MATRIX, Statement.PS_FEATURES);
		checkInput(false, inputs, DataType.MATRIX, Statement.PS_LABELS);
		checkInput(false, inputs, DataType.LIST, Statement.PS_MODEL);
		checkInput(true, inputs, DataType.LIST, Statement.PS_HYPER_PARAMS);

		// Check the output of the update function
		if (outputs.size() != 1) {
			throw new DMLRuntimeException(String.format("The output of the '%s' function "
				+ "should provide one list containing the gradients.", updFunc));
		}
		if (outputs.get(0).getDataType() != DataType.LIST) {
			throw new DMLRuntimeException(String.format("The output of the '%s' function should be type of list.", updFunc));
		}
		_output = outputs.get(0);
	}

	private void checkInput(boolean optional, ArrayList<DataIdentifier> inputs, DataType dt, String pname) {
		if (optional && inputs.stream().noneMatch(input -> pname.equals(input.getName()))) {
			// We do not need to check if the input is optional and is not provided
			return;
		}
		if (inputs.stream().filter(input -> input.getDataType() == dt && pname.equals(input.getName())).count() != 1) {
			throw new DMLRuntimeException(String.format("The '%s' function should provide "
				+ "an input of '%s' type named '%s'.", _updFunc, dt, pname));
		}
	}

	public void setFeatures(MatrixObject features) {
		_features = features;
	}

	public void setLabels(MatrixObject labels) {
		_labels = labels;
	}

	public MatrixObject getFeatures() {
		return _features;
	}

	public MatrixObject getLabels() {
		return _labels;
	}

	public abstract String getWorkerName();

	/**
	 * ----- The following methods are dedicated to statistics -------------
 	 */
	protected abstract void incWorkerNumber();

	protected abstract void accLocalModelUpdateTime(Timing time);

	protected abstract void accBatchIndexingTime(Timing time);

	protected abstract void accGradientComputeTime(Timing time);

	protected void accNumEpochs(int n) {
		//do nothing
	}
	
	protected void accNumBatches(int n) {
		//do nothing
	}
}
