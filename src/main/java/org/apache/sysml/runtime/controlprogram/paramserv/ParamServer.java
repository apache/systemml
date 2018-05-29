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

package org.apache.sysml.runtime.controlprogram.paramserv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.DataIdentifier;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.FunctionProgramBlock;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.FunctionCallCPInstruction;
import org.apache.sysml.runtime.instructions.cp.ListObject;

public abstract class ParamServer {

	public class Gradient {
		final long _workerID;
		final ListObject _gradients;

		public Gradient(long workerID, ListObject gradients) {
			this._workerID = workerID;
			this._gradients = gradients;
		}
	}

	BlockingQueue<Gradient> _gradientsQueue;
	Map<Integer, BlockingQueue<ListObject>> _modelMap;
	private ListObject _model;
	private AggregationService _aggService;
	private Thread _aggThread;
	private boolean[] _pulledStates;

	protected ParamServer(ListObject model, String aggFunc, Statement.PSFrequency freq,
			Statement.PSUpdateType updateType, ExecutionContext ec, int workerNum, ListObject hyperParams) {
		this._gradientsQueue = new LinkedBlockingDeque<>();
		this._modelMap = new HashMap<>(workerNum);
		IntStream.range(0, workerNum).forEach(i -> {
			BlockingQueue<ListObject> bq = new ArrayBlockingQueue<>(1);
			try {
				bq.put(model);
			} catch (InterruptedException e) {
				throw new DMLRuntimeException(
						String.format("Param server: failed to broadcast the model for worker_%d", i), e);
			}
			_modelMap.put(i, bq);
		});
		this._model = model;
		this._aggService = new AggregationService(aggFunc, freq, updateType, ec, workerNum, hyperParams);
		this._pulledStates = new boolean[workerNum];
		this._aggThread = new Thread(_aggService);
	}

	public abstract void push(long workerID, ListObject value);

	public abstract Data pull(long workerID);

	public void start() {
		_aggService._alive = true;
		_aggThread.start();
	}

	public void stop() {
		_aggService._alive = false;
		try {
			_aggThread.join();
		} catch (InterruptedException e) {
			throw new DMLRuntimeException("Parameter server: failed when stopping the server.", e);
		}
	}

	public ListObject getResult() {
		return _model;
	}

	public boolean getPulledState(int workerID) {
		return _pulledStates[workerID];
	}

	public void setPulledState(int workerID, boolean state) {
		_pulledStates[workerID] = state;
	}

	private void resetPulledStates() {
		_pulledStates = new boolean[_pulledStates.length];
	}

	/**
	 * Inner aggregation service which is for updating the model
	 */
	@SuppressWarnings("unused")
	private class AggregationService implements Runnable {

		protected final Log LOG = LogFactory.getLog(AggregationService.class.getName());

		protected ExecutionContext _ec;
		private Statement.PSFrequency _freq;
		private Statement.PSUpdateType _updateType;
		private FunctionCallCPInstruction _inst;
		private DataIdentifier _output;
		private boolean _alive;
		private boolean[] _finishedStates;  // Workers' finished states

		AggregationService(String aggFunc, Statement.PSFrequency freq, Statement.PSUpdateType updateType,
				ExecutionContext ec, int workerNum, ListObject hyperParams) {
			_ec = ExecutionContextFactory.createContext(ec.getProgram());
			_freq = freq;
			_updateType = updateType;
			if (hyperParams != null) {
				_ec.setVariable(Statement.PS_HYPER_PARAMS, hyperParams);
			}
			_finishedStates = new boolean[workerNum];

			// Fetch the aggregation function
			String[] keys = DMLProgram.splitFunctionKey(aggFunc);
			String funcName = keys[0];
			String funcNS = null;
			if (keys.length == 2) {
				funcNS = keys[0];
				funcName = keys[1];
			}
			FunctionProgramBlock func = _ec.getProgram().getFunctionProgramBlock(funcNS, funcName);
			ArrayList<DataIdentifier> inputs = func.getInputParams();
			ArrayList<DataIdentifier> outputs = func.getOutputParams();

			// Check the output of the aggregation function
			if (outputs.size() != 1) {
				throw new DMLRuntimeException(String.format(
						"The output of the '%s' function should provide one list containing the updated model.",
						aggFunc));
			}
			if (outputs.get(0).getDataType() != Expression.DataType.LIST) {
				throw new DMLRuntimeException(
						String.format("The output of the '%s' function should be type of list.", aggFunc));
			}
			_output = outputs.get(0);

			CPOperand[] boundInputs = inputs.stream()
					.map(input -> new CPOperand(input.getName(), input.getValueType(), input.getDataType()))
					.toArray(CPOperand[]::new);
			ArrayList<String> inputNames = inputs.stream().map(DataIdentifier::getName)
					.collect(Collectors.toCollection(ArrayList::new));
			ArrayList<String> outputNames = outputs.stream().map(DataIdentifier::getName)
					.collect(Collectors.toCollection(ArrayList::new));
			_inst = new FunctionCallCPInstruction(funcNS, funcName, boundInputs, inputNames, outputNames,
					"aggregate function");
		}

		boolean isAlive() {
			return _alive;
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

		private void broadcastModel() {
			IntStream.range(0, _finishedStates.length).forEach(i -> _modelMap.compute(i, (id, q) -> {
				if (q == null) {
					q = new ArrayBlockingQueue<>(1);
				}
				try {
					q.put(_model);
				} catch (InterruptedException e) {
					throw new DMLRuntimeException(
							String.format("Param server: failed to broadcast the model for worker_%d", id), e);
				}
				return q;
			}));
		}

		@Override
		public void run() {
			while (isAlive()) {
				do {
					Gradient p;
					try {
						p = _gradientsQueue.take();
					} catch (InterruptedException e) {
						throw new DMLRuntimeException(
								"Aggregation service: error when waiting for the coming gradients.", e);
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug(String.format("Successfully pulled the gradients [size:%d kb] of worker_%d.",
								p._gradients.getDataSize() / 1024, p._workerID));
					}

					setFinishedState((int) p._workerID);

					// Populate the variables table with the gradients and model
					_ec.setVariable(Statement.PS_GRADIENTS, p._gradients);
					_ec.setVariable(Statement.PS_MODEL, _model);

					// Invoke the aggregate function
					_inst.processInstruction(_ec);

					// Get the output
					ListObject newModel = (ListObject) _ec.getVariable(_output.getName());

					// Update the model with the new output
					ParamservUtils.cleanupListObject(_ec, _model);
					ParamservUtils.cleanupListObject(_ec, p._gradients);
					_model = newModel;

				} while (!allFinished());

				// Broadcast the updated model
				resetPulledStates();
				resetFinishedStates();
				broadcastModel();

				if (LOG.isDebugEnabled()) {
					LOG.debug("Global parameter is broadcasted successfully.");
				}
			}
		}
	}
}
