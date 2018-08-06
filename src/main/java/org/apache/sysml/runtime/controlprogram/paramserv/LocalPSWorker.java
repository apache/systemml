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

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.parfor.stat.Timing;
import org.apache.sysml.runtime.instructions.cp.ListObject;
import org.apache.sysml.utils.Statistics;

public class LocalPSWorker extends PSWorker implements Callable<Void> {

	protected static final Log LOG = LogFactory.getLog(LocalPSWorker.class.getName());
	private static final long serialVersionUID = 5195390748495357295L;

	protected LocalPSWorker() {}

	public LocalPSWorker(int workerID, String updFunc, Statement.PSFrequency freq, int epochs, long batchSize, ExecutionContext ec, ParamServer ps) {
		super(workerID, updFunc, freq, epochs, batchSize, ec, ps);
	}

	@Override
	public String getWorkerName() {
		return String.format("Local worker_%d", _workerID);
	}
	
	@Override
	public Void call() throws Exception {
		incWorkerNumber();
		try {
			long dataSize = _features.getNumRows();
			int batchIter = (int) Math.ceil((double) dataSize / _batchSize);

			switch (_freq) {
				case BATCH:
					computeBatch(dataSize, batchIter);
					break;
				case EPOCH:
					computeEpoch(dataSize, batchIter);
					break;
				default:
					throw new DMLRuntimeException(String.format("%s not support update frequency %s", getWorkerName(), _freq));
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("%s: job finished.", getWorkerName()));
			}
		} catch (Exception e) {
			throw new DMLRuntimeException(String.format("%s failed", getWorkerName()), e);
		}
		return null;
	}

	private void computeEpoch(long dataSize, int batchIter) {
		for (int i = 0; i < _epochs; i++) {
			// Pull the global parameters from ps
			ListObject params = pullModel();
			ListObject accGradients = null;
			
			for (int j = 0; j < batchIter; j++) {
				ListObject gradients = computeGradients(params, dataSize, batchIter, i, j);

				boolean localUpdate = j < batchIter - 1;
				// Accumulate the intermediate gradients
				accGradients = ParamservUtils.accrueGradients(accGradients, gradients, !localUpdate);

				// Update the local model with gradients
				if(localUpdate)
					params = updateModel(params, gradients, i, j, batchIter);

				accNumBatches(1);
			}

			// Push the gradients to ps
			pushGradients(accGradients);
			ParamservUtils.cleanupListObject(_ec, Statement.PS_MODEL);

			accNumEpochs(1);
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("%s: finished %d epoch.", getWorkerName(), i + 1));
			}
		}
	}

	private ListObject updateModel(ListObject globalParams, ListObject gradients, int i, int j, int batchIter) {
		Timing tUpd = DMLScript.STATISTICS ? new Timing(true) : null;

		globalParams = _ps.updateLocalModel(_ec, gradients, globalParams);

		accLocalModelUpdateTime(tUpd);
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("%s: local global parameter [size:%d kb] updated. "
				+ "[Epoch:%d  Total epoch:%d  Iteration:%d  Total iteration:%d]",
				getWorkerName(), globalParams.getDataSize(), i + 1, _epochs, j + 1, batchIter));
		}
		return globalParams;
	}

	private void computeBatch(long dataSize, int totalIter) {
		for (int i = 0; i < _epochs; i++) {
			for (int j = 0; j < totalIter; j++) {
				ListObject globalParams = pullModel();

				ListObject gradients = computeGradients(globalParams, dataSize, totalIter, i, j);

				// Push the gradients to ps
				pushGradients(gradients);
				ParamservUtils.cleanupListObject(_ec, Statement.PS_MODEL);
				
				accNumBatches(1);
			}
			
			accNumEpochs(1);
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("%s: finished %d epoch.", getWorkerName(), i + 1));
			}
		}
	}

	private ListObject pullModel() {
		// Pull the global parameters from ps
		ListObject globalParams = _ps.pull(_workerID);
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("%s: successfully pull the global parameters "
				+ "[size:%d kb] from ps.", getWorkerName(), globalParams.getDataSize() / 1024));
		}
		return globalParams;
	}

	private void pushGradients(ListObject gradients) {
		// Push the gradients to ps
		_ps.push(_workerID, gradients);
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("%s: successfully push the gradients "
				+ "[size:%d kb] to ps.", getWorkerName(), gradients.getDataSize() / 1024));
		}
	}

	private ListObject computeGradients(ListObject params, long dataSize, int batchIter, int i, int j) {
		_ec.setVariable(Statement.PS_MODEL, params);
		long begin = j * _batchSize + 1;
		long end = Math.min((j + 1) * _batchSize, dataSize);

		// Get batch features and labels
		Timing tSlic = DMLScript.STATISTICS ? new Timing(true) : null;
		MatrixObject bFeatures = ParamservUtils.sliceMatrix(_features, begin, end);
		MatrixObject bLabels = ParamservUtils.sliceMatrix(_labels, begin, end);
		accBatchIndexingTime(tSlic);

		_ec.setVariable(Statement.PS_FEATURES, bFeatures);
		_ec.setVariable(Statement.PS_LABELS, bLabels);

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("%s: got batch data [size:%d kb] of index from %d to %d [last index: %d]. "
				+ "[Epoch:%d  Total epoch:%d  Iteration:%d  Total iteration:%d]", getWorkerName(),
				bFeatures.getDataSize() / 1024 + bLabels.getDataSize() / 1024, begin, end, dataSize, i + 1, _epochs,
				j + 1, batchIter));
		}

		// Invoke the update function
		Timing tGrad = DMLScript.STATISTICS ? new Timing(true) : null;
		_inst.processInstruction(_ec);
		accGradientComputeTime(tGrad);

		// Get the gradients
		ListObject gradients = _ec.getListObject(_output.getName());

		ParamservUtils.cleanupData(_ec, Statement.PS_FEATURES);
		ParamservUtils.cleanupData(_ec, Statement.PS_LABELS);
		return gradients;
	}
	
	@Override
	protected void incWorkerNumber() {
		if (DMLScript.STATISTICS)
			Statistics.incWorkerNumber();
	}

	@Override
	protected void accLocalModelUpdateTime(Timing time) {
		if (DMLScript.STATISTICS)
			Statistics.accPSLocalModelUpdateTime((long) time.stop());
	}

	@Override
	protected void accBatchIndexingTime(Timing time) {
		if (DMLScript.STATISTICS)
			Statistics.accPSBatchIndexingTime((long) time.stop());
	}

	@Override
	protected void accGradientComputeTime(Timing time) {
		if (DMLScript.STATISTICS)
			Statistics.accPSGradientComputeTime((long) time.stop());
	}
}
