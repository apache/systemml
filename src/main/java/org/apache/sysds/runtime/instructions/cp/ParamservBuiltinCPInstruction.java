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

package org.apache.sysds.runtime.instructions.cp;

import static org.apache.sysds.parser.Statement.PS_AGGREGATION_FUN;
import static org.apache.sysds.parser.Statement.PS_BATCH_SIZE;
import static org.apache.sysds.parser.Statement.PS_EPOCHS;
import static org.apache.sysds.parser.Statement.PS_FEATURES;
import static org.apache.sysds.parser.Statement.PS_FREQUENCY;
import static org.apache.sysds.parser.Statement.PS_HYPER_PARAMS;
import static org.apache.sysds.parser.Statement.PS_LABELS;
import static org.apache.sysds.parser.Statement.PS_MODE;
import static org.apache.sysds.parser.Statement.PS_MODEL;
import static org.apache.sysds.parser.Statement.PS_PARALLELISM;
import static org.apache.sysds.parser.Statement.PS_SCHEME;
import static org.apache.sysds.parser.Statement.PS_UPDATE_FUN;
import static org.apache.sysds.parser.Statement.PS_UPDATE_TYPE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.util.LongAccumulator;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.hops.recompile.Recompiler;
import org.apache.sysds.lops.LopProperties;
import org.apache.sysds.parser.Statement.PSFrequency;
import org.apache.sysds.parser.Statement.PSModeType;
import org.apache.sysds.parser.Statement.PSScheme;
import org.apache.sysds.parser.Statement.FederatedPSScheme;
import org.apache.sysds.parser.Statement.PSUpdateType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.LocalVariableMap;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.controlprogram.paramserv.FederatedPSControlThread;
import org.apache.sysds.runtime.controlprogram.paramserv.LocalPSWorker;
import org.apache.sysds.runtime.controlprogram.paramserv.LocalParamServer;
import org.apache.sysds.runtime.controlprogram.paramserv.ParamServer;
import org.apache.sysds.runtime.controlprogram.paramserv.ParamservUtils;
import org.apache.sysds.runtime.controlprogram.paramserv.SparkPSBody;
import org.apache.sysds.runtime.controlprogram.paramserv.SparkPSWorker;
import org.apache.sysds.runtime.controlprogram.paramserv.dp.DataPartitionFederatedScheme;
import org.apache.sysds.runtime.controlprogram.paramserv.dp.DataPartitionLocalScheme;
import org.apache.sysds.runtime.controlprogram.paramserv.dp.FederatedDataPartitioner;
import org.apache.sysds.runtime.controlprogram.paramserv.dp.LocalDataPartitioner;
import org.apache.sysds.runtime.controlprogram.paramserv.rpc.PSRpcFactory;
import org.apache.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysds.runtime.controlprogram.parfor.stat.Timing;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.util.ProgramConverter;
import org.apache.sysds.utils.Statistics;

public class ParamservBuiltinCPInstruction extends ParameterizedBuiltinCPInstruction {
	private static final Log LOG = LogFactory.getLog(ParamservBuiltinCPInstruction.class.getName());
	
	private static final int DEFAULT_BATCH_SIZE = 64;
	private static final PSFrequency DEFAULT_UPDATE_FREQUENCY = PSFrequency.EPOCH;
	private static final PSScheme DEFAULT_SCHEME = PSScheme.DISJOINT_CONTIGUOUS;
	private static final FederatedPSScheme DEFAULT_FEDERATED_SCHEME = FederatedPSScheme.KEEP_DATA_ON_WORKER;
	private static final PSModeType DEFAULT_MODE = PSModeType.LOCAL;
	private static final PSUpdateType DEFAULT_TYPE = PSUpdateType.ASP;

	public ParamservBuiltinCPInstruction(Operator op, LinkedHashMap<String, String> paramsMap, CPOperand out, String opcode, String istr) {
		super(op, paramsMap, out, opcode, istr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		// check if the input is federated
		if(ec.getMatrixObject(getParam(PS_FEATURES)).isFederated() ||
				ec.getMatrixObject(getParam(PS_LABELS)).isFederated()) {
			runFederated(ec);
		}
		// if not federated check mode
		else {
			PSModeType mode = getPSMode();
			switch (mode) {
				case LOCAL:
					runLocally(ec, mode);
					break;
				case REMOTE_SPARK:
					runOnSpark((SparkExecutionContext) ec, mode);
					break;
				default:
					throw new DMLRuntimeException(String.format("Paramserv func: not support mode %s", mode));
			}
		}
	}

	private void runFederated(ExecutionContext ec) {
		System.out.println("PARAMETER SERVER");
		System.out.println("[+] Running in federated mode");

		// get inputs
		PSFrequency freq = getFrequency();
		PSUpdateType updateType = getUpdateType();
		FederatedPSScheme federatedPSScheme = getFederatedScheme();
		String updFunc = getParam(PS_UPDATE_FUN);
		String aggFunc = getParam(PS_AGGREGATION_FUN);

		// partition federated data
		DataPartitionFederatedScheme.Result result = new FederatedDataPartitioner(federatedPSScheme)
				.doPartitioning(ec.getMatrixObject(getParam(PS_FEATURES)), ec.getMatrixObject(getParam(PS_LABELS)));
		List<MatrixObject> pFeatures = result.pFeatures;
		List<MatrixObject> pLabels = result.pLabels;
		int workerNum = result.workerNum;

		// setup threading
		BasicThreadFactory factory = new BasicThreadFactory.Builder()
				.namingPattern("workers-pool-thread-%d").build();
		ExecutorService es = Executors.newFixedThreadPool(workerNum, factory);

		// Get the compiled execution context
		LocalVariableMap newVarsMap = createVarsMap(ec);
		// Level of par is -1 so each federated worker can scale to its cpu cores
		ExecutionContext newEC = ParamservUtils.createExecutionContext(ec, newVarsMap, updFunc, aggFunc, -1, true);
		// Create workers' execution context
		List<ExecutionContext> federatedWorkerECs = ParamservUtils.copyExecutionContext(newEC, workerNum);
		// Create the agg service's execution context
		ExecutionContext aggServiceEC = ParamservUtils.copyExecutionContext(newEC, 1).get(0);
		// Create the parameter server
		ListObject model = ec.getListObject(getParam(PS_MODEL));
		ParamServer ps = createPS(PSModeType.FEDERATED, aggFunc, updateType, workerNum, model, aggServiceEC);
		// Create the local workers
		List<FederatedPSControlThread> threads = IntStream.range(0, workerNum)
				.mapToObj(i -> new FederatedPSControlThread(i, updFunc, freq, getEpochs(), getBatchSize(), federatedWorkerECs.get(i), ps))
				.collect(Collectors.toList());

		if(workerNum != threads.size()) {
			throw new DMLRuntimeException("ParamservBuiltinCPInstruction: Federated data partitioning does not match threads!");
		}

		// Set features and lables for the control threads and write the program and instructions and hyperparams to the federated workers
		for (int i = 0; i < threads.size(); i++) {
			threads.get(i).setFeatures(pFeatures.get(i));
			threads.get(i).setLabels(pLabels.get(i));
			threads.get(i).setup();
		}

		try {
			// Launch the worker threads and wait for completion
			for (Future<Void> ret : es.invokeAll(threads))
				ret.get(); //error handling
			// Fetch the final model from ps
			ec.setVariable(output.getName(), ps.getResult());
		} catch (InterruptedException | ExecutionException e) {
			throw new DMLRuntimeException("ParamservBuiltinCPInstruction: unknown error: ", e);
		} finally {
			es.shutdownNow();
		}
	}

	@SuppressWarnings("resource")
	private void runOnSpark(SparkExecutionContext sec, PSModeType mode) {
		Timing tSetup = DMLScript.STATISTICS ? new Timing(true) : null;

		int workerNum = getWorkerNum(mode);
		String updFunc = getParam(PS_UPDATE_FUN);
		String aggFunc = getParam(PS_AGGREGATION_FUN);

		// Get the compiled execution context
		LocalVariableMap newVarsMap = createVarsMap(sec);
		// Level of par is 1 in spark backend because one worker will be launched per task
		ExecutionContext newEC = ParamservUtils.createExecutionContext(sec, newVarsMap, updFunc, aggFunc, 1);

		// Create the agg service's execution context
		ExecutionContext aggServiceEC = ParamservUtils.copyExecutionContext(newEC, 1).get(0);

		// Create the parameter server
		ListObject model = sec.getListObject(getParam(PS_MODEL));
		ParamServer ps = createPS(mode, aggFunc, getUpdateType(), workerNum, model, aggServiceEC);

		// Get driver host
		String host = sec.getSparkContext().getConf().get("spark.driver.host");

		// Create the netty server for ps
		TransportServer server = PSRpcFactory.createServer(sec.getSparkContext().getConf(),(LocalParamServer) ps, host); // Start the server

		// Force all the instructions to CP type
		Recompiler.recompileProgramBlockHierarchy2Forced(
			newEC.getProgram().getProgramBlocks(), 0, new HashSet<>(), LopProperties.ExecType.CP);

		// Serialize all the needed params for remote workers
		SparkPSBody body = new SparkPSBody(newEC);
		HashMap<String, byte[]> clsMap = new HashMap<>();
		String program = ProgramConverter.serializeSparkPSBody(body, clsMap);

		// Add the accumulators for statistics
		LongAccumulator aSetup = sec.getSparkContext().sc().longAccumulator("setup");
		LongAccumulator aWorker = sec.getSparkContext().sc().longAccumulator("workersNum");
		LongAccumulator aUpdate = sec.getSparkContext().sc().longAccumulator("modelUpdate");
		LongAccumulator aIndex = sec.getSparkContext().sc().longAccumulator("batchIndex");
		LongAccumulator aGrad = sec.getSparkContext().sc().longAccumulator("gradCompute");
		LongAccumulator aRPC = sec.getSparkContext().sc().longAccumulator("rpcRequest");
		LongAccumulator aBatch = sec.getSparkContext().sc().longAccumulator("numBatches");
		LongAccumulator aEpoch = sec.getSparkContext().sc().longAccumulator("numEpochs");
		
		// Create remote workers
		SparkPSWorker worker = new SparkPSWorker(getParam(PS_UPDATE_FUN), getParam(PS_AGGREGATION_FUN),
			getFrequency(), getEpochs(), getBatchSize(), program, clsMap, sec.getSparkContext().getConf(),
			server.getPort(), aSetup, aWorker, aUpdate, aIndex, aGrad, aRPC, aBatch, aEpoch);

		if (DMLScript.STATISTICS)
			Statistics.accPSSetupTime((long) tSetup.stop());

		MatrixObject features = sec.getMatrixObject(getParam(PS_FEATURES));
		MatrixObject labels = sec.getMatrixObject(getParam(PS_LABELS));
		try {
			ParamservUtils.doPartitionOnSpark(sec, features, labels, getScheme(), workerNum) // Do data partitioning
				.foreach(worker); // Run remote workers
		} catch (Exception e) {
			throw new DMLRuntimeException("Paramserv function failed: ", e);
		} finally {
			server.close(); // Stop the netty server
		}

		// Accumulate the statistics for remote workers
		if (DMLScript.STATISTICS) {
			Statistics.accPSSetupTime(aSetup.value());
			Statistics.incWorkerNumber(aWorker.value());
			Statistics.accPSLocalModelUpdateTime(aUpdate.value());
			Statistics.accPSBatchIndexingTime(aIndex.value());
			Statistics.accPSGradientComputeTime(aGrad.value());
			Statistics.accPSRpcRequestTime(aRPC.value());
		}

		// Fetch the final model from ps
		sec.setVariable(output.getName(), ps.getResult());
	}

	private void runLocally(ExecutionContext ec, PSModeType mode) {
		Timing tSetup = DMLScript.STATISTICS ? new Timing(true) : null;
		int workerNum = getWorkerNum(mode);
		BasicThreadFactory factory = new BasicThreadFactory.Builder()
			.namingPattern("workers-pool-thread-%d").build();
		ExecutorService es = Executors.newFixedThreadPool(workerNum, factory);
		String updFunc = getParam(PS_UPDATE_FUN);
		String aggFunc = getParam(PS_AGGREGATION_FUN);

		// Get the compiled execution context
		LocalVariableMap newVarsMap = createVarsMap(ec);
		ExecutionContext newEC = ParamservUtils.createExecutionContext(ec, newVarsMap, updFunc, aggFunc, getParLevel(workerNum));

		// Create workers' execution context
		List<ExecutionContext> workerECs = ParamservUtils.copyExecutionContext(newEC, workerNum);

		// Create the agg service's execution context
		ExecutionContext aggServiceEC = ParamservUtils.copyExecutionContext(newEC, 1).get(0);

		PSFrequency freq = getFrequency();
		PSUpdateType updateType = getUpdateType();

		// Create the parameter server
		ListObject model = ec.getListObject(getParam(PS_MODEL));
		ParamServer ps = createPS(mode, aggFunc, updateType, workerNum, model, aggServiceEC);

		// Create the local workers
		List<LocalPSWorker> workers = IntStream.range(0, workerNum)
			.mapToObj(i -> new LocalPSWorker(i, updFunc, freq, getEpochs(), getBatchSize(), workerECs.get(i), ps))
			.collect(Collectors.toList());

		// Do data partition
		PSScheme scheme = getScheme();
		partitionLocally(scheme, ec, workers);

		if (DMLScript.STATISTICS)
			Statistics.accPSSetupTime((long) tSetup.stop());

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("\nConfiguration of paramserv func: "
				+ "\nmode: %s \nworkerNum: %d \nupdate frequency: %s "
				+ "\nstrategy: %s \ndata partitioner: %s",
				mode, workerNum, freq, updateType, scheme));
		}

		try {
			// Launch the worker threads and wait for completion
			for (Future<Void> ret : es.invokeAll(workers))
				ret.get(); //error handling
			// Fetch the final model from ps
			ec.setVariable(output.getName(), ps.getResult());
		} catch (InterruptedException | ExecutionException e) {
			throw new DMLRuntimeException("ParamservBuiltinCPInstruction: some error occurred: ", e);
		} finally {
			es.shutdownNow();
		}
	}

	private LocalVariableMap createVarsMap(ExecutionContext ec) {
		// Put the hyperparam into the variables table
		LocalVariableMap varsMap = new LocalVariableMap();
		ListObject hyperParams = getHyperParams(ec);
		if (hyperParams != null) {
			varsMap.put(PS_HYPER_PARAMS, hyperParams);
		}
		return varsMap;
	}

	private PSModeType getPSMode() {
		if (!getParameterMap().containsKey(PS_MODE)) {
			return DEFAULT_MODE;
		}
		PSModeType mode;
		try {
			mode = PSModeType.valueOf(getParam(PS_MODE));
		} catch (IllegalArgumentException e) {
			throw new DMLRuntimeException(String.format("Paramserv function: not support ps execution mode '%s'", getParam(PS_MODE)));
		}
		return mode;
	}

	private int getEpochs() {
		int epochs = Integer.valueOf(getParam(PS_EPOCHS));
		if (epochs <= 0) {
			throw new DMLRuntimeException(String.format("Paramserv function: "
				+ "The argument '%s' could not be less than or equal to 0.", PS_EPOCHS));
		}
		return epochs;
	}

	private static int getParLevel(int workerNum) {
		return Math.max((int)Math.ceil((double)getRemainingCores()/workerNum), 1);
	}

	private PSUpdateType getUpdateType() {
		if (!getParameterMap().containsKey(PS_UPDATE_TYPE)) {
			return DEFAULT_TYPE;
		}
		PSUpdateType updType;
		try {
			updType = PSUpdateType.valueOf(getParam(PS_UPDATE_TYPE));
		} catch (IllegalArgumentException e) {
			throw new DMLRuntimeException(String.format("Paramserv function: not support update type '%s'.", getParam(PS_UPDATE_TYPE)));
		}
		if (updType == PSUpdateType.SSP)
			throw new DMLRuntimeException("Paramserv function: Not support update type SSP.");
		return updType;
	}

	private PSFrequency getFrequency() {
		if (!getParameterMap().containsKey(PS_FREQUENCY)) {
			return DEFAULT_UPDATE_FREQUENCY;
		}
		try {
			return PSFrequency.valueOf(getParam(PS_FREQUENCY));
		} catch (IllegalArgumentException e) {
			throw new DMLRuntimeException(String.format("Paramserv function: "
				+ "not support '%s' update frequency.", getParam(PS_FREQUENCY)));
		}
	}

	private static int getRemainingCores() {
		return InfrastructureAnalyzer.getLocalParallelism();
	}

	/**
	 * Get the worker numbers according to the vcores
	 *
	 * @param mode execution mode
	 * @return worker numbers
	 */
	private int getWorkerNum(PSModeType mode) {
		switch (mode) {
			case LOCAL:
				return getParameterMap().containsKey(PS_PARALLELISM) ?
					Integer.valueOf(getParam(PS_PARALLELISM)) : getRemainingCores();
			case REMOTE_SPARK:
				return getParameterMap().containsKey(PS_PARALLELISM) ?
					Integer.valueOf(getParam(PS_PARALLELISM)) : SparkExecutionContext.getDefaultParallelism(true);
			default:
				throw new DMLRuntimeException("Unsupported parameter server: " + mode.name());
		}
	}

	/**
	 * Create a server which serves the local or remote workers
	 *
	 * @return parameter server
	 */
	private static ParamServer createPS(PSModeType mode, String aggFunc, PSUpdateType updateType, int workerNum, ListObject model, ExecutionContext ec) {
		switch (mode) {
			case FEDERATED:
			case LOCAL:
			case REMOTE_SPARK:
				return LocalParamServer.create(model, aggFunc, updateType, ec, workerNum);
			default:
				throw new DMLRuntimeException("Unsupported parameter server: "+mode.name());
		}
	}

	private long getBatchSize() {
		if (!getParameterMap().containsKey(PS_BATCH_SIZE)) {
			return DEFAULT_BATCH_SIZE;
		}
		long batchSize = Integer.valueOf(getParam(PS_BATCH_SIZE));
		if (batchSize <= 0) {
			throw new DMLRuntimeException(String.format("Paramserv function: the number "
				+ "of argument '%s' could not be less than or equal to 0.", PS_BATCH_SIZE));
		}
		return batchSize;
	}

	private ListObject getHyperParams(ExecutionContext ec) {
		ListObject hyperparams = null;
		if (getParameterMap().containsKey(PS_HYPER_PARAMS)) {
			hyperparams = ec.getListObject(getParam(PS_HYPER_PARAMS));
		}
		return hyperparams;
	}

	private void partitionLocally(PSScheme scheme, ExecutionContext ec, List<LocalPSWorker> workers) {
		MatrixObject features = ec.getMatrixObject(getParam(PS_FEATURES));
		MatrixObject labels = ec.getMatrixObject(getParam(PS_LABELS));
		DataPartitionLocalScheme.Result result = new LocalDataPartitioner(scheme).doPartitioning(workers.size(), features.acquireReadAndRelease(), labels.acquireReadAndRelease());
		List<MatrixObject> pfs = result.pFeatures;
		List<MatrixObject> pls = result.pLabels;
		if (pfs.size() < workers.size()) {
			if (LOG.isWarnEnabled()) {
				LOG.warn(String.format("There is only %d batches of data but has %d workers. "
					+ "Hence, reset the number of workers with %d.", pfs.size(), workers.size(), pfs.size()));
			}
			workers = workers.subList(0, pfs.size());
		}
		for (int i = 0; i < workers.size(); i++) {
			workers.get(i).setFeatures(pfs.get(i));
			workers.get(i).setLabels(pls.get(i));
		}
	}

	private PSScheme getScheme() {
		PSScheme scheme = DEFAULT_SCHEME;
		if (getParameterMap().containsKey(PS_SCHEME)) {
			try {
				scheme = PSScheme.valueOf(getParam(PS_SCHEME));
			} catch (IllegalArgumentException e) {
				throw new DMLRuntimeException(String.format("Paramserv function: not support data partition scheme '%s'", getParam(PS_SCHEME)));
			}
		}
		return scheme;
	}

	private FederatedPSScheme getFederatedScheme() {
		FederatedPSScheme federated_scheme = DEFAULT_FEDERATED_SCHEME;
		if (getParameterMap().containsKey(PS_SCHEME)) {
			try {
				federated_scheme = FederatedPSScheme.valueOf(getParam(PS_SCHEME));
			} catch (IllegalArgumentException e) {
				throw new DMLRuntimeException(String.format("Paramserv function in federated mode: not support data partition scheme '%s'", getParam(PS_SCHEME)));
			}
		}
		return federated_scheme;
	}
}
