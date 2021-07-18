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
import org.apache.sysds.common.Types.ExecType;
import org.apache.sysds.hops.recompile.Recompiler;
import org.apache.sysds.parser.Statement.PSFrequency;
import org.apache.sysds.parser.Statement.PSModeType;
import org.apache.sysds.parser.Statement.PSScheme;
import org.apache.sysds.parser.Statement.FederatedPSScheme;
import org.apache.sysds.parser.Statement.PSUpdateType;
import org.apache.sysds.parser.Statement.PSRuntimeBalancing;
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

import static java.lang.Boolean.parseBoolean;
import static org.apache.sysds.parser.Statement.*;

public class ParamservBuiltinCPInstruction extends ParameterizedBuiltinCPInstruction {
	private static final Log LOG = LogFactory.getLog(ParamservBuiltinCPInstruction.class.getName());
	
	public static final int DEFAULT_BATCH_SIZE = 64;
	private static final PSFrequency DEFAULT_UPDATE_FREQUENCY = PSFrequency.EPOCH;
	private static final PSScheme DEFAULT_SCHEME = PSScheme.DISJOINT_CONTIGUOUS;
	private static final PSRuntimeBalancing DEFAULT_RUNTIME_BALANCING = PSRuntimeBalancing.NONE;
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
		if(DMLScript.STATISTICS)
			Statistics.getPSExecutionTimer().start();

		Timing tSetup = DMLScript.STATISTICS ? new Timing(true) : null;
		LOG.info("PARAMETER SERVER");
		LOG.info("[+] Running in federated mode");
		// get inputs
		String updFunc = getParam(PS_UPDATE_FUN);
		String modelAvg = getParam(PS_MODELAVG);
		String aggFunc = getParam(PS_AGGREGATION_FUN);
		PSUpdateType updateType = getUpdateType();
		PSFrequency freq = getFrequency();
		FederatedPSScheme federatedPSScheme = getFederatedScheme();
		PSRuntimeBalancing runtimeBalancing = getRuntimeBalancing();
		boolean weighting = getWeighting();
		int seed = getSeed();

		if( LOG.isInfoEnabled() ) {
			LOG.info("[+] Update Type: " + updateType);
			LOG.info("[+] Frequency: " + freq);
			LOG.info("[+] Data Partitioning: " + federatedPSScheme);
			LOG.info("[+] Runtime Balancing: " + runtimeBalancing);
			LOG.info("[+] Weighting: " + weighting);
			LOG.info("[+] Seed: " + seed);
			LOG.info("[+] modelAvg: " + modelAvg);
		}
		if (tSetup != null)
			Statistics.accPSSetupTime((long) tSetup.stop());

		// partition federated data
		Timing tDataPartitioning = DMLScript.STATISTICS ? new Timing(true) : null;
		DataPartitionFederatedScheme.Result result = new FederatedDataPartitioner(federatedPSScheme, seed)
			.doPartitioning(ec.getMatrixObject(getParam(PS_FEATURES)), ec.getMatrixObject(getParam(PS_LABELS)));
		int workerNum = result._workerNum;
		if (DMLScript.STATISTICS)
			Statistics.accFedPSDataPartitioningTime((long) tDataPartitioning.stop());


		if (DMLScript.STATISTICS)
			tSetup.start();
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
		MatrixObject val_features = (getParam(PS_VAL_FEATURES) != null) ? ec.getMatrixObject(getParam(PS_VAL_FEATURES)) : null;
		MatrixObject val_labels = (getParam(PS_VAL_LABELS) != null) ? ec.getMatrixObject(getParam(PS_VAL_LABELS)) : null;

		ParamServer ps = createPS(PSModeType.FEDERATED, aggFunc, updateType, freq, workerNum, model, aggServiceEC, getValFunction(),
				getNumBatchesPerEpoch(runtimeBalancing, result._balanceMetrics), val_features, val_labels,parseBoolean(modelAvg));

		// Create the local workers
		int finalNumBatchesPerEpoch = getNumBatchesPerEpoch(runtimeBalancing, result._balanceMetrics);
		List<FederatedPSControlThread> threads = IntStream.range(0, workerNum)
			.mapToObj(i -> new FederatedPSControlThread(i, updFunc, freq, runtimeBalancing, weighting,
				getEpochs(), getBatchSize(), finalNumBatchesPerEpoch, federatedWorkerECs.get(i), ps))
			.collect(Collectors.toList());
		if(workerNum != threads.size()) {
			throw new DMLRuntimeException("ParamservBuiltinCPInstruction: Federated data partitioning does not match threads!");
		}
		// Set features and lables for the control threads and write the program and instructions and hyperparams to the federated workers
		for (int i = 0; i < threads.size(); i++) {
			threads.get(i).setFeatures(result._pFeatures.get(i));
			threads.get(i).setLabels(result._pLabels.get(i));
			threads.get(i).setup(result._weightingFactors.get(i));
		}
		if (DMLScript.STATISTICS)
			Statistics.accPSSetupTime((long) tSetup.stop());

		try {
			// Launch the worker threads and wait for completion
			for (Future<Void> ret : es.invokeAll(threads))
				ret.get(); //error handling
			// Fetch the final model from ps
			ec.setVariable(output.getName(), ps.getResult());
			if (DMLScript.STATISTICS)
				Statistics.accPSExecutionTime((long) Statistics.getPSExecutionTimer().stop());
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
		String modelAvg = getParam(PS_MODELAVG);


		// Get the compiled execution context
		LocalVariableMap newVarsMap = createVarsMap(sec);
		// Level of par is 1 in spark backend because one worker will be launched per task
		ExecutionContext newEC = ParamservUtils.createExecutionContext(sec, newVarsMap, updFunc, aggFunc, 1);

		// Create the agg service's execution context
		ExecutionContext aggServiceEC = ParamservUtils.copyExecutionContext(newEC, 1).get(0);

		// Create the parameter server
		ListObject model = sec.getListObject(getParam(PS_MODEL));
		ParamServer ps = createPS(mode, aggFunc, getUpdateType(), getFrequency(), workerNum, model, aggServiceEC, parseBoolean(getParam(modelAvg)));

		// Get driver host
		String host = sec.getSparkContext().getConf().get("spark.driver.host");

		// Create the netty server for ps
		TransportServer server = PSRpcFactory.createServer(sec.getSparkContext().getConf(),(LocalParamServer) ps, host); // Start the server

		// Force all the instructions to CP type
		Recompiler.recompileProgramBlockHierarchy2Forced(
			newEC.getProgram().getProgramBlocks(), 0, new HashSet<>(), ExecType.CP);

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
		if(DMLScript.STATISTICS)
			Statistics.getPSExecutionTimer().start();
		String modelAvg = getParam(PS_MODELAVG);
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

		double rows_per_worker = Math.ceil((float) ec.getMatrixObject(getParam(PS_FEATURES)).getNumRows() / workerNum);
		int num_batches_per_epoch = (int) Math.ceil(rows_per_worker / getBatchSize());

		// Create the parameter server
		ListObject model = ec.getListObject(getParam(PS_MODEL));
		MatrixObject val_features = (getParam(PS_VAL_FEATURES) != null) ? ec.getMatrixObject(getParam(PS_VAL_FEATURES)) : null;
		MatrixObject val_labels = (getParam(PS_VAL_LABELS) != null) ? ec.getMatrixObject(getParam(PS_VAL_LABELS)) : null;
		ParamServer ps = createPS(mode, aggFunc, updateType, freq, workerNum, model, aggServiceEC, getValFunction(),
				num_batches_per_epoch, val_features, val_labels,parseBoolean(modelAvg));

		// Create the local workers
		List<LocalPSWorker> workers = IntStream.range(0, workerNum)
			.mapToObj(i -> new LocalPSWorker(i, updFunc, freq,
				getEpochs(), getBatchSize(), workerECs.get(i), ps,parseBoolean(modelAvg)))
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
			if (DMLScript.STATISTICS)
				Statistics.accPSExecutionTime((long) Statistics.getPSExecutionTimer().stop());
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

	private PSRuntimeBalancing getRuntimeBalancing() {
		if (!getParameterMap().containsKey(PS_FED_RUNTIME_BALANCING)) {
			return DEFAULT_RUNTIME_BALANCING;
		}
		try {
			return PSRuntimeBalancing.valueOf(getParam(PS_FED_RUNTIME_BALANCING));
		} catch (IllegalArgumentException e) {
			throw new DMLRuntimeException(String.format("Paramserv function: "
				+ "not support '%s' runtime balancing.", getParam(PS_FED_RUNTIME_BALANCING)));
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
	private static ParamServer createPS(PSModeType mode, String aggFunc, PSUpdateType updateType,
		PSFrequency freq, int workerNum, ListObject model, ExecutionContext ec,boolean modelAvg)
	{
		return createPS(mode, aggFunc, updateType, freq, workerNum, model, ec, null, -1, null, null,modelAvg );
	}

	// When this creation is used the parameter server is able to validate after each epoch
	private static ParamServer createPS(PSModeType mode, String aggFunc, PSUpdateType updateType,
		PSFrequency freq, int workerNum, ListObject model, ExecutionContext ec, String valFunc,
		int numBatchesPerEpoch, MatrixObject valFeatures, MatrixObject valLabels,boolean modelAvg)
	{
			switch (mode) {
			case FEDERATED:
			case LOCAL:
			case REMOTE_SPARK:
				return LocalParamServer.create(model, aggFunc, updateType, freq, ec, workerNum, valFunc, numBatchesPerEpoch, valFeatures, valLabels,modelAvg);
			default:
				throw new DMLRuntimeException("Unsupported parameter server: " + mode.name());
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
		DataPartitionLocalScheme.Result result = new LocalDataPartitioner(scheme)
			.doPartitioning(workers.size(), features.acquireReadAndRelease(), labels.acquireReadAndRelease());
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

	/**
	 * Calculates the number of batches per epoch depending on the balance metrics and the runtime balancing
	 *
	 * @param runtimeBalancing the runtime balancing
	 * @param balanceMetrics the balance metrics calculated during data partitioning
	 * @return numBatchesPerEpoch
	 */
	private int getNumBatchesPerEpoch(PSRuntimeBalancing runtimeBalancing, DataPartitionFederatedScheme.BalanceMetrics balanceMetrics) {
		int numBatchesPerEpoch;
		if(runtimeBalancing == PSRuntimeBalancing.CYCLE_MIN || runtimeBalancing == PSRuntimeBalancing.BASELINE) {
			numBatchesPerEpoch = (int) Math.ceil(balanceMetrics._minRows / (float) getBatchSize());
		} else if (runtimeBalancing == PSRuntimeBalancing.CYCLE_AVG
				|| runtimeBalancing == PSRuntimeBalancing.SCALE_BATCH) {
			numBatchesPerEpoch = (int) Math.ceil(balanceMetrics._avgRows / (float) getBatchSize());
		} else if (runtimeBalancing == PSRuntimeBalancing.CYCLE_MAX) {
			numBatchesPerEpoch = (int) Math.ceil(balanceMetrics._maxRows / (float) getBatchSize());
		} else {
			numBatchesPerEpoch = (int) Math.ceil(balanceMetrics._avgRows / (float) getBatchSize());
		}
		return numBatchesPerEpoch;
	}

	private boolean getWeighting() {
		return getParameterMap().containsKey(PS_FED_WEIGHTING) && parseBoolean(getParam(PS_FED_WEIGHTING));
	}

	private String getValFunction() {
		if (getParameterMap().containsKey(PS_VAL_FUN)) {
			return getParam(PS_VAL_FUN);
		}
		return null;
	}

	private int getSeed() {
		return (getParameterMap().containsKey(PS_SEED)) ? Integer.parseInt(getParam(PS_SEED)) : (int) System.currentTimeMillis();
	}
	private boolean getModelAvg() {
		return getParameterMap().containsKey(PS_MODELAVG) && parseBoolean(getParam(PS_MODELAVG));
	}

}
