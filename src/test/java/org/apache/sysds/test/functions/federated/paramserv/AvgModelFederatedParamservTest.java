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

package org.apache.sysds.test.functions.federated.paramserv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.hops.codegen.SpoofFusedOp;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.apache.sysds.utils.Statistics;
import org.dmg.pmml.True;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class AvgModelFederatedParamservTest extends AutomatedTestBase {
	private static final Log LOG = LogFactory.getLog(AvgModelFederatedParamservTest.class.getName());
	private final static String TEST_DIR = "functions/federated/paramserv/";
	private final static String TEST_NAME = "FederatedParamservTest";
	private final static String TEST_CLASS_DIR = TEST_DIR + AvgModelFederatedParamservTest.class.getSimpleName() + "/";

	private final String _networkType;
	private final int _numFederatedWorkers;
	private final int _dataSetSize;
	private final int _epochs;
	private final int _batch_size;
	private final double _eta;
	private final String _utype;
	private final String _freq;
	private final String _scheme;
	private final String _runtime_balancing;
	private final String _weighting;
	private final String _data_distribution;
	private final int _seed;

	// parameters
	@Parameterized.Parameters
	public static Collection<Object[]> parameters() {
		return Arrays.asList(new Object[][] {
			// Network type, number of federated workers, data set size, batch size, epochs, learning rate, update type, update frequency
			// basic functionality
			//{"TwoNN",	4, 60000, 32, 4, 0.01, 	"BSP", "BATCH", "KEEP_DATA_ON_WORKER", 	"NONE" ,		"false","BALANCED",		200},

			{"TwoNN",	2, 4, 1, 4, 0.01, 		"BSP", "BATCH", "SHUFFLE", 	"BASELINE",		"true",	"BALANCED",	200, true},
			{"CNN", 	2, 4, 1, 4, 0.01, 		"BSP", "EPOCH", "SHUFFLE", 				"CYCLE_MIN", 		"true",	"IMBALANCED", 	200,true},
			{"CNN",		2, 4, 1, 4, 0.01, 		"BSP", "BATCH", "REPLICATE_TO_MAX", 	"CYCLE_MIN", 	"true",	"IMBALANCED",	200,true},
			{"TwoNN", 	2, 4, 1, 4, 0.01, 		"BSP", "EPOCH", "BALANCE_TO_AVG", 		"CYCLE_MAX", 	"true",	"IMBALANCED",	200,true},
		    {"TwoNN", 	5, 10, 1, 2, 0.01, 	"BSP", "BATCH", "KEEP_DATA_ON_WORKER", 	"NONE", 		"true",	"BALANCED",		200,true},

			/*
				// runtime balancing
				{"TwoNN", 	2, 4, 1, 4, 0.01, 		"BSP", "BATCH", "KEEP_DATA_ON_WORKER", 	"CYCLE_MIN", 	"true",	"IMBALANCED",	200},
				{"TwoNN", 	2, 4, 1, 4, 0.01, 		"BSP", "EPOCH", "KEEP_DATA_ON_WORKER", 	"CYCLE_MIN", 	"true",	"IMBALANCED",	200},
				{"TwoNN", 	2, 4, 1, 4, 0.01, 		"BSP", "BATCH", "KEEP_DATA_ON_WORKER", 	"CYCLE_AVG", 	"true",	"IMBALANCED",	200},
				{"TwoNN", 	2, 4, 1, 4, 0.01, 		"BSP", "EPOCH", "KEEP_DATA_ON_WORKER", 	"CYCLE_AVG", 	"true",	"IMBALANCED",	200},
				{"TwoNN", 	2, 4, 1, 4, 0.01, 		"BSP", "BATCH", "KEEP_DATA_ON_WORKER", 	"CYCLE_MAX",	"true", "IMBALANCED",	200},
				{"TwoNN", 	2, 4, 1, 4, 0.01, 		"BSP", "EPOCH", "KEEP_DATA_ON_WORKER", 	"CYCLE_MAX",	"true", "IMBALANCED",	200},

				// data partitioning
				{"TwoNN", 	2, 4, 1, 1, 0.01, 		"BSP", "BATCH", "SHUFFLE", 				"CYCLE_AVG", 	"true",	"IMBALANCED",	200},
				{"TwoNN", 	2, 4, 1, 1, 0.01, 		"BSP", "BATCH", "REPLICATE_TO_MAX",	 	"NONE", 		"true",	"IMBALANCED",	200},
				{"TwoNN", 	2, 4, 1, 1, 0.01, 		"BSP", "BATCH", "SUBSAMPLE_TO_MIN",		"NONE", 		"true",	"IMBALANCED",	200},
				{"TwoNN", 	2, 4, 1, 1, 0.01, 		"BSP", "BATCH", "BALANCE_TO_AVG",		"NONE", 		"true",	"IMBALANCED",	200},

				// balanced tests
				{"CNN", 	5, 1000, 100, 2, 0.01, 	"BSP", "EPOCH", "KEEP_DATA_ON_WORKER", 	"NONE", 		"true",	"BALANCED",		200}
			*/
		});
	}

	public AvgModelFederatedParamservTest(String networkType, int numFederatedWorkers, int dataSetSize, int batch_size,
                                          int epochs, double eta, String utype, String freq, String scheme, String runtime_balancing, String weighting, String data_distribution, int seed,boolean modelAvg) {

		_networkType = networkType;
		_numFederatedWorkers = numFederatedWorkers;
		_dataSetSize = dataSetSize;
		_batch_size = batch_size;
		_epochs = epochs;
		_eta = eta;
		_utype = utype;
		_freq = freq;
		_scheme = scheme;
		_runtime_balancing = runtime_balancing;
		_weighting = weighting;
		_data_distribution = data_distribution;
		_seed = seed;
	}

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void federatedParamservSingleNode() {
		federatedParamserv(ExecMode.SINGLE_NODE, true);
	}

	@Test
	public void federatedParamservHybrid() {

		federatedParamserv(ExecMode.HYBRID, true);
	}

	private void federatedParamserv(ExecMode mode,boolean modelAvg) {
		// Warning Statistics accumulate in unit test
		// config
		getAndLoadTestConfiguration(TEST_NAME);
		String HOME = SCRIPT_DIR + TEST_DIR;
		setOutputBuffering(false);

		int C = 1, Hin = 28, Win = 28;
		int numLabels = 10;

		ExecMode platformOld = setExecMode(mode);

		try {
			// start threads
			List<Integer> ports = new ArrayList<>();
			List<Thread> threads = new ArrayList<>();
			for(int i = 0; i < _numFederatedWorkers; i++) {
				ports.add(getRandomAvailablePort());
				threads.add(startLocalFedWorkerThread(ports.get(i), FED_WORKER_WAIT));
			}

			// generate test data
			double[][] features = generateDummyMNISTFeatures(_dataSetSize, C, Hin, Win);
			double[][] labels = generateDummyMNISTLabels(_dataSetSize, numLabels);
			String featuresName = "";
			String labelsName = "";

			// federate test data balanced or imbalanced
			if(_data_distribution.equals("IMBALANCED")) {
				featuresName = "X_IMBALANCED_" + _numFederatedWorkers;
				labelsName = "y_IMBALANCED_" + _numFederatedWorkers;
				double[][] ranges = {{0,1}, {1,4}};
				rowFederateLocallyAndWriteInputMatrixWithMTD(featuresName, features, _numFederatedWorkers, ports, ranges);
				rowFederateLocallyAndWriteInputMatrixWithMTD(labelsName, labels, _numFederatedWorkers, ports, ranges);
			}
			else {
				featuresName = "X_BALANCED_" + _numFederatedWorkers;
				labelsName = "y_BALANCED_" + _numFederatedWorkers;
				double[][] ranges = generateBalancedFederatedRowRanges(_numFederatedWorkers, features.length);
				rowFederateLocallyAndWriteInputMatrixWithMTD(featuresName, features, _numFederatedWorkers, ports, ranges);
				rowFederateLocallyAndWriteInputMatrixWithMTD(labelsName, labels, _numFederatedWorkers, ports, ranges);
			}

			try {
				//wait for all workers to be setup
				Thread.sleep(FED_WORKER_WAIT);
			}
			catch(InterruptedException e) {
				e.printStackTrace();
			}

			// dml name
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			// generate program args
			List<String> programArgsList = new ArrayList<>(Arrays.asList("-stats",
					"-nvargs",
					"features=" + input(featuresName),
					"labels=" + input(labelsName),
					"epochs=" + _epochs,
					"batch_size=" + _batch_size,
					"eta=" + _eta,
					"utype=" + _utype,
					"freq=" + _freq,
					"scheme=" + _scheme,
					"runtime_balancing=" + _runtime_balancing,
					"weighting=" + _weighting,
					"network_type=" + _networkType,
					"channels=" + C,
					"hin=" + Hin,
					"win=" + Win,
					"seed=" + _seed,
			        "modelAvg="+ modelAvg));

			programArgs = programArgsList.toArray(new String[0]);

			runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);

		//	Assert.assertEquals(0, Statistics.getNoOfExecutedSPInst());

			// shut down threads
			for(int i = 0; i < _numFederatedWorkers; i++) {
				TestUtils.shutdownThreads(threads.get(i));
			}
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	/**
	 * Generates an feature matrix that has the same format as the MNIST dataset,
	 * but is completely random and normalized
	 *
	 *  @param numExamples Number of examples to generate
	 *  @param C Channels in the input data
	 *  @param Hin Height in Pixels of the input data
	 *  @param Win Width in Pixels of the input data
	 *  @return a dummy MNIST feature matrix
	 */
	private double[][] generateDummyMNISTFeatures(int numExamples, int C, int Hin, int Win) {
		// Seed -1 takes the time in milliseconds as a seed
		// Sparsity 1 means no sparsity
		return getRandomMatrix(numExamples, C*Hin*Win, 0, 1, 1, -1);
	}

	/**
	 * Generates an label matrix that has the same format as the MNIST dataset, but is completely random and consists
	 * of one hot encoded vectors as rows
	 *
	 *  @param numExamples Number of examples to generate
	 *  @param numLabels Number of labels to generate
	 *  @return a dummy MNIST lable matrix
	 */
	private double[][] generateDummyMNISTLabels(int numExamples, int numLabels) {
		// Seed -1 takes the time in milliseconds as a seed
		// Sparsity 1 means no sparsity
		return getRandomMatrix(numExamples, numLabels, 0, 1, 1, -1);
	}
}
