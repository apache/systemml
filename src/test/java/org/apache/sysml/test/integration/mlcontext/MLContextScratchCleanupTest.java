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

package org.apache.sysml.test.integration.mlcontext;

import static org.apache.sysml.api.mlcontext.ScriptFactory.dmlFromFile;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.api.DMLScript.RUNTIME_PLATFORM;
import org.apache.sysml.api.mlcontext.MLContext;
import org.apache.sysml.api.linalg.Matrix;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.test.integration.AutomatedTestBase;
import org.apache.sysml.test.utils.TestUtils;
import org.junit.After;
import org.junit.Test;


public class MLContextScratchCleanupTest extends AutomatedTestBase 
{
	private final static String TEST_DIR = "org/apache/sysml/api/mlcontext";
	private final static String TEST_NAME = "MLContextScratchCleanup";

	private final static int rows = 1123;
	private final static int cols = 789;
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_DIR, TEST_NAME);
		getAndLoadTestConfiguration(TEST_NAME);
	}

	@Test
	public void testMLContextMultipleScriptsCP() {
		runMLContextTestMultipleScript(RUNTIME_PLATFORM.SINGLE_NODE, false);
	}
	
	@Test
	public void testMLContextMultipleScriptsHybrid() {
		runMLContextTestMultipleScript(RUNTIME_PLATFORM.HYBRID_SPARK, false);
	}
	
	@Test
	public void testMLContextMultipleScriptsSpark() {
		runMLContextTestMultipleScript(RUNTIME_PLATFORM.SPARK, false);
	}
	
	@Test
	public void testMLContextMultipleScriptsWithReadCP() {
		runMLContextTestMultipleScript(RUNTIME_PLATFORM.SINGLE_NODE, true);
	}
	
	@Test
	public void testMLContextMultipleScriptsWithReadHybrid() {
		runMLContextTestMultipleScript(RUNTIME_PLATFORM.HYBRID_SPARK, true);
	}
	
	@Test
	public void testMLContextMultipleScriptsWithReadSpark() {
		runMLContextTestMultipleScript(RUNTIME_PLATFORM.SPARK, true);
	}

	/**
	 * 
	 * @param platform
	 */
	private void runMLContextTestMultipleScript(RUNTIME_PLATFORM platform, boolean wRead) 
	{
		RUNTIME_PLATFORM oldplatform = DMLScript.rtplatform;
		DMLScript.rtplatform = platform;
		
		//create mlcontext
		SparkConf conf = SparkExecutionContext.createSystemMLSparkConf()
				.setAppName("MLContextFrameTest").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		MLContext ml = new MLContext(sc);
		ml.setExplain(true);
		
		String dml1 = baseDirectory + File.separator + "ScratchCleanup1.dml";
		String dml2 = baseDirectory + File.separator + (wRead?"ScratchCleanup2b.dml":"ScratchCleanup2.dml");
		
		try
		{
			Script script1 = dmlFromFile(dml1).in("$rows", rows).in("$cols", cols).out("X");
			Matrix X = ml.execute(script1).getMatrix("X");
			
			//clear in-memory/cached data to emulate on-disk storage
			X.toMatrixObject().clearData();
			
			Script script2 = dmlFromFile(dml2).in("X", X).out("z");
			String z = ml.execute(script2).getString("z");
			
			System.out.println(z);
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		finally {
			DMLScript.rtplatform = oldplatform;
			
			// stop spark context to allow single jvm tests (otherwise the
			// next test that tries to create a SparkContext would fail)
			sc.stop();
			// clear status mlcontext and spark exec context
			ml.close();
		}
	}

	@After
	public void tearDown() {
		super.tearDown();
	}
}
