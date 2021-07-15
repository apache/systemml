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
package org.apache.sysds.test.functions.dnn;

import java.util.HashMap;

import org.junit.Test;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.common.Types.ExecType;
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;

public class Conv2DTest extends AutomatedTestBase
{
	private final static String TEST_NAME = "Conv2DTest";
	private final static String TEST_DIR = "functions/tensor/";
	private final static String TEST_CLASS_DIR = TEST_DIR + Conv2DTest.class.getSimpleName() + "/";
	private final static double epsilon=0.0000000001;
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"B"}));
	}
	
	@Test
	public void testConv2DDense1() {
		int numImg = 5; int imgSize = 3; int numChannels = 3; int numFilters = 6; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	
	@Test
	public void testConv2DDense2() {
		int numImg = 1; int imgSize = 10; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense3() {
		int numImg = 1; int imgSize = 10; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense4() {
		int numImg = 3; int imgSize = 10; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense5() {
		int numImg = 3; int imgSize = 8; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 1; int pad = 2;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense6() {
		int numImg = 1; int imgSize = 10; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense7() {
		int numImg = 3; int imgSize = 64; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DSparse1a() {
		int numImg = 64; int imgSize = 16; int numChannels = 3; int numFilters = 6; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DSparse2a() {
		int numImg = 64; int imgSize = 16; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DSparse3a() {
		int numImg = 64; int imgSize = 16; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DSparse4a() {
		int numImg = 64; int imgSize = 16; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DSparse5a() {
		int numImg = 64; int imgSize = 16; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 1; int pad = 2;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DSparse6a() {
		int numImg = 64; int imgSize = 16; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DSparse7a() {
		int numImg = 64; int imgSize = 16; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DSparse1b() {
		int numImg = 64; int imgSize = 16; int numChannels = 3; int numFilters = 6; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	@Test
	public void testConv2DSparse2b() {
		int numImg = 64; int imgSize = 16; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	@Test
	public void testConv2DSparse3b() {
		int numImg = 64; int imgSize = 16; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	@Test
	public void testConv2DSparse4b() {
		int numImg = 64; int imgSize = 16; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	@Test
	public void testConv2DSparse5b() {
		int numImg = 64; int imgSize = 16; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 1; int pad = 2;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	@Test
	public void testConv2DSparse6b() {
		int numImg = 64; int imgSize = 16; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	@Test
	public void testConv2DSparse7b() {
		int numImg = 64; int imgSize = 16; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.CP, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	// --------------------------------------------
	

	@Test
	public void testConv2DDense1SP() 
	{
		int numImg = 5; int imgSize = 3; int numChannels = 3; int numFilters = 6; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense2SP() 
	{
		int numImg = 1; int imgSize = 10; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 0;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense3SP() 
	{
		int numImg = 1; int imgSize = 10; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense4SP() 
	{
		int numImg = 3; int imgSize = 10; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense5SP() 
	{
		int numImg = 3; int imgSize = 8; int numChannels = 2; int numFilters = 3; int filterSize = 3; int stride = 1; int pad = 2;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense6SP() 
	{
		int numImg = 1; int imgSize = 10; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DDense7SP() 
	{
		int numImg = 3; int imgSize = 10; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, false);
	}
	
	@Test
	public void testConv2DSparse1SP() 
	{
		int numImg = 5; int imgSize = 3; int numChannels = 3; int numFilters = 6; int filterSize = 2; int stride = 1; int pad = 0;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, false, true);
	}
	
	@Test
	public void testConv2DSparse2SP() 
	{
		int numImg = 1; int imgSize = 10; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 0;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	@Test
	public void testConv2DSparse3SP() 
	{
		int numImg = 1; int imgSize = 10; int numChannels = 4; int numFilters = 3; int filterSize = 4; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, false);
	}
	
	public void testConv2DSparse4SP() 
	{
		int numImg = 3; int imgSize = 10; int numChannels = 1; int numFilters = 3; int filterSize = 2; int stride = 2; int pad = 1;
		runConv2DTest(ExecType.SPARK, imgSize, numImg, numChannels, numFilters, filterSize, stride, pad, true, true);
	}
	
	public void runConv2DTest( ExecType et, int imgSize, int numImg, int numChannels, int numFilters, 
			int filterSize, int stride, int pad, boolean sparse1, boolean sparse2) 
	{
		ExecMode platformOld = rtplatform;
		switch( et ){
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecMode.SPARK )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		
		try
		{
			String sparseVal1 = String.valueOf(sparse1).toUpperCase();
			String sparseVal2 = String.valueOf(sparse2).toUpperCase();
			TestConfiguration config = getTestConfiguration(TEST_NAME);
			loadTestConfiguration(config);
			
			String RI_HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = RI_HOME + TEST_NAME + ".dml";
			programArgs = new String[] {"-explain", "recompile_runtime", "-args",
				String.valueOf(imgSize), String.valueOf(numImg), 
				String.valueOf(numChannels), String.valueOf(numFilters), 
				String.valueOf(filterSize), String.valueOf(stride), String.valueOf(pad), 
				output("B"), sparseVal1, sparseVal2};
			fullRScriptName = RI_HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + imgSize + " " + numImg + 
					" " + numChannels + " " + numFilters + 
					" " + filterSize + " " + stride + " " + pad + " " + expectedDir() +
					" " + sparseVal1 + " " + sparseVal2; 
			
			// Run DML and R scripts
			runTest(true, false, null, -1);
			runRScript(true);
			
			HashMap<CellIndex, Double> bHM = readRMatrixFromExpectedDir("B");
			HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromOutputDir("B");
			TestUtils.compareMatrices(dmlfile, bHM, epsilon, "B-DML", "B-R");
		}
		finally {
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
}

