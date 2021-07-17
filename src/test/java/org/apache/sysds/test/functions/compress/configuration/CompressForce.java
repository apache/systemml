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

package org.apache.sysds.test.functions.compress.configuration;

import java.io.File;

import org.apache.sysds.common.Types.ExecType;
import org.junit.Ignore;
import org.junit.Test;

public class CompressForce extends CompressBase {

	public String TEST_NAME = "compress";
	public String TEST_DIR = "functions/compress/force/";
	public String TEST_CLASS_DIR = TEST_DIR + CompressForce.class.getSimpleName() + "/";
	private String TEST_CONF = "SystemDS-config-compress.xml";
	private File TEST_CONF_FILE = new File(SCRIPT_DIR + TEST_DIR, TEST_CONF);

	protected String getTestClassDir() {
		return TEST_CLASS_DIR;
	}

	protected String getTestName() {
		return TEST_NAME;
	}

	protected String getTestDir() {
		return TEST_DIR;
	}

	@Test
	public void testTranspose_CP() {
		runTest(1500, 20, 1, 1, ExecType.CP, "transpose");
	}

	@Test
	public void testTranspose_SP() {
		runTest(1500, 1, 2, 1, ExecType.SPARK, "transpose");
	}

	@Test
	public void testSum_CP() {
		runTest(1500, 20, 0, 1, ExecType.CP, "sum");
	}

	@Test
	public void testSum_SP() {
		runTest(1500, 1, 0, 1, ExecType.SPARK, "sum");
	}

	@Test
	public void testRowAggregate_CP() {
		runTest(1500, 20, 0, 1, ExecType.CP, "row_min");
	}

	@Test
	public void testRowAggregate_SP() {
		runTest(1500, 1, 0, 1, ExecType.SPARK, "row_min");
	}

	@Test
	public void testSequence_CP() {
		runTest(1500, 1, 1, 1, ExecType.CP, "plus_mm_ewbm_sum");
	}

	@Test
	public void testSequence_SP() {
		runTest(1500, 1, 2, 1, ExecType.SPARK, "plus_mm_ewbm_sum");
	}

	@Test
	public void testPlus_CP() {
		runTest(1500, 1, 0, 1, ExecType.CP, "plus");
	}

	@Test
	public void testPlus_MM_SP() {
		runTest(1500, 1, 0, 1, ExecType.SPARK, "plus_mm");
	}

	@Test
	public void test_ElementWiseBinaryMultiplyOp_right_CP() {
		runTest(1500, 1, 1, 1, ExecType.CP, "ewbm_right");
	}

	@Test
	public void test_ElementWiseBinaryMultiplyOp_right_SP() {
		runTest(1500, 1, 2, 1, ExecType.SPARK, "ewbm_right");
	}

	@Test
	public void test_ElementWiseBinaryMultiplyOp_left_CP() {
		runTest(1500, 1, 1, 1, ExecType.CP, "ewbm_left");
	}

	@Test
	public void test_ElementWiseBinaryMultiplyOp_left_SP() {
		runTest(1500, 1, 2, 1, ExecType.SPARK, "ewbm_left");
	}


	@Test
	public void test_ElementWiseBinaryMultiplyOp_left_SP_larger() {
		runTest(1500, 15, 2, 1, ExecType.SPARK, "ewbm_left");
	}

	@Test
	public void test_ElementWiseBinaryPlusOp_CP() {
		runTest(1500, 1, 0, 1, ExecType.CP, "ewbp");
	}

	@Test
	public void test_ElementWiseBinaryPlusOp_SP() {
		runTest(1500, 1, 0, 1, ExecType.SPARK, "ewbp");
	}

	@Test
	public void testPlus_MM_CP() {
		runTest(1500, 1, 0, 1, ExecType.CP, "plus_mm");
	}

	@Test
	public void testPlus_SP() {
		runTest(1500, 1, 0, 1, ExecType.SPARK, "plus");
	}

	@Test
	public void testMatrixMultRightSum_Smaller_CP() {
		runTest(1500, 1, 0, 1, ExecType.CP, "mmr_sum");
	}

	@Test
	public void testMatrixMultRightSum_Smaller_SP() {
		runTest(1500, 1, 0, 1, ExecType.SPARK, "mmr_sum");
	}

	@Test
	public void testMatrixMultRightSum_Larger_SP() {
		runTest(1500, 11, 0, 1, ExecType.SPARK, "mmr_sum");
	}

	@Test
	public void testMatrixMultLeftSum_CP() {
		runTest(1500, 1, 0, 1, ExecType.CP, "mml_sum");
	}

	@Test
	@Ignore
	public void testMatrixMultLeftSum_SP_SmallerThanLeft() {
		// see task: https://issues.apache.org/jira/browse/SYSTEMDS-3038
		runTest(1500, 1, 0, 1, ExecType.SPARK, "mml_sum");
	}

	@Test
	public void testMatrixMultLeftSum_SP_LargerThanLeft() {
		runTest(1500, 11, 0, 1, ExecType.SPARK, "mml_sum");
	}

	/**
	 * Override default configuration with custom test configuration to ensure scratch space and local temporary
	 * directory locations are also updated.
	 */
	@Override
	protected File getConfigTemplateFile() {
		return TEST_CONF_FILE;
	}
}
