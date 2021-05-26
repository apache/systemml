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

package org.apache.sysds.test.functions.binary.tensor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.common.Types.ExecType;
import org.apache.sysds.hops.BinaryOp;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;

import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class ElementwiseAdditionTest extends AutomatedTestBase {
	private final static String TEST_DIR = "functions/binary/tensor/";
	private final static String TEST_NAME = "ElementwiseAdditionTest";
	private final static String TEST_CLASS_DIR = TEST_DIR + ElementwiseAdditionTest.class.getSimpleName() + "/";

	private String _lvalue, _rvalue;
	private int[] _dimsLeft, _dimsRight;

	public ElementwiseAdditionTest(int[] dimsLeft, int[] dimsRight, String lv, String rv) {
		_dimsLeft = dimsLeft;
		_dimsRight = dimsRight;
		_lvalue = lv;
		_rvalue = rv;
	}

	@Parameters
	public static Collection<Object[]> data() {
		Object[][] data = new Object[][]{
				{new int[]{3, 4, 5}, new int[]{3, 4, 5}, "3", "-2"},
				{new int[]{1, 1, 1, 1, 1}, new int[]{1, 1, 1, 1, 1}, "2", "30000000000.0"},
				{new int[]{2000, 2000}, new int[]{2000, 2000}, "3.0", "-2.0"},
				{new int[]{2000, 2000}, new int[]{2000, 1}, "3.0", "-2.0"},
				{new int[]{2000, 2000}, new int[]{1, 1}, "3.0", "-2"},
				{new int[]{2000, 200, 40}, new int[]{2000, 200}, "3.0", "-2"},
				{new int[]{130, 130, 130}, new int[]{130, 130}, "1", "-2"},
		};
		return Arrays.asList(data);
	}

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"A.scalar"}));
	}

	@Test
	public void elementwiseAdditionTestCP() {
		testElementwiseAddition(TEST_NAME, ExecType.CP);
	}

	@Test
	public void elementwiseAdditionTestSpark() {
		BinaryOp.FORCED_BINARY_METHOD = null;
		testElementwiseAddition(TEST_NAME, ExecType.SPARK);
	}

	@Test
	public void elementwiseAdditionTestBroadcastSpark() {
		BinaryOp.FORCED_BINARY_METHOD = BinaryOp.MMBinaryMethod.MR_BINARY_M;
		testElementwiseAddition(TEST_NAME, ExecType.SPARK);
	}

	private void testElementwiseAddition(String testName, ExecType platform) {
		ExecMode platformOld = rtplatform;
		if (platform == ExecType.SPARK) {
			rtplatform = ExecMode.SPARK;
		}
		else {
			rtplatform = ExecMode.SINGLE_NODE;
		}

		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if (rtplatform == ExecMode.SPARK) {
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		}
		try {
			//TODO test correctness
			//assertTrue("the test is not done, needs comparison, of result.", false);
			getAndLoadTestConfiguration(TEST_NAME);

			String HOME = SCRIPT_DIR + TEST_DIR;

			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			String ldimString = Arrays.toString(_dimsLeft).replace("[", "")
					.replace(",", "").replace("]", "");
			String rdimString = Arrays.toString(_dimsRight).replace("[", "")
					.replace(",", "").replace("]", "");
			programArgs = new String[]{"-explain", "-args", ldimString, Integer.toString(_dimsLeft.length), rdimString,
					Integer.toString(_dimsRight.length), _lvalue, _rvalue, output("A")};

			runTest(true, false, null, -1);
		}
		finally {
			rtplatform = platformOld;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
}
