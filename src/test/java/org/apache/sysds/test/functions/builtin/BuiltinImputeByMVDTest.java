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
package org.apache.sysds.test.functions.builtin;

import org.apache.sysds.common.Types;
import org.apache.sysds.lops.LopProperties;
import org.apache.sysds.runtime.io.FrameWriter;
import org.apache.sysds.runtime.io.FrameWriterFactory;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.util.UtilFunctions;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BuiltinImputeByMVDTest extends AutomatedTestBase {
	private final static String TEST_NAME = "imputeByMVD";
	private final static String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinImputeFDTest.class.getSimpleName() + "/";
	private final static int rows = 12;
	private final static int cols = 4;
	private final static double epsilon = 0.0000000001;

	private final static Types.ValueType[] schema = {Types.ValueType.BOOLEAN, Types.ValueType.STRING,
		Types.ValueType.STRING, Types.ValueType.FP64};

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"C"}));
	}

	@Test
	public void test1() throws IOException {
		runImpute_MVDTests(2,3, "max", 1,  LopProperties.ExecType.CP);
	}

	@Test
	public void test2() throws IOException {
		runImpute_MVDTests(2,3, "min", 2, LopProperties.ExecType.CP);
	}

	@Test
	public void test3() throws IOException {
		runImpute_MVDTests(2,3, "max", 1,  LopProperties.ExecType.SPARK);
	}

	@Test
	public void test4() throws IOException {
		runImpute_MVDTests(2,3, "min", 2, LopProperties.ExecType.SPARK);
	}


	private void runImpute_MVDTests(int source, int target, String replace, int test, LopProperties.ExecType instType)
		throws IOException
	{
		Types.ExecMode platformOld = setExecMode(instType);
		try {
			loadTestConfiguration(getTestConfiguration(TEST_NAME));
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[] {"-args", input("A"), String.valueOf(source),String.valueOf(target),
				String.valueOf(replace), output("B")};
			//initialize the frame data.
			FrameBlock frame1 = new FrameBlock(schema);
			FrameWriter writer = FrameWriterFactory.createFrameWriter(Types.FileFormat.CSV);
			double[][] A = getRandomMatrix(rows, cols, 0, 1, 0.7, -1);
			initFrameDataString(frame1, A);
			writer.writeFrameToHDFS(frame1.slice(0, rows - 1, 0, schema.length - 1, new FrameBlock()),
				input("A"), rows, schema.length);

			runTest(true, false, null, -1);
			FrameBlock frameRead = readDMLFrameFromHDFS("B", Types.FileFormat.BINARY);
			FrameBlock realFrame = tureOutput(A, test);
			verifyFrameData(frameRead, realFrame, schema);
		}
		finally {
			rtplatform = platformOld;
		}
	}
//	TODO add more test cases
	private static void initFrameDataString(FrameBlock frame1, double[][] data) {
		boolean[] b = new boolean[rows];
		long[] l = new long[rows];
		String[] s1 = null, s2 = null;
		for (int i = 0; i < rows; i++) {
			data[i][1] = (b[i] = (Boolean) UtilFunctions.doubleToObject(Types.ValueType.BOOLEAN, data[i][1], false)) ? 1 : 0;
			l[i] = (Long) UtilFunctions.doubleToObject(Types.ValueType.INT64, data[i][2], false);
		}

		//				salary ranges:
		//					Ph.D. = 11k - 15k
		//					Master = 8k - 10k
		//					Bachelor = 4k - 7k
		//					Diploma = 1k - 3k
//		Errors at Ph.D. = 8100 and Diploma =11000
		s1 = new String[]  {"PhD", "PhD", "Master", "Master", "Master", "Bachelor","Bachelor", "Diploma",
			"Diploma", "Diploma", "PhD","PhD"};
		s2 = new String[]  {"8100", "15000", "10000", "8100", "8000", "4500", "7000", "2000",
			"1000", "11000", "15000", "11000"};

		frame1.appendColumn(b);
		frame1.appendColumn(s1);
		frame1.appendColumn(s2);
		frame1.appendColumn(l);
	}

	private static FrameBlock tureOutput(double[][] data, int test) {
		FrameBlock frame1 = new FrameBlock(schema);
		boolean[] b = new boolean[rows];
		String[] s1 = null, s2 = null;
		switch (test)
		{
			case 1: //max case
				s1 = new String[]  {"PhD", "PhD", "Master", "Master", "Master", "Bachelor","Bachelor", "Diploma",
					"Diploma", "Diploma", "PhD","PhD"};
				s2 = new String[]   {"15000.0", "15000.0", "10000.0", "8100.0", "8000.0", "4500.0", "7000.0", "2000.0",
					"1000.0", "2000.0", "15000.0", "11000.0"};
				break;

			case 2: //min case
				s1 = new String[]  {"PhD", "PhD", "Master", "Master", "Master", "Bachelor","Bachelor", "Diploma",
					"Diploma", "Diploma", "PhD","PhD"};
				s2 = new String[]  {"11000.0", "15000.0", "10000.0", "8100.0", "8000.0", "4500.0", "7000.0", "2000.0",
					"1000.0", "1000.0", "15000.0", "11000.0"};
				break;
		}
		long[] l = new long[rows];
		for (int i = 0; i < rows; i++) {
			data[i][1] = (b[i] = (Boolean) UtilFunctions.doubleToObject(Types.ValueType.BOOLEAN, data[i][1], false)) ? 1 : 0;
			l[i] = (Long) UtilFunctions.doubleToObject(Types.ValueType.INT64, data[i][2], false);
		}
		frame1.appendColumn(b);
		frame1.appendColumn(s1);
		frame1.appendColumn(s2);
		frame1.appendColumn(l);
		return frame1;
	}

	private static void verifyFrameData(FrameBlock frame1, FrameBlock frame2, Types.ValueType[] schema) {
		for (int i = 0; i < frame1.getNumRows(); i++)
			for (int j = 0; j < frame1.getNumColumns(); j++) {
				Object val1 = UtilFunctions.stringToObject(schema[j], UtilFunctions.objectToString(frame1.get(i, j)));
				Object val2 = UtilFunctions.stringToObject(schema[j], UtilFunctions.objectToString(frame2.get(i, j)));
				if (TestUtils.compareToR(schema[j], val1, val2, epsilon) != 0)
					Assert.fail("The DML data for cell (" + i + "," + j + ") is " + val1 + ", not same as the expected value " + val2);
			}
	}
}
