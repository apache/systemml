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

import static org.apache.sysml.api.mlcontext.ScriptFactory.dml;
import static org.apache.sysml.api.mlcontext.ScriptFactory.dmlFromFile;
import static org.apache.sysml.api.mlcontext.ScriptFactory.dmlFromInputStream;
import static org.apache.sysml.api.mlcontext.ScriptFactory.dmlFromLocalFile;
import static org.apache.sysml.api.mlcontext.ScriptFactory.dmlFromUrl;
import static org.apache.sysml.api.mlcontext.ScriptFactory.pydml;
import static org.apache.sysml.api.mlcontext.ScriptFactory.pydmlFromFile;
import static org.apache.sysml.api.mlcontext.ScriptFactory.pydmlFromInputStream;
import static org.apache.sysml.api.mlcontext.ScriptFactory.pydmlFromLocalFile;
import static org.apache.sysml.api.mlcontext.ScriptFactory.pydmlFromUrl;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.sysml.api.mlcontext.MLContextConversionUtil;
import org.apache.sysml.api.mlcontext.MLContextException;
import org.apache.sysml.api.mlcontext.MLContextUtil;
import org.apache.sysml.api.mlcontext.MLResults;
import org.apache.sysml.api.mlcontext.Matrix;
import org.apache.sysml.api.mlcontext.MatrixFormat;
import org.apache.sysml.api.mlcontext.MatrixMetadata;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.api.mlcontext.ScriptExecutor;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.util.DataConverter;
import org.apache.sysml.utils.Statistics;
import org.junit.Assert;
import org.junit.Test;

import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class MLContextTest extends MLContextTestBase {

	@Test
	public void testBasicGlobalVariablesTest() {
		System.out.println("MLContextTest - basic global variables test");
		Script script = dmlFromFile(baseDirectory + File.separator + "global-variables-test.dml");
		ml.execute(script);
		Assert.assertTrue(Statistics.getNoOfExecutedSPInst() == 0);
	}
	
	@Test
	public void testBuiltinConstantsTest() {
		System.out.println("MLContextTest - basic builtin constants test");
		Script script = dmlFromFile(baseDirectory + File.separator + "builtin-constants-test.dml");
		ml.execute(script);
		Assert.assertTrue(Statistics.getNoOfExecutedSPInst() == 0);
	}
	
	@Test
	public void testBasicExecuteEvalTest() {
		System.out.println("MLContextTest - basic eval test");
		setExpectedStdOut("10");
		Script script = dmlFromFile(baseDirectory + File.separator + "eval-test.dml");
		ml.execute(script);
	}
	
	@Test
	public void testRewriteExecuteEvalTest() {
		System.out.println("MLContextTest - eval rewrite test");
		Script script = dmlFromFile(baseDirectory + File.separator + "eval2-test.dml");
		ml.execute(script);
		Assert.assertTrue(Statistics.getNoOfExecutedSPInst() == 0);
	}

	@Test
	public void testCreateDMLScriptBasedOnStringAndExecute() {
		System.out.println("MLContextTest - create DML script based on string and execute");
		String testString = "Create DML script based on string and execute";
		setExpectedStdOut(testString);
		Script script = dml("print('" + testString + "');");
		ml.execute(script);
	}

	@Test
	public void testCreatePYDMLScriptBasedOnStringAndExecute() {
		System.out.println("MLContextTest - create PYDML script based on string and execute");
		String testString = "Create PYDML script based on string and execute";
		setExpectedStdOut(testString);
		Script script = pydml("print('" + testString + "')");
		ml.execute(script);
	}

	@Test
	public void testCreateDMLScriptBasedOnFileAndExecute() {
		System.out.println("MLContextTest - create DML script based on file and execute");
		setExpectedStdOut("hello world");
		Script script = dmlFromFile(baseDirectory + File.separator + "hello-world.dml");
		ml.execute(script);
	}

	@Test
	public void testCreatePYDMLScriptBasedOnFileAndExecute() {
		System.out.println("MLContextTest - create PYDML script based on file and execute");
		setExpectedStdOut("hello world");
		Script script = pydmlFromFile(baseDirectory + File.separator + "hello-world.pydml");
		ml.execute(script);
	}

	@Test
	public void testCreateDMLScriptBasedOnInputStreamAndExecute() throws IOException {
		System.out.println("MLContextTest - create DML script based on InputStream and execute");
		setExpectedStdOut("hello world");
		File file = new File(baseDirectory + File.separator + "hello-world.dml");
		try( InputStream is = new FileInputStream(file) ) {
			Script script = dmlFromInputStream(is);
			ml.execute(script);
		}
	}

	@Test
	public void testCreatePYDMLScriptBasedOnInputStreamAndExecute() throws IOException {
		System.out.println("MLContextTest - create PYDML script based on InputStream and execute");
		setExpectedStdOut("hello world");
		File file = new File(baseDirectory + File.separator + "hello-world.pydml");
		try( InputStream is = new FileInputStream(file) ) {
			Script script = pydmlFromInputStream(is);
			ml.execute(script);
		}
	}

	@Test
	public void testCreateDMLScriptBasedOnLocalFileAndExecute() {
		System.out.println("MLContextTest - create DML script based on local file and execute");
		setExpectedStdOut("hello world");
		File file = new File(baseDirectory + File.separator + "hello-world.dml");
		Script script = dmlFromLocalFile(file);
		ml.execute(script);
	}

	@Test
	public void testCreatePYDMLScriptBasedOnLocalFileAndExecute() {
		System.out.println("MLContextTest - create PYDML script based on local file and execute");
		setExpectedStdOut("hello world");
		File file = new File(baseDirectory + File.separator + "hello-world.pydml");
		Script script = pydmlFromLocalFile(file);
		ml.execute(script);
	}

	@Test
	public void testCreateDMLScriptBasedOnURL() throws MalformedURLException {
		System.out.println("MLContextTest - create DML script based on URL");
		String urlString = "https://raw.githubusercontent.com/apache/systemml/master/src/test/scripts/applications/hits/HITS.dml";
		URL url = new URL(urlString);
		Script script = dmlFromUrl(url);
		String expectedContent = "Licensed to the Apache Software Foundation";
		String s = script.getScriptString();
		assertTrue("Script string doesn't contain expected content: " + expectedContent, s.contains(expectedContent));
	}

	@Test
	public void testCreatePYDMLScriptBasedOnURL() throws MalformedURLException {
		System.out.println("MLContextTest - create PYDML script based on URL");
		String urlString = "https://raw.githubusercontent.com/apache/systemml/master/src/test/scripts/applications/hits/HITS.pydml";
		URL url = new URL(urlString);
		Script script = pydmlFromUrl(url);
		String expectedContent = "Licensed to the Apache Software Foundation";
		String s = script.getScriptString();
		assertTrue("Script string doesn't contain expected content: " + expectedContent, s.contains(expectedContent));
	}

	@Test
	public void testCreateDMLScriptBasedOnURLString() throws MalformedURLException {
		System.out.println("MLContextTest - create DML script based on URL string");
		String urlString = "https://raw.githubusercontent.com/apache/systemml/master/src/test/scripts/applications/hits/HITS.dml";
		Script script = dmlFromUrl(urlString);
		String expectedContent = "Licensed to the Apache Software Foundation";
		String s = script.getScriptString();
		assertTrue("Script string doesn't contain expected content: " + expectedContent, s.contains(expectedContent));
	}

	@Test
	public void testCreatePYDMLScriptBasedOnURLString() throws MalformedURLException {
		System.out.println("MLContextTest - create PYDML script based on URL string");
		String urlString = "https://raw.githubusercontent.com/apache/systemml/master/src/test/scripts/applications/hits/HITS.pydml";
		Script script = pydmlFromUrl(urlString);
		String expectedContent = "Licensed to the Apache Software Foundation";
		String s = script.getScriptString();
		assertTrue("Script string doesn't contain expected content: " + expectedContent, s.contains(expectedContent));
	}

	@Test
	public void testExecuteDMLScript() {
		System.out.println("MLContextTest - execute DML script");
		String testString = "hello dml world!";
		setExpectedStdOut(testString);
		Script script = new Script("print('" + testString + "');", org.apache.sysml.api.mlcontext.ScriptType.DML);
		ml.execute(script);
	}

	@Test
	public void testExecutePYDMLScript() {
		System.out.println("MLContextTest - execute PYDML script");
		String testString = "hello pydml world!";
		setExpectedStdOut(testString);
		Script script = new Script("print('" + testString + "')", org.apache.sysml.api.mlcontext.ScriptType.PYDML);
		ml.execute(script);
	}

	@Test
	public void testInputParametersAddDML() {
		System.out.println("MLContextTest - input parameters add DML");

		String s = "x = $X; y = $Y; print('x + y = ' + (x + y));";
		Script script = dml(s).in("$X", 3).in("$Y", 4);
		setExpectedStdOut("x + y = 7");
		ml.execute(script);
	}

	@Test
	public void testInputParametersAddPYDML() {
		System.out.println("MLContextTest - input parameters add PYDML");

		String s = "x = $X\ny = $Y\nprint('x + y = ' + (x + y))";
		Script script = pydml(s).in("$X", 3).in("$Y", 4);
		setExpectedStdOut("x + y = 7");
		ml.execute(script);
	}

	@Test
	public void testJavaRDDCSVSumDML() {
		System.out.println("MLContextTest - JavaRDD<String> CSV sum DML");

		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		Script script = dml("print('sum: ' + sum(M));").in("M", javaRDD);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testJavaRDDCSVSumPYDML() {
		System.out.println("MLContextTest - JavaRDD<String> CSV sum PYDML");

		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		Script script = pydml("print('sum: ' + sum(M))").in("M", javaRDD);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testJavaRDDIJVSumDML() {
		System.out.println("MLContextTest - JavaRDD<String> IJV sum DML");

		List<String> list = new ArrayList<String>();
		list.add("1 1 5");
		list.add("2 2 5");
		list.add("3 3 5");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.IJV, 3, 3);

		Script script = dml("print('sum: ' + sum(M));").in("M", javaRDD, mm);
		setExpectedStdOut("sum: 15.0");
		ml.execute(script);
	}

	@Test
	public void testJavaRDDIJVSumPYDML() {
		System.out.println("MLContextTest - JavaRDD<String> IJV sum PYDML");

		List<String> list = new ArrayList<String>();
		list.add("1 1 5");
		list.add("2 2 5");
		list.add("3 3 5");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.IJV, 3, 3);

		Script script = pydml("print('sum: ' + sum(M))").in("M", javaRDD, mm);
		setExpectedStdOut("sum: 15.0");
		ml.execute(script);
	}

	@Test
	public void testJavaRDDAndInputParameterDML() {
		System.out.println("MLContextTest - JavaRDD<String> and input parameter DML");

		List<String> list = new ArrayList<String>();
		list.add("1,2");
		list.add("3,4");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		String s = "M = M + $X; print('sum: ' + sum(M));";
		Script script = dml(s).in("M", javaRDD).in("$X", 1);
		setExpectedStdOut("sum: 14.0");
		ml.execute(script);
	}

	@Test
	public void testJavaRDDAndInputParameterPYDML() {
		System.out.println("MLContextTest - JavaRDD<String> and input parameter PYDML");

		List<String> list = new ArrayList<String>();
		list.add("1,2");
		list.add("3,4");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		String s = "M = M + $X\nprint('sum: ' + sum(M))";
		Script script = pydml(s).in("M", javaRDD).in("$X", 1);
		setExpectedStdOut("sum: 14.0");
		ml.execute(script);
	}

	@Test
	public void testInputMapDML() {
		System.out.println("MLContextTest - input map DML");

		List<String> list = new ArrayList<String>();
		list.add("10,20");
		list.add("30,40");
		final JavaRDD<String> javaRDD = sc.parallelize(list);

		Map<String, Object> inputs = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put("$X", 2);
				put("M", javaRDD);
			}
		};

		String s = "M = M + $X; print('sum: ' + sum(M));";
		Script script = dml(s).in(inputs);
		setExpectedStdOut("sum: 108.0");
		ml.execute(script);
	}

	@Test
	public void testInputMapPYDML() {
		System.out.println("MLContextTest - input map PYDML");

		List<String> list = new ArrayList<String>();
		list.add("10,20");
		list.add("30,40");
		final JavaRDD<String> javaRDD = sc.parallelize(list);

		Map<String, Object> inputs = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put("$X", 2);
				put("M", javaRDD);
			}
		};

		String s = "M = M + $X\nprint('sum: ' + sum(M))";
		Script script = pydml(s).in(inputs);
		setExpectedStdOut("sum: 108.0");
		ml.execute(script);
	}

	@Test
	public void testCustomExecutionStepDML() {
		System.out.println("MLContextTest - custom execution step DML");
		String testString = "custom execution step";
		setExpectedStdOut(testString);
		Script script = new Script("print('" + testString + "');", org.apache.sysml.api.mlcontext.ScriptType.DML);

		ScriptExecutor scriptExecutor = new ScriptExecutor() {
			@Override
			protected void showExplanation() {}
		};
		ml.execute(script, scriptExecutor);
	}

	@Test
	public void testCustomExecutionStepPYDML() {
		System.out.println("MLContextTest - custom execution step PYDML");
		String testString = "custom execution step";
		setExpectedStdOut(testString);
		Script script = new Script("print('" + testString + "')", org.apache.sysml.api.mlcontext.ScriptType.PYDML);

		ScriptExecutor scriptExecutor = new ScriptExecutor() {
			@Override
			protected void showExplanation() {}
		};
		ml.execute(script, scriptExecutor);
	}

	@Test
	public void testRDDSumCSVDML() {
		System.out.println("MLContextTest - RDD<String> CSV sum DML");

		List<String> list = new ArrayList<String>();
		list.add("1,1,1");
		list.add("2,2,2");
		list.add("3,3,3");
		JavaRDD<String> javaRDD = sc.parallelize(list);
		RDD<String> rdd = JavaRDD.toRDD(javaRDD);

		Script script = dml("print('sum: ' + sum(M));").in("M", rdd);
		setExpectedStdOut("sum: 18.0");
		ml.execute(script);
	}

	@Test
	public void testRDDSumCSVPYDML() {
		System.out.println("MLContextTest - RDD<String> CSV sum PYDML");

		List<String> list = new ArrayList<String>();
		list.add("1,1,1");
		list.add("2,2,2");
		list.add("3,3,3");
		JavaRDD<String> javaRDD = sc.parallelize(list);
		RDD<String> rdd = JavaRDD.toRDD(javaRDD);

		Script script = pydml("print('sum: ' + sum(M))").in("M", rdd);
		setExpectedStdOut("sum: 18.0");
		ml.execute(script);
	}

	@Test
	public void testRDDSumIJVDML() {
		System.out.println("MLContextTest - RDD<String> IJV sum DML");

		List<String> list = new ArrayList<String>();
		list.add("1 1 1");
		list.add("2 1 2");
		list.add("1 2 3");
		list.add("3 3 4");
		JavaRDD<String> javaRDD = sc.parallelize(list);
		RDD<String> rdd = JavaRDD.toRDD(javaRDD);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.IJV, 3, 3);

		Script script = dml("print('sum: ' + sum(M));").in("M", rdd, mm);
		setExpectedStdOut("sum: 10.0");
		ml.execute(script);
	}

	@Test
	public void testRDDSumIJVPYDML() {
		System.out.println("MLContextTest - RDD<String> IJV sum PYDML");

		List<String> list = new ArrayList<String>();
		list.add("1 1 1");
		list.add("2 1 2");
		list.add("1 2 3");
		list.add("3 3 4");
		JavaRDD<String> javaRDD = sc.parallelize(list);
		RDD<String> rdd = JavaRDD.toRDD(javaRDD);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.IJV, 3, 3);

		Script script = pydml("print('sum: ' + sum(M))").in("M", rdd, mm);
		setExpectedStdOut("sum: 10.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLDoublesWithNoIDColumn() {
		System.out.println("MLContextTest - DataFrame sum DML, doubles with no ID column");

		List<String> list = new ArrayList<String>();
		list.add("10,20,30");
		list.add("40,50,60");
		list.add("70,80,90");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_DOUBLES);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 450.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLDoublesWithNoIDColumn() {
		System.out.println("MLContextTest - DataFrame sum PYDML, doubles with no ID column");

		List<String> list = new ArrayList<String>();
		list.add("10,20,30");
		list.add("40,50,60");
		list.add("70,80,90");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_DOUBLES);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 450.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLDoublesWithIDColumn() {
		System.out.println("MLContextTest - DataFrame sum DML, doubles with ID column");

		List<String> list = new ArrayList<String>();
		list.add("1,1,2,3");
		list.add("2,4,5,6");
		list.add("3,7,8,9");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_DOUBLES_WITH_INDEX);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLDoublesWithIDColumn() {
		System.out.println("MLContextTest - DataFrame sum PYDML, doubles with ID column");

		List<String> list = new ArrayList<String>();
		list.add("1,1,2,3");
		list.add("2,4,5,6");
		list.add("3,7,8,9");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_DOUBLES_WITH_INDEX);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLDoublesWithIDColumnSortCheck() {
		System.out.println("MLContextTest - DataFrame sum DML, doubles with ID column sort check");

		List<String> list = new ArrayList<String>();
		list.add("3,7,8,9");
		list.add("1,1,2,3");
		list.add("2,4,5,6");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_DOUBLES_WITH_INDEX);

		Script script = dml("print('M[1,1]: ' + as.scalar(M[1,1]));").in("M", dataFrame, mm);
		setExpectedStdOut("M[1,1]: 1.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLDoublesWithIDColumnSortCheck() {
		System.out.println("MLContextTest - DataFrame sum PYDML ID, doubles with ID column sort check");

		List<String> list = new ArrayList<String>();
		list.add("3,7,8,9");
		list.add("1,1,2,3");
		list.add("2,4,5,6");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_DOUBLES_WITH_INDEX);

		Script script = pydml("print('M[0,0]: ' + scalar(M[0,0]))").in("M", dataFrame, mm);
		setExpectedStdOut("M[0,0]: 1.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLVectorWithIDColumn() {
		System.out.println("MLContextTest - DataFrame sum DML, vector with ID column");

		List<Tuple2<Double, Vector>> list = new ArrayList<Tuple2<Double, Vector>>();
		list.add(new Tuple2<Double, Vector>(1.0, Vectors.dense(1.0, 2.0, 3.0)));
		list.add(new Tuple2<Double, Vector>(2.0, Vectors.dense(4.0, 5.0, 6.0)));
		list.add(new Tuple2<Double, Vector>(3.0, Vectors.dense(7.0, 8.0, 9.0)));
		JavaRDD<Tuple2<Double, Vector>> javaRddTuple = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddTuple.map(new DoubleVectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_VECTOR_WITH_INDEX);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLVectorWithIDColumn() {
		System.out.println("MLContextTest - DataFrame sum PYDML, vector with ID column");

		List<Tuple2<Double, Vector>> list = new ArrayList<Tuple2<Double, Vector>>();
		list.add(new Tuple2<Double, Vector>(1.0, Vectors.dense(1.0, 2.0, 3.0)));
		list.add(new Tuple2<Double, Vector>(2.0, Vectors.dense(4.0, 5.0, 6.0)));
		list.add(new Tuple2<Double, Vector>(3.0, Vectors.dense(7.0, 8.0, 9.0)));
		JavaRDD<Tuple2<Double, Vector>> javaRddTuple = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddTuple.map(new DoubleVectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_VECTOR_WITH_INDEX);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLMllibVectorWithIDColumn() {
		System.out.println("MLContextTest - DataFrame sum DML, mllib vector with ID column");

		List<Tuple2<Double, org.apache.spark.mllib.linalg.Vector>> list = new ArrayList<Tuple2<Double, org.apache.spark.mllib.linalg.Vector>>();
		list.add(new Tuple2<Double, org.apache.spark.mllib.linalg.Vector>(1.0,
				org.apache.spark.mllib.linalg.Vectors.dense(1.0, 2.0, 3.0)));
		list.add(new Tuple2<Double, org.apache.spark.mllib.linalg.Vector>(2.0,
				org.apache.spark.mllib.linalg.Vectors.dense(4.0, 5.0, 6.0)));
		list.add(new Tuple2<Double, org.apache.spark.mllib.linalg.Vector>(3.0,
				org.apache.spark.mllib.linalg.Vectors.dense(7.0, 8.0, 9.0)));
		JavaRDD<Tuple2<Double, org.apache.spark.mllib.linalg.Vector>> javaRddTuple = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddTuple.map(new DoubleMllibVectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", new org.apache.spark.mllib.linalg.VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_VECTOR_WITH_INDEX);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLMllibVectorWithIDColumn() {
		System.out.println("MLContextTest - DataFrame sum PYDML, mllib vector with ID column");

		List<Tuple2<Double, org.apache.spark.mllib.linalg.Vector>> list = new ArrayList<Tuple2<Double, org.apache.spark.mllib.linalg.Vector>>();
		list.add(new Tuple2<Double, org.apache.spark.mllib.linalg.Vector>(1.0,
				org.apache.spark.mllib.linalg.Vectors.dense(1.0, 2.0, 3.0)));
		list.add(new Tuple2<Double, org.apache.spark.mllib.linalg.Vector>(2.0,
				org.apache.spark.mllib.linalg.Vectors.dense(4.0, 5.0, 6.0)));
		list.add(new Tuple2<Double, org.apache.spark.mllib.linalg.Vector>(3.0,
				org.apache.spark.mllib.linalg.Vectors.dense(7.0, 8.0, 9.0)));
		JavaRDD<Tuple2<Double, org.apache.spark.mllib.linalg.Vector>> javaRddTuple = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddTuple.map(new DoubleMllibVectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", new org.apache.spark.mllib.linalg.VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_VECTOR_WITH_INDEX);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLVectorWithNoIDColumn() {
		System.out.println("MLContextTest - DataFrame sum DML, vector with no ID column");

		List<Vector> list = new ArrayList<Vector>();
		list.add(Vectors.dense(1.0, 2.0, 3.0));
		list.add(Vectors.dense(4.0, 5.0, 6.0));
		list.add(Vectors.dense(7.0, 8.0, 9.0));
		JavaRDD<Vector> javaRddVector = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddVector.map(new VectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_VECTOR);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLVectorWithNoIDColumn() {
		System.out.println("MLContextTest - DataFrame sum PYDML, vector with no ID column");

		List<Vector> list = new ArrayList<Vector>();
		list.add(Vectors.dense(1.0, 2.0, 3.0));
		list.add(Vectors.dense(4.0, 5.0, 6.0));
		list.add(Vectors.dense(7.0, 8.0, 9.0));
		JavaRDD<Vector> javaRddVector = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddVector.map(new VectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_VECTOR);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLMllibVectorWithNoIDColumn() {
		System.out.println("MLContextTest - DataFrame sum DML, mllib vector with no ID column");

		List<org.apache.spark.mllib.linalg.Vector> list = new ArrayList<org.apache.spark.mllib.linalg.Vector>();
		list.add(org.apache.spark.mllib.linalg.Vectors.dense(1.0, 2.0, 3.0));
		list.add(org.apache.spark.mllib.linalg.Vectors.dense(4.0, 5.0, 6.0));
		list.add(org.apache.spark.mllib.linalg.Vectors.dense(7.0, 8.0, 9.0));
		JavaRDD<org.apache.spark.mllib.linalg.Vector> javaRddVector = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddVector.map(new MllibVectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", new org.apache.spark.mllib.linalg.VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_VECTOR);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLMllibVectorWithNoIDColumn() {
		System.out.println("MLContextTest - DataFrame sum PYDML, mllib vector with no ID column");

		List<org.apache.spark.mllib.linalg.Vector> list = new ArrayList<org.apache.spark.mllib.linalg.Vector>();
		list.add(org.apache.spark.mllib.linalg.Vectors.dense(1.0, 2.0, 3.0));
		list.add(org.apache.spark.mllib.linalg.Vectors.dense(4.0, 5.0, 6.0));
		list.add(org.apache.spark.mllib.linalg.Vectors.dense(7.0, 8.0, 9.0));
		JavaRDD<org.apache.spark.mllib.linalg.Vector> javaRddVector = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddVector.map(new MllibVectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", new org.apache.spark.mllib.linalg.VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.DF_VECTOR);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	static class DoubleVectorRow implements Function<Tuple2<Double, Vector>, Row> {
		private static final long serialVersionUID = 3605080559931384163L;

		@Override
		public Row call(Tuple2<Double, Vector> tup) throws Exception {
			Double doub = tup._1();
			Vector vect = tup._2();
			return RowFactory.create(doub, vect);
		}
	}

	static class DoubleMllibVectorRow implements Function<Tuple2<Double, org.apache.spark.mllib.linalg.Vector>, Row> {
		private static final long serialVersionUID = -3121178154451876165L;

		@Override
		public Row call(Tuple2<Double, org.apache.spark.mllib.linalg.Vector> tup) throws Exception {
			Double doub = tup._1();
			org.apache.spark.mllib.linalg.Vector vect = tup._2();
			return RowFactory.create(doub, vect);
		}
	}

	static class VectorRow implements Function<Vector, Row> {
		private static final long serialVersionUID = 7077761802433569068L;

		@Override
		public Row call(Vector vect) throws Exception {
			return RowFactory.create(vect);
		}
	}

	static class MllibVectorRow implements Function<org.apache.spark.mllib.linalg.Vector, Row> {
		private static final long serialVersionUID = -408929813562996706L;

		@Override
		public Row call(org.apache.spark.mllib.linalg.Vector vect) throws Exception {
			return RowFactory.create(vect);
		}
	}

	static class CommaSeparatedValueStringToRow implements Function<String, Row> {
		private static final long serialVersionUID = -7871020122671747808L;

		@Override
		public Row call(String str) throws Exception {
			String[] fields = str.split(",");
			return RowFactory.create((Object[]) fields);
		}
	}

	static class CommaSeparatedValueStringToDoubleArrayRow implements Function<String, Row> {
		private static final long serialVersionUID = -8058786466523637317L;

		@Override
		public Row call(String str) throws Exception {
			String[] strings = str.split(",");
			Double[] doubles = new Double[strings.length];
			for (int i = 0; i < strings.length; i++) {
				doubles[i] = Double.parseDouble(strings[i]);
			}
			return RowFactory.create((Object[]) doubles);
		}
	}

	@Test
	public void testCSVMatrixFileInputParameterSumDML() {
		System.out.println("MLContextTest - CSV matrix file input parameter sum DML");

		String s = "M = read($Min); print('sum: ' + sum(M));";
		String csvFile = baseDirectory + File.separator + "1234.csv";
		setExpectedStdOut("sum: 10.0");
		ml.execute(dml(s).in("$Min", csvFile));
	}

	@Test
	public void testCSVMatrixFileInputParameterSumPYDML() {
		System.out.println("MLContextTest - CSV matrix file input parameter sum PYDML");

		String s = "M = load($Min)\nprint('sum: ' + sum(M))";
		String csvFile = baseDirectory + File.separator + "1234.csv";
		setExpectedStdOut("sum: 10.0");
		ml.execute(pydml(s).in("$Min", csvFile));
	}

	@Test
	public void testCSVMatrixFileInputVariableSumDML() {
		System.out.println("MLContextTest - CSV matrix file input variable sum DML");

		String s = "M = read($Min); print('sum: ' + sum(M));";
		String csvFile = baseDirectory + File.separator + "1234.csv";
		setExpectedStdOut("sum: 10.0");
		ml.execute(dml(s).in("$Min", csvFile));
	}

	@Test
	public void testCSVMatrixFileInputVariableSumPYDML() {
		System.out.println("MLContextTest - CSV matrix file input variable sum PYDML");

		String s = "M = load($Min)\nprint('sum: ' + sum(M))";
		String csvFile = baseDirectory + File.separator + "1234.csv";
		setExpectedStdOut("sum: 10.0");
		ml.execute(pydml(s).in("$Min", csvFile));
	}

	@Test
	public void test2DDoubleSumDML() {
		System.out.println("MLContextTest - two-dimensional double array sum DML");

		double[][] matrix = new double[][] { { 10.0, 20.0 }, { 30.0, 40.0 } };

		Script script = dml("print('sum: ' + sum(M));").in("M", matrix);
		setExpectedStdOut("sum: 100.0");
		ml.execute(script);
	}

	@Test
	public void test2DDoubleSumPYDML() {
		System.out.println("MLContextTest - two-dimensional double array sum PYDML");

		double[][] matrix = new double[][] { { 10.0, 20.0 }, { 30.0, 40.0 } };

		Script script = pydml("print('sum: ' + sum(M))").in("M", matrix);
		setExpectedStdOut("sum: 100.0");
		ml.execute(script);
	}

	@Test
	public void testAddScalarIntegerInputsDML() {
		System.out.println("MLContextTest - add scalar integer inputs DML");
		String s = "total = in1 + in2; print('total: ' + total);";
		Script script = dml(s).in("in1", 1).in("in2", 2);
		setExpectedStdOut("total: 3");
		ml.execute(script);
	}

	@Test
	public void testAddScalarIntegerInputsPYDML() {
		System.out.println("MLContextTest - add scalar integer inputs PYDML");
		String s = "total = in1 + in2\nprint('total: ' + total)";
		Script script = pydml(s).in("in1", 1).in("in2", 2);
		setExpectedStdOut("total: 3");
		ml.execute(script);
	}

	@Test
	public void testInputScalaMapDML() {
		System.out.println("MLContextTest - input Scala map DML");

		List<String> list = new ArrayList<String>();
		list.add("10,20");
		list.add("30,40");
		final JavaRDD<String> javaRDD = sc.parallelize(list);

		Map<String, Object> inputs = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put("$X", 2);
				put("M", javaRDD);
			}
		};

		scala.collection.mutable.Map<String, Object> scalaMap = JavaConversions.mapAsScalaMap(inputs);

		String s = "M = M + $X; print('sum: ' + sum(M));";
		Script script = dml(s).in(scalaMap);
		setExpectedStdOut("sum: 108.0");
		ml.execute(script);
	}

	@Test
	public void testInputScalaMapPYDML() {
		System.out.println("MLContextTest - input Scala map PYDML");

		List<String> list = new ArrayList<String>();
		list.add("10,20");
		list.add("30,40");
		final JavaRDD<String> javaRDD = sc.parallelize(list);

		Map<String, Object> inputs = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put("$X", 2);
				put("M", javaRDD);
			}
		};

		scala.collection.mutable.Map<String, Object> scalaMap = JavaConversions.mapAsScalaMap(inputs);

		String s = "M = M + $X\nprint('sum: ' + sum(M))";
		Script script = pydml(s).in(scalaMap);
		setExpectedStdOut("sum: 108.0");
		ml.execute(script);
	}

	@Test
	public void testOutputDoubleArrayMatrixDML() {
		System.out.println("MLContextTest - output double array matrix DML");
		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		double[][] matrix = ml.execute(dml(s).out("M")).getMatrixAs2DDoubleArray("M");
		Assert.assertEquals(1.0, matrix[0][0], 0);
		Assert.assertEquals(2.0, matrix[0][1], 0);
		Assert.assertEquals(3.0, matrix[1][0], 0);
		Assert.assertEquals(4.0, matrix[1][1], 0);
	}

	@Test
	public void testOutputDoubleArrayMatrixPYDML() {
		System.out.println("MLContextTest - output double array matrix PYDML");
		String s = "M = full('1 2 3 4', rows=2, cols=2)";
		double[][] matrix = ml.execute(pydml(s).out("M")).getMatrixAs2DDoubleArray("M");
		Assert.assertEquals(1.0, matrix[0][0], 0);
		Assert.assertEquals(2.0, matrix[0][1], 0);
		Assert.assertEquals(3.0, matrix[1][0], 0);
		Assert.assertEquals(4.0, matrix[1][1], 0);
	}

	@Test
	public void testOutputScalarLongDML() {
		System.out.println("MLContextTest - output scalar long DML");
		String s = "m = 5;";
		long result = ml.execute(dml(s).out("m")).getLong("m");
		Assert.assertEquals(5, result);
	}

	@Test
	public void testOutputScalarLongPYDML() {
		System.out.println("MLContextTest - output scalar long PYDML");
		String s = "m = 5";
		long result = ml.execute(pydml(s).out("m")).getLong("m");
		Assert.assertEquals(5, result);
	}

	@Test
	public void testOutputScalarDoubleDML() {
		System.out.println("MLContextTest - output scalar double DML");
		String s = "m = 1.23";
		double result = ml.execute(dml(s).out("m")).getDouble("m");
		Assert.assertEquals(1.23, result, 0);
	}

	@Test
	public void testOutputScalarDoublePYDML() {
		System.out.println("MLContextTest - output scalar double PYDML");
		String s = "m = 1.23";
		double result = ml.execute(pydml(s).out("m")).getDouble("m");
		Assert.assertEquals(1.23, result, 0);
	}

	@Test
	public void testOutputScalarBooleanDML() {
		System.out.println("MLContextTest - output scalar boolean DML");
		String s = "m = FALSE;";
		boolean result = ml.execute(dml(s).out("m")).getBoolean("m");
		Assert.assertEquals(false, result);
	}

	@Test
	public void testOutputScalarBooleanPYDML() {
		System.out.println("MLContextTest - output scalar boolean PYDML");
		String s = "m = False";
		boolean result = ml.execute(pydml(s).out("m")).getBoolean("m");
		Assert.assertEquals(false, result);
	}

	@Test
	public void testOutputScalarStringDML() {
		System.out.println("MLContextTest - output scalar string DML");
		String s = "m = 'hello';";
		String result = ml.execute(dml(s).out("m")).getString("m");
		Assert.assertEquals("hello", result);
	}

	@Test
	public void testOutputScalarStringPYDML() {
		System.out.println("MLContextTest - output scalar string PYDML");
		String s = "m = 'hello'";
		String result = ml.execute(pydml(s).out("m")).getString("m");
		Assert.assertEquals("hello", result);
	}

	@Test
	public void testInputFrameDML() {
		System.out.println("MLContextTest - input frame DML");

		String s = "M = read($Min, data_type='frame', format='csv'); print(toString(M));";
		String csvFile = baseDirectory + File.separator + "one-two-three-four.csv";
		Script script = dml(s).in("$Min", csvFile);
		setExpectedStdOut("one");
		ml.execute(script);
	}

	@Test
	public void testInputFramePYDML() {
		System.out.println("MLContextTest - input frame PYDML");

		String s = "M = load($Min, data_type='frame', format='csv')\nprint(toString(M))";
		String csvFile = baseDirectory + File.separator + "one-two-three-four.csv";
		Script script = pydml(s).in("$Min", csvFile);
		setExpectedStdOut("one");
		ml.execute(script);
	}

	@Test
	public void testOutputJavaRDDStringIJVDML() {
		System.out.println("MLContextTest - output Java RDD String IJV DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		JavaRDD<String> javaRDDStringIJV = results.getJavaRDDStringIJV("M");
		List<String> lines = javaRDDStringIJV.collect();
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testOutputJavaRDDStringIJVPYDML() {
		System.out.println("MLContextTest - output Java RDD String IJV PYDML");

		String s = "M = full('1 2 3 4', rows=2, cols=2)";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		JavaRDD<String> javaRDDStringIJV = results.getJavaRDDStringIJV("M");
		List<String> lines = javaRDDStringIJV.collect();
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testOutputJavaRDDStringCSVDenseDML() {
		System.out.println("MLContextTest - output Java RDD String CSV Dense DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2); print(toString(M));";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		JavaRDD<String> javaRDDStringCSV = results.getJavaRDDStringCSV("M");
		List<String> lines = javaRDDStringCSV.collect();
		Assert.assertEquals("1.0,2.0", lines.get(0));
		Assert.assertEquals("3.0,4.0", lines.get(1));
	}

	@Test
	public void testOutputJavaRDDStringCSVDensePYDML() {
		System.out.println("MLContextTest - output Java RDD String CSV Dense PYDML");

		String s = "M = full('1 2 3 4', rows=2, cols=2)\nprint(toString(M))";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		JavaRDD<String> javaRDDStringCSV = results.getJavaRDDStringCSV("M");
		List<String> lines = javaRDDStringCSV.collect();
		Assert.assertEquals("1.0,2.0", lines.get(0));
		Assert.assertEquals("3.0,4.0", lines.get(1));
	}

	/**
	 * Reading from dense and sparse matrices is handled differently, so we have
	 * tests for both dense and sparse matrices.
	 */
	@Test
	public void testOutputJavaRDDStringCSVSparseDML() {
		System.out.println("MLContextTest - output Java RDD String CSV Sparse DML");

		String s = "M = matrix(0, rows=10, cols=10); M[1,1]=1; M[1,2]=2; M[2,1]=3; M[2,2]=4; print(toString(M));";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		JavaRDD<String> javaRDDStringCSV = results.getJavaRDDStringCSV("M");
		List<String> lines = javaRDDStringCSV.collect();
		Assert.assertEquals("1.0,2.0", lines.get(0));
		Assert.assertEquals("3.0,4.0", lines.get(1));
	}

	/**
	 * Reading from dense and sparse matrices is handled differently, so we have
	 * tests for both dense and sparse matrices.
	 */
	@Test
	public void testOutputJavaRDDStringCSVSparsePYDML() {
		System.out.println("MLContextTest - output Java RDD String CSV Sparse PYDML");

		String s = "M = full(0, rows=10, cols=10)\nM[0,0]=1\nM[0,1]=2\nM[1,0]=3\nM[1,1]=4\nprint(toString(M))";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		JavaRDD<String> javaRDDStringCSV = results.getJavaRDDStringCSV("M");
		List<String> lines = javaRDDStringCSV.collect();
		Assert.assertEquals("1.0,2.0", lines.get(0));
		Assert.assertEquals("3.0,4.0", lines.get(1));
	}

	@Test
	public void testOutputRDDStringIJVDML() {
		System.out.println("MLContextTest - output RDD String IJV DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		RDD<String> rddStringIJV = results.getRDDStringIJV("M");
		Iterator<String> iterator = rddStringIJV.toLocalIterator();
		Assert.assertEquals("1 1 1.0", iterator.next());
		Assert.assertEquals("1 2 2.0", iterator.next());
		Assert.assertEquals("2 1 3.0", iterator.next());
		Assert.assertEquals("2 2 4.0", iterator.next());
	}

	@Test
	public void testOutputRDDStringIJVPYDML() {
		System.out.println("MLContextTest - output RDD String IJV PYDML");

		String s = "M = full('1 2 3 4', rows=2, cols=2)";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		RDD<String> rddStringIJV = results.getRDDStringIJV("M");
		Iterator<String> iterator = rddStringIJV.toLocalIterator();
		Assert.assertEquals("1 1 1.0", iterator.next());
		Assert.assertEquals("1 2 2.0", iterator.next());
		Assert.assertEquals("2 1 3.0", iterator.next());
		Assert.assertEquals("2 2 4.0", iterator.next());
	}

	@Test
	public void testOutputRDDStringCSVDenseDML() {
		System.out.println("MLContextTest - output RDD String CSV Dense DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2); print(toString(M));";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		RDD<String> rddStringCSV = results.getRDDStringCSV("M");
		Iterator<String> iterator = rddStringCSV.toLocalIterator();
		Assert.assertEquals("1.0,2.0", iterator.next());
		Assert.assertEquals("3.0,4.0", iterator.next());
	}

	@Test
	public void testOutputRDDStringCSVDensePYDML() {
		System.out.println("MLContextTest - output RDD String CSV Dense PYDML");

		String s = "M = full('1 2 3 4', rows=2, cols=2)\nprint(toString(M))";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		RDD<String> rddStringCSV = results.getRDDStringCSV("M");
		Iterator<String> iterator = rddStringCSV.toLocalIterator();
		Assert.assertEquals("1.0,2.0", iterator.next());
		Assert.assertEquals("3.0,4.0", iterator.next());
	}

	@Test
	public void testOutputRDDStringCSVSparseDML() {
		System.out.println("MLContextTest - output RDD String CSV Sparse DML");

		String s = "M = matrix(0, rows=10, cols=10); M[1,1]=1; M[1,2]=2; M[2,1]=3; M[2,2]=4; print(toString(M));";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		RDD<String> rddStringCSV = results.getRDDStringCSV("M");
		Iterator<String> iterator = rddStringCSV.toLocalIterator();
		Assert.assertEquals("1.0,2.0", iterator.next());
		Assert.assertEquals("3.0,4.0", iterator.next());
	}

	@Test
	public void testOutputRDDStringCSVSparsePYDML() {
		System.out.println("MLContextTest - output RDD String CSV Sparse PYDML");

		String s = "M = full(0, rows=10, cols=10)\nM[0,0]=1\nM[0,1]=2\nM[1,0]=3\nM[1,1]=4\nprint(toString(M))";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		RDD<String> rddStringCSV = results.getRDDStringCSV("M");
		Iterator<String> iterator = rddStringCSV.toLocalIterator();
		Assert.assertEquals("1.0,2.0", iterator.next());
		Assert.assertEquals("3.0,4.0", iterator.next());
	}

	@Test
	public void testOutputDataFrameDML() {
		System.out.println("MLContextTest - output DataFrame DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrame("M");
		List<Row> list = dataFrame.collectAsList();
		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertEquals(1.0, row1.getDouble(1), 0.0);
		Assert.assertEquals(2.0, row1.getDouble(2), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Assert.assertEquals(3.0, row2.getDouble(1), 0.0);
		Assert.assertEquals(4.0, row2.getDouble(2), 0.0);
	}

	@Test
	public void testOutputDataFramePYDML() {
		System.out.println("MLContextTest - output DataFrame PYDML");

		String s = "M = full('1 2 3 4', rows=2, cols=2)";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrame("M");
		List<Row> list = dataFrame.collectAsList();
		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertEquals(1.0, row1.getDouble(1), 0.0);
		Assert.assertEquals(2.0, row1.getDouble(2), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Assert.assertEquals(3.0, row2.getDouble(1), 0.0);
		Assert.assertEquals(4.0, row2.getDouble(2), 0.0);
	}

	@Test
	public void testOutputDataFrameDMLVectorWithIDColumn() {
		System.out.println("MLContextTest - output DataFrame DML, vector with ID column");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrameVectorWithIDColumn("M");
		List<Row> list = dataFrame.collectAsList();

		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertArrayEquals(new double[] { 1.0, 2.0 }, ((Vector) row1.get(1)).toArray(), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Assert.assertArrayEquals(new double[] { 3.0, 4.0 }, ((Vector) row2.get(1)).toArray(), 0.0);
	}

	@Test
	public void testOutputDataFramePYDMLVectorWithIDColumn() {
		System.out.println("MLContextTest - output DataFrame PYDML, vector with ID column");

		String s = "M = full('1 2 3 4', rows=2, cols=2)";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrameVectorWithIDColumn("M");
		List<Row> list = dataFrame.collectAsList();

		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertArrayEquals(new double[] { 1.0, 2.0 }, ((Vector) row1.get(1)).toArray(), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Assert.assertArrayEquals(new double[] { 3.0, 4.0 }, ((Vector) row2.get(1)).toArray(), 0.0);
	}

	@Test
	public void testOutputDataFrameDMLVectorNoIDColumn() {
		System.out.println("MLContextTest - output DataFrame DML, vector no ID column");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrameVectorNoIDColumn("M");
		List<Row> list = dataFrame.collectAsList();

		Row row1 = list.get(0);
		Assert.assertArrayEquals(new double[] { 1.0, 2.0 }, ((Vector) row1.get(0)).toArray(), 0.0);

		Row row2 = list.get(1);
		Assert.assertArrayEquals(new double[] { 3.0, 4.0 }, ((Vector) row2.get(0)).toArray(), 0.0);
	}

	@Test
	public void testOutputDataFramePYDMLVectorNoIDColumn() {
		System.out.println("MLContextTest - output DataFrame PYDML, vector no ID column");

		String s = "M = full('1 2 3 4', rows=2, cols=2)";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrameVectorNoIDColumn("M");
		List<Row> list = dataFrame.collectAsList();

		Row row1 = list.get(0);
		Assert.assertArrayEquals(new double[] { 1.0, 2.0 }, ((Vector) row1.get(0)).toArray(), 0.0);

		Row row2 = list.get(1);
		Assert.assertArrayEquals(new double[] { 3.0, 4.0 }, ((Vector) row2.get(0)).toArray(), 0.0);
	}

	@Test
	public void testOutputDataFrameDMLDoublesWithIDColumn() {
		System.out.println("MLContextTest - output DataFrame DML, doubles with ID column");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrameDoubleWithIDColumn("M");
		List<Row> list = dataFrame.collectAsList();

		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertEquals(1.0, row1.getDouble(1), 0.0);
		Assert.assertEquals(2.0, row1.getDouble(2), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Assert.assertEquals(3.0, row2.getDouble(1), 0.0);
		Assert.assertEquals(4.0, row2.getDouble(2), 0.0);
	}

	@Test
	public void testOutputDataFramePYDMLDoublesWithIDColumn() {
		System.out.println("MLContextTest - output DataFrame PYDML, doubles with ID column");

		String s = "M = full('1 2 3 4', rows=2, cols=2)";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrameDoubleWithIDColumn("M");
		List<Row> list = dataFrame.collectAsList();

		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertEquals(1.0, row1.getDouble(1), 0.0);
		Assert.assertEquals(2.0, row1.getDouble(2), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Assert.assertEquals(3.0, row2.getDouble(1), 0.0);
		Assert.assertEquals(4.0, row2.getDouble(2), 0.0);
	}

	@Test
	public void testOutputDataFrameDMLDoublesNoIDColumn() {
		System.out.println("MLContextTest - output DataFrame DML, doubles no ID column");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrameDoubleNoIDColumn("M");
		List<Row> list = dataFrame.collectAsList();

		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertEquals(2.0, row1.getDouble(1), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(3.0, row2.getDouble(0), 0.0);
		Assert.assertEquals(4.0, row2.getDouble(1), 0.0);
	}

	@Test
	public void testOutputDataFramePYDMLDoublesNoIDColumn() {
		System.out.println("MLContextTest - output DataFrame PYDML, doubles no ID column");

		String s = "M = full('1 2 3 4', rows=2, cols=2)";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		Dataset<Row> dataFrame = results.getDataFrameDoubleNoIDColumn("M");
		List<Row> list = dataFrame.collectAsList();

		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertEquals(2.0, row1.getDouble(1), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(3.0, row2.getDouble(0), 0.0);
		Assert.assertEquals(4.0, row2.getDouble(1), 0.0);
	}

	@Test
	public void testTwoScriptsDML() {
		System.out.println("MLContextTest - two scripts with inputs and outputs DML");

		double[][] m1 = new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } };
		String s1 = "sum1 = sum(m1);";
		double sum1 = ml.execute(dml(s1).in("m1", m1).out("sum1")).getDouble("sum1");
		Assert.assertEquals(10.0, sum1, 0.0);

		double[][] m2 = new double[][] { { 5.0, 6.0 }, { 7.0, 8.0 } };
		String s2 = "sum2 = sum(m2);";
		double sum2 = ml.execute(dml(s2).in("m2", m2).out("sum2")).getDouble("sum2");
		Assert.assertEquals(26.0, sum2, 0.0);
	}

	@Test
	public void testTwoScriptsPYDML() {
		System.out.println("MLContextTest - two scripts with inputs and outputs PYDML");

		double[][] m1 = new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } };
		String s1 = "sum1 = sum(m1)";
		double sum1 = ml.execute(pydml(s1).in("m1", m1).out("sum1")).getDouble("sum1");
		Assert.assertEquals(10.0, sum1, 0.0);

		double[][] m2 = new double[][] { { 5.0, 6.0 }, { 7.0, 8.0 } };
		String s2 = "sum2 = sum(m2)";
		double sum2 = ml.execute(pydml(s2).in("m2", m2).out("sum2")).getDouble("sum2");
		Assert.assertEquals(26.0, sum2, 0.0);
	}

	@Test
	public void testOneScriptTwoExecutionsDML() {
		System.out.println("MLContextTest - one script with two executions DML");

		Script script = new Script(org.apache.sysml.api.mlcontext.ScriptType.DML);

		double[][] m1 = new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } };
		script.setScriptString("sum1 = sum(m1);").in("m1", m1).out("sum1");
		ml.execute(script);
		Assert.assertEquals(10.0, script.results().getDouble("sum1"), 0.0);

		script.clearAll();

		double[][] m2 = new double[][] { { 5.0, 6.0 }, { 7.0, 8.0 } };
		script.setScriptString("sum2 = sum(m2);").in("m2", m2).out("sum2");
		ml.execute(script);
		Assert.assertEquals(26.0, script.results().getDouble("sum2"), 0.0);
	}

	@Test
	public void testOneScriptTwoExecutionsPYDML() {
		System.out.println("MLContextTest - one script with two executions PYDML");

		Script script = new Script(org.apache.sysml.api.mlcontext.ScriptType.PYDML);

		double[][] m1 = new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } };
		script.setScriptString("sum1 = sum(m1)").in("m1", m1).out("sum1");
		ml.execute(script);
		Assert.assertEquals(10.0, script.results().getDouble("sum1"), 0.0);

		script.clearAll();

		double[][] m2 = new double[][] { { 5.0, 6.0 }, { 7.0, 8.0 } };
		script.setScriptString("sum2 = sum(m2)").in("m2", m2).out("sum2");
		ml.execute(script);
		Assert.assertEquals(26.0, script.results().getDouble("sum2"), 0.0);
	}

	@Test
	public void testInputParameterBooleanDML() {
		System.out.println("MLContextTest - input parameter boolean DML");

		String s = "x = $X; if (x == TRUE) { print('yes'); }";
		Script script = dml(s).in("$X", true);
		setExpectedStdOut("yes");
		ml.execute(script);
	}

	@Test
	public void testInputParameterBooleanPYDML() {
		System.out.println("MLContextTest - input parameter boolean PYDML");

		String s = "x = $X\nif (x == True):\n  print('yes')";
		Script script = pydml(s).in("$X", true);
		setExpectedStdOut("yes");
		ml.execute(script);
	}

	@Test
	public void testMultipleOutDML() {
		System.out.println("MLContextTest - multiple out DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2); N = sum(M)";
		// alternative to .out("M").out("N")
		MLResults results = ml.execute(dml(s).out("M", "N"));
		double[][] matrix = results.getMatrixAs2DDoubleArray("M");
		double sum = results.getDouble("N");
		Assert.assertEquals(1.0, matrix[0][0], 0);
		Assert.assertEquals(2.0, matrix[0][1], 0);
		Assert.assertEquals(3.0, matrix[1][0], 0);
		Assert.assertEquals(4.0, matrix[1][1], 0);
		Assert.assertEquals(10.0, sum, 0);
	}

	@Test
	public void testMultipleOutPYDML() {
		System.out.println("MLContextTest - multiple out PYDML");

		String s = "M = full('1 2 3 4', rows=2, cols=2)\nN = sum(M)";
		// alternative to .out("M").out("N")
		MLResults results = ml.execute(pydml(s).out("M", "N"));
		double[][] matrix = results.getMatrixAs2DDoubleArray("M");
		double sum = results.getDouble("N");
		Assert.assertEquals(1.0, matrix[0][0], 0);
		Assert.assertEquals(2.0, matrix[0][1], 0);
		Assert.assertEquals(3.0, matrix[1][0], 0);
		Assert.assertEquals(4.0, matrix[1][1], 0);
		Assert.assertEquals(10.0, sum, 0);
	}

	@Test
	public void testOutputMatrixObjectDML() {
		System.out.println("MLContextTest - output matrix object DML");
		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		MatrixObject mo = ml.execute(dml(s).out("M")).getMatrixObject("M");
		RDD<String> rddStringCSV = MLContextConversionUtil.matrixObjectToRDDStringCSV(mo);
		Iterator<String> iterator = rddStringCSV.toLocalIterator();
		Assert.assertEquals("1.0,2.0", iterator.next());
		Assert.assertEquals("3.0,4.0", iterator.next());
	}

	@Test
	public void testOutputMatrixObjectPYDML() {
		System.out.println("MLContextTest - output matrix object PYDML");
		String s = "M = full('1 2 3 4', rows=2, cols=2);";
		MatrixObject mo = ml.execute(pydml(s).out("M")).getMatrixObject("M");
		RDD<String> rddStringCSV = MLContextConversionUtil.matrixObjectToRDDStringCSV(mo);
		Iterator<String> iterator = rddStringCSV.toLocalIterator();
		Assert.assertEquals("1.0,2.0", iterator.next());
		Assert.assertEquals("3.0,4.0", iterator.next());
	}

	@Test
	public void testInputMatrixBlockDML() {
		System.out.println("MLContextTest - input MatrixBlock DML");

		List<String> list = new ArrayList<String>();
		list.add("10,20,30");
		list.add("40,50,60");
		list.add("70,80,90");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Matrix m = new Matrix(dataFrame);
		MatrixBlock matrixBlock = m.toMatrixBlock();
		Script script = dml("avg = avg(M);").in("M", matrixBlock).out("avg");
		double avg = ml.execute(script).getDouble("avg");
		Assert.assertEquals(50.0, avg, 0.0);
	}

	@Test
	public void testInputMatrixBlockPYDML() {
		System.out.println("MLContextTest - input MatrixBlock PYDML");

		List<String> list = new ArrayList<String>();
		list.add("10,20,30");
		list.add("40,50,60");
		list.add("70,80,90");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Matrix m = new Matrix(dataFrame);
		MatrixBlock matrixBlock = m.toMatrixBlock();
		Script script = pydml("avg = avg(M)").in("M", matrixBlock).out("avg");
		double avg = ml.execute(script).getDouble("avg");
		Assert.assertEquals(50.0, avg, 0.0);
	}

	@Test
	public void testOutputBinaryBlocksDML() {
		System.out.println("MLContextTest - output binary blocks DML");
		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		MLResults results = ml.execute(dml(s).out("M"));
		Matrix m = results.getMatrix("M");
		JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlocks = m.toBinaryBlocks();
		MatrixMetadata mm = m.getMatrixMetadata();
		MatrixCharacteristics mc = mm.asMatrixCharacteristics();
		JavaRDD<String> javaRDDStringIJV = RDDConverterUtils.binaryBlockToTextCell(binaryBlocks, mc);

		List<String> lines = javaRDDStringIJV.collect();
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testOutputBinaryBlocksPYDML() {
		System.out.println("MLContextTest - output binary blocks PYDML");
		String s = "M = full('1 2 3 4', rows=2, cols=2);";
		MLResults results = ml.execute(pydml(s).out("M"));
		Matrix m = results.getMatrix("M");
		JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlocks = m.toBinaryBlocks();
		MatrixMetadata mm = m.getMatrixMetadata();
		MatrixCharacteristics mc = mm.asMatrixCharacteristics();
		JavaRDD<String> javaRDDStringIJV = RDDConverterUtils.binaryBlockToTextCell(binaryBlocks, mc);

		List<String> lines = javaRDDStringIJV.collect();
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testOutputListStringCSVDenseDML() {
		System.out.println("MLContextTest - output List String CSV Dense DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2); print(toString(M));";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		MatrixObject mo = results.getMatrixObject("M");
		List<String> lines = MLContextConversionUtil.matrixObjectToListStringCSV(mo);
		Assert.assertEquals("1.0,2.0", lines.get(0));
		Assert.assertEquals("3.0,4.0", lines.get(1));
	}

	@Test
	public void testOutputListStringCSVDensePYDML() {
		System.out.println("MLContextTest - output List String CSV Dense PYDML");

		String s = "M = full('1 2 3 4', rows=2, cols=2)\nprint(toString(M))";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		MatrixObject mo = results.getMatrixObject("M");
		List<String> lines = MLContextConversionUtil.matrixObjectToListStringCSV(mo);
		Assert.assertEquals("1.0,2.0", lines.get(0));
		Assert.assertEquals("3.0,4.0", lines.get(1));
	}

	@Test
	public void testOutputListStringCSVSparseDML() {
		System.out.println("MLContextTest - output List String CSV Sparse DML");

		String s = "M = matrix(0, rows=10, cols=10); M[1,1]=1; M[1,2]=2; M[2,1]=3; M[2,2]=4; print(toString(M));";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		MatrixObject mo = results.getMatrixObject("M");
		List<String> lines = MLContextConversionUtil.matrixObjectToListStringCSV(mo);
		Assert.assertEquals("1.0,2.0", lines.get(0));
		Assert.assertEquals("3.0,4.0", lines.get(1));
	}

	@Test
	public void testOutputListStringCSVSparsePYDML() {
		System.out.println("MLContextTest - output List String CSV Sparse PYDML");

		String s = "M = full(0, rows=10, cols=10)\nM[0,0]=1\nM[0,1]=2\nM[1,0]=3\nM[1,1]=4\nprint(toString(M))";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		MatrixObject mo = results.getMatrixObject("M");
		List<String> lines = MLContextConversionUtil.matrixObjectToListStringCSV(mo);
		Assert.assertEquals("1.0,2.0", lines.get(0));
		Assert.assertEquals("3.0,4.0", lines.get(1));
	}

	@Test
	public void testOutputListStringIJVDenseDML() {
		System.out.println("MLContextTest - output List String IJV Dense DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2); print(toString(M));";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		MatrixObject mo = results.getMatrixObject("M");
		List<String> lines = MLContextConversionUtil.matrixObjectToListStringIJV(mo);
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testOutputListStringIJVDensePYDML() {
		System.out.println("MLContextTest - output List String IJV Dense PYDML");

		String s = "M = full('1 2 3 4', rows=2, cols=2)\nprint(toString(M))";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		MatrixObject mo = results.getMatrixObject("M");
		List<String> lines = MLContextConversionUtil.matrixObjectToListStringIJV(mo);
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testOutputListStringIJVSparseDML() {
		System.out.println("MLContextTest - output List String IJV Sparse DML");

		String s = "M = matrix(0, rows=10, cols=10); M[1,1]=1; M[1,2]=2; M[2,1]=3; M[2,2]=4; print(toString(M));";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		MatrixObject mo = results.getMatrixObject("M");
		List<String> lines = MLContextConversionUtil.matrixObjectToListStringIJV(mo);
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testOutputListStringIJVSparsePYDML() {
		System.out.println("MLContextTest - output List String IJV Sparse PYDML");

		String s = "M = full(0, rows=10, cols=10)\nM[0,0]=1\nM[0,1]=2\nM[1,0]=3\nM[1,1]=4\nprint(toString(M))";
		Script script = pydml(s).out("M");
		MLResults results = ml.execute(script);
		MatrixObject mo = results.getMatrixObject("M");
		List<String> lines = MLContextConversionUtil.matrixObjectToListStringIJV(mo);
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testJavaRDDGoodMetadataDML() {
		System.out.println("MLContextTest - JavaRDD<String> good metadata DML");

		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		MatrixMetadata mm = new MatrixMetadata(3, 3, 9);

		Script script = dml("print('sum: ' + sum(M));").in("M", javaRDD, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testJavaRDDGoodMetadataPYDML() {
		System.out.println("MLContextTest - JavaRDD<String> good metadata PYDML");

		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		MatrixMetadata mm = new MatrixMetadata(3, 3, 9);

		Script script = pydml("print('sum: ' + sum(M))").in("M", javaRDD, mm);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test(expected = MLContextException.class)
	public void testJavaRDDBadMetadataDML() {
		System.out.println("MLContextTest - JavaRDD<String> bad metadata DML");

		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		MatrixMetadata mm = new MatrixMetadata(1, 1, 9);

		Script script = dml("print('sum: ' + sum(M));").in("M", javaRDD, mm);
		ml.execute(script);
	}

	@Test(expected = MLContextException.class)
	public void testJavaRDDBadMetadataPYDML() {
		System.out.println("MLContextTest - JavaRDD<String> bad metadata PYML");

		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> javaRDD = sc.parallelize(list);

		MatrixMetadata mm = new MatrixMetadata(1, 1, 9);

		Script script = dml("print('sum: ' + sum(M))").in("M", javaRDD, mm);
		ml.execute(script);
	}

	@Test
	public void testRDDGoodMetadataDML() {
		System.out.println("MLContextTest - RDD<String> good metadata DML");

		List<String> list = new ArrayList<String>();
		list.add("1,1,1");
		list.add("2,2,2");
		list.add("3,3,3");
		JavaRDD<String> javaRDD = sc.parallelize(list);
		RDD<String> rdd = JavaRDD.toRDD(javaRDD);

		MatrixMetadata mm = new MatrixMetadata(3, 3, 9);

		Script script = dml("print('sum: ' + sum(M));").in("M", rdd, mm);
		setExpectedStdOut("sum: 18.0");
		ml.execute(script);
	}

	@Test
	public void testRDDGoodMetadataPYDML() {
		System.out.println("MLContextTest - RDD<String> good metadata PYDML");

		List<String> list = new ArrayList<String>();
		list.add("1,1,1");
		list.add("2,2,2");
		list.add("3,3,3");
		JavaRDD<String> javaRDD = sc.parallelize(list);
		RDD<String> rdd = JavaRDD.toRDD(javaRDD);

		MatrixMetadata mm = new MatrixMetadata(3, 3, 9);

		Script script = pydml("print('sum: ' + sum(M))").in("M", rdd, mm);
		setExpectedStdOut("sum: 18.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameGoodMetadataDML() {
		System.out.println("MLContextTest - DataFrame good metadata DML");

		List<String> list = new ArrayList<String>();
		list.add("10,20,30");
		list.add("40,50,60");
		list.add("70,80,90");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(3, 3, 9);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 450.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameGoodMetadataPYDML() {
		System.out.println("MLContextTest - DataFrame good metadata PYDML");

		List<String> list = new ArrayList<String>();
		list.add("10,20,30");
		list.add("40,50,60");
		list.add("70,80,90");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		MatrixMetadata mm = new MatrixMetadata(3, 3, 9);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame, mm);
		setExpectedStdOut("sum: 450.0");
		ml.execute(script);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testInputTupleSeqNoMetadataDML() {
		System.out.println("MLContextTest - Tuple sequence no metadata DML");

		List<String> list1 = new ArrayList<String>();
		list1.add("1,2");
		list1.add("3,4");
		JavaRDD<String> javaRDD1 = sc.parallelize(list1);
		RDD<String> rdd1 = JavaRDD.toRDD(javaRDD1);

		List<String> list2 = new ArrayList<String>();
		list2.add("5,6");
		list2.add("7,8");
		JavaRDD<String> javaRDD2 = sc.parallelize(list2);
		RDD<String> rdd2 = JavaRDD.toRDD(javaRDD2);

		Tuple2 tuple1 = new Tuple2("m1", rdd1);
		Tuple2 tuple2 = new Tuple2("m2", rdd2);
		List tupleList = new ArrayList();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		Seq seq = JavaConversions.asScalaBuffer(tupleList).toSeq();

		Script script = dml("print('sums: ' + sum(m1) + ' ' + sum(m2));").in(seq);
		setExpectedStdOut("sums: 10.0 26.0");
		ml.execute(script);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testInputTupleSeqNoMetadataPYDML() {
		System.out.println("MLContextTest - Tuple sequence no metadata PYDML");

		List<String> list1 = new ArrayList<String>();
		list1.add("1,2");
		list1.add("3,4");
		JavaRDD<String> javaRDD1 = sc.parallelize(list1);
		RDD<String> rdd1 = JavaRDD.toRDD(javaRDD1);

		List<String> list2 = new ArrayList<String>();
		list2.add("5,6");
		list2.add("7,8");
		JavaRDD<String> javaRDD2 = sc.parallelize(list2);
		RDD<String> rdd2 = JavaRDD.toRDD(javaRDD2);

		Tuple2 tuple1 = new Tuple2("m1", rdd1);
		Tuple2 tuple2 = new Tuple2("m2", rdd2);
		List tupleList = new ArrayList();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		Seq seq = JavaConversions.asScalaBuffer(tupleList).toSeq();

		Script script = pydml("print('sums: ' + sum(m1) + ' ' + sum(m2))").in(seq);
		setExpectedStdOut("sums: 10.0 26.0");
		ml.execute(script);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testInputTupleSeqWithMetadataDML() {
		System.out.println("MLContextTest - Tuple sequence with metadata DML");

		List<String> list1 = new ArrayList<String>();
		list1.add("1,2");
		list1.add("3,4");
		JavaRDD<String> javaRDD1 = sc.parallelize(list1);
		RDD<String> rdd1 = JavaRDD.toRDD(javaRDD1);

		List<String> list2 = new ArrayList<String>();
		list2.add("5,6");
		list2.add("7,8");
		JavaRDD<String> javaRDD2 = sc.parallelize(list2);
		RDD<String> rdd2 = JavaRDD.toRDD(javaRDD2);

		MatrixMetadata mm1 = new MatrixMetadata(2, 2);
		MatrixMetadata mm2 = new MatrixMetadata(2, 2);

		Tuple3 tuple1 = new Tuple3("m1", rdd1, mm1);
		Tuple3 tuple2 = new Tuple3("m2", rdd2, mm2);
		List tupleList = new ArrayList();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		Seq seq = JavaConversions.asScalaBuffer(tupleList).toSeq();

		Script script = dml("print('sums: ' + sum(m1) + ' ' + sum(m2));").in(seq);
		setExpectedStdOut("sums: 10.0 26.0");
		ml.execute(script);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testInputTupleSeqWithMetadataPYDML() {
		System.out.println("MLContextTest - Tuple sequence with metadata PYDML");

		List<String> list1 = new ArrayList<String>();
		list1.add("1,2");
		list1.add("3,4");
		JavaRDD<String> javaRDD1 = sc.parallelize(list1);
		RDD<String> rdd1 = JavaRDD.toRDD(javaRDD1);

		List<String> list2 = new ArrayList<String>();
		list2.add("5,6");
		list2.add("7,8");
		JavaRDD<String> javaRDD2 = sc.parallelize(list2);
		RDD<String> rdd2 = JavaRDD.toRDD(javaRDD2);

		MatrixMetadata mm1 = new MatrixMetadata(2, 2);
		MatrixMetadata mm2 = new MatrixMetadata(2, 2);

		Tuple3 tuple1 = new Tuple3("m1", rdd1, mm1);
		Tuple3 tuple2 = new Tuple3("m2", rdd2, mm2);
		List tupleList = new ArrayList();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		Seq seq = JavaConversions.asScalaBuffer(tupleList).toSeq();

		Script script = pydml("print('sums: ' + sum(m1) + ' ' + sum(m2))").in(seq);
		setExpectedStdOut("sums: 10.0 26.0");
		ml.execute(script);
	}

	@Test
	public void testCSVMatrixFromURLSumDML() throws MalformedURLException {
		System.out.println("MLContextTest - CSV matrix from URL sum DML");
		String csv = "https://raw.githubusercontent.com/apache/systemml/master/src/test/scripts/org/apache/sysml/api/mlcontext/1234.csv";
		URL url = new URL(csv);
		Script script = dml("print('sum: ' + sum(M));").in("M", url);
		setExpectedStdOut("sum: 10.0");
		ml.execute(script);
	}

	@Test
	public void testCSVMatrixFromURLSumPYDML() throws MalformedURLException {
		System.out.println("MLContextTest - CSV matrix from URL sum PYDML");
		String csv = "https://raw.githubusercontent.com/apache/systemml/master/src/test/scripts/org/apache/sysml/api/mlcontext/1234.csv";
		URL url = new URL(csv);
		Script script = pydml("print('sum: ' + sum(M))").in("M", url);
		setExpectedStdOut("sum: 10.0");
		ml.execute(script);
	}

	@Test
	public void testIJVMatrixFromURLSumDML() throws MalformedURLException {
		System.out.println("MLContextTest - IJV matrix from URL sum DML");
		String ijv = "https://raw.githubusercontent.com/apache/systemml/master/src/test/scripts/org/apache/sysml/api/mlcontext/1234.ijv";
		URL url = new URL(ijv);
		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.IJV, 2, 2);
		Script script = dml("print('sum: ' + sum(M));").in("M", url, mm);
		setExpectedStdOut("sum: 10.0");
		ml.execute(script);
	}

	@Test
	public void testIJVMatrixFromURLSumPYDML() throws MalformedURLException {
		System.out.println("MLContextTest - IJV matrix from URL sum PYDML");
		String ijv = "https://raw.githubusercontent.com/apache/systemml/master/src/test/scripts/org/apache/sysml/api/mlcontext/1234.ijv";
		URL url = new URL(ijv);
		MatrixMetadata mm = new MatrixMetadata(MatrixFormat.IJV, 2, 2);
		Script script = pydml("print('sum: ' + sum(M))").in("M", url, mm);
		setExpectedStdOut("sum: 10.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLDoublesWithNoIDColumnNoFormatSpecified() {
		System.out.println("MLContextTest - DataFrame sum DML, doubles with no ID column, no format specified");

		List<String> list = new ArrayList<String>();
		list.add("2,2,2");
		list.add("3,3,3");
		list.add("4,4,4");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame);
		setExpectedStdOut("sum: 27.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLDoublesWithNoIDColumnNoFormatSpecified() {
		System.out.println("MLContextTest - DataFrame sum PYDML, doubles with no ID column, no format specified");

		List<String> list = new ArrayList<String>();
		list.add("2,2,2");
		list.add("3,3,3");
		list.add("4,4,4");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame);
		setExpectedStdOut("sum: 27.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLDoublesWithIDColumnNoFormatSpecified() {
		System.out.println("MLContextTest - DataFrame sum DML, doubles with ID column, no format specified");

		List<String> list = new ArrayList<String>();
		list.add("1,2,2,2");
		list.add("2,3,3,3");
		list.add("3,4,4,4");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame);
		setExpectedStdOut("sum: 27.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLDoublesWithIDColumnNoFormatSpecified() {
		System.out.println("MLContextTest - DataFrame sum PYDML, doubles with ID column, no format specified");

		List<String> list = new ArrayList<String>();
		list.add("1,2,2,2");
		list.add("2,3,3,3");
		list.add("3,4,4,4");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Script script = pydml("print('sum: ' + sum(M))").in("M", dataFrame);
		setExpectedStdOut("sum: 27.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLVectorWithIDColumnNoFormatSpecified() {
		System.out.println("MLContextTest - DataFrame sum DML, vector with ID column, no format specified");

		List<Tuple2<Double, Vector>> list = new ArrayList<Tuple2<Double, Vector>>();
		list.add(new Tuple2<Double, Vector>(1.0, Vectors.dense(1.0, 2.0, 3.0)));
		list.add(new Tuple2<Double, Vector>(2.0, Vectors.dense(4.0, 5.0, 6.0)));
		list.add(new Tuple2<Double, Vector>(3.0, Vectors.dense(7.0, 8.0, 9.0)));
		JavaRDD<Tuple2<Double, Vector>> javaRddTuple = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddTuple.map(new DoubleVectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLVectorWithIDColumnNoFormatSpecified() {
		System.out.println("MLContextTest - DataFrame sum PYDML, vector with ID column, no format specified");

		List<Tuple2<Double, Vector>> list = new ArrayList<Tuple2<Double, Vector>>();
		list.add(new Tuple2<Double, Vector>(1.0, Vectors.dense(1.0, 2.0, 3.0)));
		list.add(new Tuple2<Double, Vector>(2.0, Vectors.dense(4.0, 5.0, 6.0)));
		list.add(new Tuple2<Double, Vector>(3.0, Vectors.dense(7.0, 8.0, 9.0)));
		JavaRDD<Tuple2<Double, Vector>> javaRddTuple = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddTuple.map(new DoubleVectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Script script = dml("print('sum: ' + sum(M))").in("M", dataFrame);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumDMLVectorWithNoIDColumnNoFormatSpecified() {
		System.out.println("MLContextTest - DataFrame sum DML, vector with no ID column, no format specified");

		List<Vector> list = new ArrayList<Vector>();
		list.add(Vectors.dense(1.0, 2.0, 3.0));
		list.add(Vectors.dense(4.0, 5.0, 6.0));
		list.add(Vectors.dense(7.0, 8.0, 9.0));
		JavaRDD<Vector> javaRddVector = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddVector.map(new VectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Script script = dml("print('sum: ' + sum(M));").in("M", dataFrame);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDataFrameSumPYDMLVectorWithNoIDColumnNoFormatSpecified() {
		System.out.println("MLContextTest - DataFrame sum PYDML, vector with no ID column, no format specified");

		List<Vector> list = new ArrayList<Vector>();
		list.add(Vectors.dense(1.0, 2.0, 3.0));
		list.add(Vectors.dense(4.0, 5.0, 6.0));
		list.add(Vectors.dense(7.0, 8.0, 9.0));
		JavaRDD<Vector> javaRddVector = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddVector.map(new VectorRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", new VectorUDT(), true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		Script script = dml("print('sum: ' + sum(M))").in("M", dataFrame);
		setExpectedStdOut("sum: 45.0");
		ml.execute(script);
	}

	@Test
	public void testDisplayBooleanDML() {
		System.out.println("MLContextTest - display boolean DML");
		String s = "print(b);";
		Script script = dml(s).in("b", true);
		setExpectedStdOut("TRUE");
		ml.execute(script);
	}

	@Test
	public void testDisplayBooleanPYDML() {
		System.out.println("MLContextTest - display boolean PYDML");
		String s = "print(b)";
		Script script = pydml(s).in("b", true);
		setExpectedStdOut("True");
		ml.execute(script);
	}

	@Test
	public void testDisplayBooleanNotDML() {
		System.out.println("MLContextTest - display boolean 'not' DML");
		String s = "print(!b);";
		Script script = dml(s).in("b", true);
		setExpectedStdOut("FALSE");
		ml.execute(script);
	}

	@Test
	public void testDisplayBooleanNotPYDML() {
		System.out.println("MLContextTest - display boolean 'not' PYDML");
		String s = "print(!b)";
		Script script = pydml(s).in("b", true);
		setExpectedStdOut("False");
		ml.execute(script);
	}

	@Test
	public void testDisplayIntegerAddDML() {
		System.out.println("MLContextTest - display integer add DML");
		String s = "print(i+j);";
		Script script = dml(s).in("i", 5).in("j", 6);
		setExpectedStdOut("11");
		ml.execute(script);
	}

	@Test
	public void testDisplayIntegerAddPYDML() {
		System.out.println("MLContextTest - display integer add PYDML");
		String s = "print(i+j)";
		Script script = pydml(s).in("i", 5).in("j", 6);
		setExpectedStdOut("11");
		ml.execute(script);
	}

	@Test
	public void testDisplayStringConcatenationDML() {
		System.out.println("MLContextTest - display string concatenation DML");
		String s = "print(str1+str2);";
		Script script = dml(s).in("str1", "hello").in("str2", "goodbye");
		setExpectedStdOut("hellogoodbye");
		ml.execute(script);
	}

	@Test
	public void testDisplayStringConcatenationPYDML() {
		System.out.println("MLContextTest - display string concatenation PYDML");
		String s = "print(str1+str2)";
		Script script = pydml(s).in("str1", "hello").in("str2", "goodbye");
		setExpectedStdOut("hellogoodbye");
		ml.execute(script);
	}

	@Test
	public void testDisplayDoubleAddDML() {
		System.out.println("MLContextTest - display double add DML");
		String s = "print(i+j);";
		Script script = dml(s).in("i", 5.1).in("j", 6.2);
		setExpectedStdOut("11.3");
		ml.execute(script);
	}

	@Test
	public void testDisplayDoubleAddPYDML() {
		System.out.println("MLContextTest - display double add PYDML");
		String s = "print(i+j)";
		Script script = pydml(s).in("i", 5.1).in("j", 6.2);
		setExpectedStdOut("11.3");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingStringSubstitution() {
		System.out.println("MLContextTest - print formatting string substitution");
		Script script = dml("print('hello %s', 'world');");
		setExpectedStdOut("hello world");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingStringSubstitutions() {
		System.out.println("MLContextTest - print formatting string substitutions");
		Script script = dml("print('%s %s', 'hello', 'world');");
		setExpectedStdOut("hello world");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingStringSubstitutionAlignment() {
		System.out.println("MLContextTest - print formatting string substitution alignment");
		Script script = dml("print(\"'%10s' '%-10s'\", \"hello\", \"world\");");
		setExpectedStdOut("'     hello' 'world     '");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingStringSubstitutionVariables() {
		System.out.println("MLContextTest - print formatting string substitution variables");
		Script script = dml("a='hello'; b='world'; print('%s %s', a, b);");
		setExpectedStdOut("hello world");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingIntegerSubstitution() {
		System.out.println("MLContextTest - print formatting integer substitution");
		Script script = dml("print('int %d', 42);");
		setExpectedStdOut("int 42");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingIntegerSubstitutions() {
		System.out.println("MLContextTest - print formatting integer substitutions");
		Script script = dml("print('%d %d', 42, 43);");
		setExpectedStdOut("42 43");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingIntegerSubstitutionAlignment() {
		System.out.println("MLContextTest - print formatting integer substitution alignment");
		Script script = dml("print(\"'%10d' '%-10d'\", 42, 43);");
		setExpectedStdOut("'        42' '43        '");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingIntegerSubstitutionVariables() {
		System.out.println("MLContextTest - print formatting integer substitution variables");
		Script script = dml("a=42; b=43; print('%d %d', a, b);");
		setExpectedStdOut("42 43");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingDoubleSubstitution() {
		System.out.println("MLContextTest - print formatting double substitution");
		Script script = dml("print('double %f', 42.0);");
		setExpectedStdOut("double 42.000000");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingDoubleSubstitutions() {
		System.out.println("MLContextTest - print formatting double substitutions");
		Script script = dml("print('%f %f', 42.42, 43.43);");
		setExpectedStdOut("42.420000 43.430000");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingDoubleSubstitutionAlignment() {
		System.out.println("MLContextTest - print formatting double substitution alignment");
		Script script = dml("print(\"'%10.2f' '%-10.2f'\", 42.53, 43.54);");
		setExpectedStdOut("'     42.53' '43.54     '");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingDoubleSubstitutionVariables() {
		System.out.println("MLContextTest - print formatting double substitution variables");
		Script script = dml("a=12.34; b=56.78; print('%f %f', a, b);");
		setExpectedStdOut("12.340000 56.780000");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingBooleanSubstitution() {
		System.out.println("MLContextTest - print formatting boolean substitution");
		Script script = dml("print('boolean %b', TRUE);");
		setExpectedStdOut("boolean true");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingBooleanSubstitutions() {
		System.out.println("MLContextTest - print formatting boolean substitutions");
		Script script = dml("print('%b %b', TRUE, FALSE);");
		setExpectedStdOut("true false");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingBooleanSubstitutionAlignment() {
		System.out.println("MLContextTest - print formatting boolean substitution alignment");
		Script script = dml("print(\"'%10b' '%-10b'\", TRUE, FALSE);");
		setExpectedStdOut("'      true' 'false     '");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingBooleanSubstitutionVariables() {
		System.out.println("MLContextTest - print formatting boolean substitution variables");
		Script script = dml("a=TRUE; b=FALSE; print('%b %b', a, b);");
		setExpectedStdOut("true false");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingMultipleTypes() {
		System.out.println("MLContextTest - print formatting multiple types");
		Script script = dml("a='hello'; b=3; c=4.5; d=TRUE; print('%s %d %f %b', a, b, c, d);");
		setExpectedStdOut("hello 3 4.500000 true");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingMultipleExpressions() {
		System.out.println("MLContextTest - print formatting multiple expressions");
		Script script = dml(
				"a='hello'; b='goodbye'; c=4; d=3; e=3.0; f=5.0; g=FALSE; print('%s %d %f %b', (a+b), (c-d), (e*f), !g);");
		setExpectedStdOut("hellogoodbye 1 15.000000 true");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingForLoop() {
		System.out.println("MLContextTest - print formatting for loop");
		Script script = dml("for (i in 1:3) { print('int value %d', i); }");
		// check that one of the lines is returned
		setExpectedStdOut("int value 3");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingParforLoop() {
		System.out.println("MLContextTest - print formatting parfor loop");
		Script script = dml("parfor (i in 1:3) { print('int value %d', i); }");
		// check that one of the lines is returned
		setExpectedStdOut("int value 3");
		ml.execute(script);
	}

	@Test
	public void testPrintFormattingForLoopMultiply() {
		System.out.println("MLContextTest - print formatting for loop multiply");
		Script script = dml("a = 5.0; for (i in 1:3) { print('%d %f', i, a * i); }");
		// check that one of the lines is returned
		setExpectedStdOut("3 15.000000");
		ml.execute(script);
	}

	@Test
	public void testInputVariablesAddLongsDML() {
		System.out.println("MLContextTest - input variables add longs DML");

		String s = "print('x + y = ' + (x + y));";
		Script script = dml(s).in("x", 3L).in("y", 4L);
		setExpectedStdOut("x + y = 7");
		ml.execute(script);
	}

	@Test
	public void testInputVariablesAddLongsPYDML() {
		System.out.println("MLContextTest - input variables add longs PYDML");

		String s = "print('x + y = ' + (x + y))";
		Script script = pydml(s).in("x", 3L).in("y", 4L);
		setExpectedStdOut("x + y = 7");
		ml.execute(script);
	}

	@Test
	public void testInputVariablesAddFloatsDML() {
		System.out.println("MLContextTest - input variables add floats DML");

		String s = "print('x + y = ' + (x + y));";
		Script script = dml(s).in("x", 3F).in("y", 4F);
		setExpectedStdOut("x + y = 7.0");
		ml.execute(script);
	}

	@Test
	public void testInputVariablesAddFloatsPYDML() {
		System.out.println("MLContextTest - input variables add floats PYDML");

		String s = "print('x + y = ' + (x + y))";
		Script script = pydml(s).in("x", 3F).in("y", 4F);
		setExpectedStdOut("x + y = 7.0");
		ml.execute(script);
	}

	@Test
	public void testFunctionNoReturnValueDML() {
		System.out.println("MLContextTest - function with no return value DML");

		String s = "hello=function(){print('no return value')}\nhello();";
		Script script = dml(s);
		setExpectedStdOut("no return value");
		ml.execute(script);
	}

	@Test
	public void testFunctionNoReturnValuePYDML() {
		System.out.println("MLContextTest - function with no return value PYDML");

		String s = "def hello():\n\tprint('no return value')\nhello()";
		Script script = pydml(s);
		setExpectedStdOut("no return value");
		ml.execute(script);
	}

	@Test
	public void testFunctionNoReturnValueForceFunctionCallDML() {
		System.out.println("MLContextTest - function with no return value, force function call DML");

		String s = "hello=function(){\nwhile(FALSE){};\nprint('no return value, force function call');\n}\nhello();";
		Script script = dml(s);
		setExpectedStdOut("no return value, force function call");
		ml.execute(script);
	}

	@Test
	public void testFunctionNoReturnValueForceFunctionCallPYDML() {
		System.out.println("MLContextTest - function with no return value, force function call PYDML");

		String s = "def hello():\n\tif (1==1):\n\t\tprint('')\n\tprint('no return value, force function call')\nhello()";
		Script script = pydml(s);
		setExpectedStdOut("no return value, force function call");
		ml.execute(script);
	}

	@Test
	public void testFunctionReturnValueDML() {
		System.out.println("MLContextTest - function with return value DML");

		String s = "hello=function()return(string s){s='return value'}\na=hello();\nprint(a);";
		Script script = dml(s);
		setExpectedStdOut("return value");
		ml.execute(script);
	}

	@Test
	public void testFunctionReturnValuePYDML() {
		System.out.println("MLContextTest - function with return value PYDML");

		String s = "def hello()->(s:str):\n\ts='return value'\na=hello()\nprint(a)";
		Script script = pydml(s);
		setExpectedStdOut("return value");
		ml.execute(script);
	}

	@Test
	public void testFunctionTwoReturnValuesDML() {
		System.out.println("MLContextTest - function with two return values DML");

		String s = "hello=function()return(string s1,string s2){s1='return'; s2='values'}\n[a,b]=hello();\nprint(a+' '+b);";
		Script script = dml(s);
		setExpectedStdOut("return values");
		ml.execute(script);
	}

	@Test
	public void testFunctionTwoReturnValuesPYDML() {
		System.out.println("MLContextTest - function with two return values PYDML");

		String s = "def hello()->(s1:str,s2:str):\n\ts1='return'\n\ts2='values'\n[a,b]=hello()\nprint(a+' '+b)";
		Script script = pydml(s);
		setExpectedStdOut("return values");
		ml.execute(script);
	}

	@Test
	public void testOutputListDML() {
		System.out.println("MLContextTest - output specified as List DML");

		List<String> outputs = Arrays.asList("x", "y");
		Script script = dml("a=1;x=a+1;y=x+1").out(outputs);
		MLResults results = ml.execute(script);
		Assert.assertEquals(2, results.getLong("x"));
		Assert.assertEquals(3, results.getLong("y"));
	}

	@Test
	public void testOutputListPYDML() {
		System.out.println("MLContextTest - output specified as List PYDML");

		List<String> outputs = Arrays.asList("x", "y");
		Script script = pydml("a=1\nx=a+1\ny=x+1").out(outputs);
		MLResults results = ml.execute(script);
		Assert.assertEquals(2, results.getLong("x"));
		Assert.assertEquals(3, results.getLong("y"));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testOutputScalaSeqDML() {
		System.out.println("MLContextTest - output specified as Scala Seq DML");

		List outputs = Arrays.asList("x", "y");
		Seq seq = JavaConversions.asScalaBuffer(outputs).toSeq();
		Script script = dml("a=1;x=a+1;y=x+1").out(seq);
		MLResults results = ml.execute(script);
		Assert.assertEquals(2, results.getLong("x"));
		Assert.assertEquals(3, results.getLong("y"));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testOutputScalaSeqPYDML() {
		System.out.println("MLContextTest - output specified as Scala Seq PYDML");

		List outputs = Arrays.asList("x", "y");
		Seq seq = JavaConversions.asScalaBuffer(outputs).toSeq();
		Script script = pydml("a=1\nx=a+1\ny=x+1").out(seq);
		MLResults results = ml.execute(script);
		Assert.assertEquals(2, results.getLong("x"));
		Assert.assertEquals(3, results.getLong("y"));
	}

	@Test
	public void testOutputDataFrameOfVectorsDML() {
		System.out.println("MLContextTest - output DataFrame of vectors DML");

		String s = "m=matrix('1 2 3 4',rows=2,cols=2);";
		Script script = dml(s).out("m");
		MLResults results = ml.execute(script);
		Dataset<Row> df = results.getDataFrame("m", true);
		Dataset<Row> sortedDF = df.sort(RDDConverterUtils.DF_ID_COLUMN);

		// verify column types
		StructType schema = sortedDF.schema();
		StructField[] fields = schema.fields();
		StructField idColumn = fields[0];
		StructField vectorColumn = fields[1];
		Assert.assertTrue(idColumn.dataType() instanceof DoubleType);
		Assert.assertTrue(vectorColumn.dataType() instanceof VectorUDT);

		List<Row> list = sortedDF.collectAsList();

		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Vector v1 = (DenseVector) row1.get(1);
		double[] arr1 = v1.toArray();
		Assert.assertArrayEquals(new double[] { 1.0, 2.0 }, arr1, 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Vector v2 = (DenseVector) row2.get(1);
		double[] arr2 = v2.toArray();
		Assert.assertArrayEquals(new double[] { 3.0, 4.0 }, arr2, 0.0);
	}

	@Test
	public void testOutputDoubleArrayFromMatrixDML() {
		System.out.println("MLContextTest - output double array from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		double[][] matrix = ml.execute(dml(s).out("M")).getMatrix("M").to2DDoubleArray();
		Assert.assertEquals(1.0, matrix[0][0], 0);
		Assert.assertEquals(2.0, matrix[0][1], 0);
		Assert.assertEquals(3.0, matrix[1][0], 0);
		Assert.assertEquals(4.0, matrix[1][1], 0);
	}

	@Test
	public void testOutputDataFrameFromMatrixDML() {
		System.out.println("MLContextTest - output DataFrame from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		Dataset<Row> df = ml.execute(script).getMatrix("M").toDF();
		Dataset<Row> sortedDF = df.sort(RDDConverterUtils.DF_ID_COLUMN);
		List<Row> list = sortedDF.collectAsList();
		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertEquals(1.0, row1.getDouble(1), 0.0);
		Assert.assertEquals(2.0, row1.getDouble(2), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Assert.assertEquals(3.0, row2.getDouble(1), 0.0);
		Assert.assertEquals(4.0, row2.getDouble(2), 0.0);
	}

	@Test
	public void testOutputDataFrameDoublesNoIDColumnFromMatrixDML() {
		System.out.println("MLContextTest - output DataFrame of doubles with no ID column from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=1, cols=4);";
		Script script = dml(s).out("M");
		Dataset<Row> df = ml.execute(script).getMatrix("M").toDFDoubleNoIDColumn();
		List<Row> list = df.collectAsList();

		Row row = list.get(0);
		Assert.assertEquals(1.0, row.getDouble(0), 0.0);
		Assert.assertEquals(2.0, row.getDouble(1), 0.0);
		Assert.assertEquals(3.0, row.getDouble(2), 0.0);
		Assert.assertEquals(4.0, row.getDouble(3), 0.0);
	}

	@Test
	public void testOutputDataFrameDoublesWithIDColumnFromMatrixDML() {
		System.out.println("MLContextTest - output DataFrame of doubles with ID column from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		Dataset<Row> df = ml.execute(script).getMatrix("M").toDFDoubleWithIDColumn();
		Dataset<Row> sortedDF = df.sort(RDDConverterUtils.DF_ID_COLUMN);
		List<Row> list = sortedDF.collectAsList();

		Row row1 = list.get(0);
		Assert.assertEquals(1.0, row1.getDouble(0), 0.0);
		Assert.assertEquals(1.0, row1.getDouble(1), 0.0);
		Assert.assertEquals(2.0, row1.getDouble(2), 0.0);

		Row row2 = list.get(1);
		Assert.assertEquals(2.0, row2.getDouble(0), 0.0);
		Assert.assertEquals(3.0, row2.getDouble(1), 0.0);
		Assert.assertEquals(4.0, row2.getDouble(2), 0.0);
	}

	@Test
	public void testOutputDataFrameVectorsNoIDColumnFromMatrixDML() {
		System.out.println("MLContextTest - output DataFrame of vectors with no ID column from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=1, cols=4);";
		Script script = dml(s).out("M");
		Dataset<Row> df = ml.execute(script).getMatrix("M").toDFVectorNoIDColumn();
		List<Row> list = df.collectAsList();

		Row row = list.get(0);
		Assert.assertArrayEquals(new double[] { 1.0, 2.0, 3.0, 4.0 }, ((Vector) row.get(0)).toArray(), 0.0);
	}

	@Test
	public void testOutputDataFrameVectorsWithIDColumnFromMatrixDML() {
		System.out.println("MLContextTest - output DataFrame of vectors with ID column from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=1, cols=4);";
		Script script = dml(s).out("M");
		Dataset<Row> df = ml.execute(script).getMatrix("M").toDFVectorWithIDColumn();
		List<Row> list = df.collectAsList();

		Row row = list.get(0);
		Assert.assertEquals(1.0, row.getDouble(0), 0.0);
		Assert.assertArrayEquals(new double[] { 1.0, 2.0, 3.0, 4.0 }, ((Vector) row.get(1)).toArray(), 0.0);
	}

	@Test
	public void testOutputJavaRDDStringCSVFromMatrixDML() {
		System.out.println("MLContextTest - output Java RDD String CSV from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=1, cols=4);";
		Script script = dml(s).out("M");
		JavaRDD<String> javaRDDStringCSV = ml.execute(script).getMatrix("M").toJavaRDDStringCSV();
		List<String> lines = javaRDDStringCSV.collect();
		Assert.assertEquals("1.0,2.0,3.0,4.0", lines.get(0));
	}

	@Test
	public void testOutputJavaRDDStringIJVFromMatrixDML() {
		System.out.println("MLContextTest - output Java RDD String IJV from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		MLResults results = ml.execute(script);
		JavaRDD<String> javaRDDStringIJV = results.getJavaRDDStringIJV("M");
		List<String> lines = javaRDDStringIJV.sortBy(row -> row, true, 1).collect();
		Assert.assertEquals("1 1 1.0", lines.get(0));
		Assert.assertEquals("1 2 2.0", lines.get(1));
		Assert.assertEquals("2 1 3.0", lines.get(2));
		Assert.assertEquals("2 2 4.0", lines.get(3));
	}

	@Test
	public void testOutputRDDStringCSVFromMatrixDML() {
		System.out.println("MLContextTest - output RDD String CSV from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=1, cols=4);";
		Script script = dml(s).out("M");
		RDD<String> rddStringCSV = ml.execute(script).getMatrix("M").toRDDStringCSV();
		Iterator<String> iterator = rddStringCSV.toLocalIterator();
		Assert.assertEquals("1.0,2.0,3.0,4.0", iterator.next());
	}

	@Test
	public void testOutputRDDStringIJVFromMatrixDML() {
		System.out.println("MLContextTest - output RDD String IJV from matrix DML");

		String s = "M = matrix('1 2 3 4', rows=2, cols=2);";
		Script script = dml(s).out("M");
		RDD<String> rddStringIJV = ml.execute(script).getMatrix("M").toRDDStringIJV();
		String[] rows = (String[]) rddStringIJV.collect();
		Arrays.sort(rows);
		Assert.assertEquals("1 1 1.0", rows[0]);
		Assert.assertEquals("1 2 2.0", rows[1]);
		Assert.assertEquals("2 1 3.0", rows[2]);
		Assert.assertEquals("2 2 4.0", rows[3]);
	}

	@Test
	public void testMLContextVersionMessage() {
		System.out.println("MLContextTest - version message");

		String version = ml.version();
		// not available until jar built
		Assert.assertEquals(MLContextUtil.VERSION_NOT_AVAILABLE, version);
	}

	@Test
	public void testMLContextBuildTimeMessage() {
		System.out.println("MLContextTest - build time message");

		String buildTime = ml.buildTime();
		// not available until jar built
		Assert.assertEquals(MLContextUtil.BUILD_TIME_NOT_AVAILABLE, buildTime);
	}

	@Test
	public void testMLContextCreateAndClose() {
		// MLContext created by the @BeforeClass method in MLContextTestBase
		// MLContext closed by the @AfterClass method in MLContextTestBase
		System.out.println("MLContextTest - create MLContext and close (without script execution)");
	}

	@Test
	public void testDataFrameToBinaryBlocks() {
		System.out.println("MLContextTest - DataFrame to binary blocks");

		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> javaRddString = sc.parallelize(list);

		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = spark.createDataFrame(javaRddRow, schema);

		JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlocks = MLContextConversionUtil
				.dataFrameToMatrixBinaryBlocks(dataFrame);
		Tuple2<MatrixIndexes, MatrixBlock> first = binaryBlocks.first();
		MatrixBlock mb = first._2();
		double[][] matrix = DataConverter.convertToDoubleMatrix(mb);
		Assert.assertArrayEquals(new double[] { 1.0, 2.0, 3.0 }, matrix[0], 0.0);
		Assert.assertArrayEquals(new double[] { 4.0, 5.0, 6.0 }, matrix[1], 0.0);
		Assert.assertArrayEquals(new double[] { 7.0, 8.0, 9.0 }, matrix[2], 0.0);
	}

	@Test
	public void testGetTuple1DML() {
		System.out.println("MLContextTest - Get Tuple1<Matrix> DML");
		JavaRDD<String> javaRddString = sc
				.parallelize(Stream.of("1,2,3", "4,5,6", "7,8,9").collect(Collectors.toList()));
		JavaRDD<Row> javaRddRow = javaRddString.map(new CommaSeparatedValueStringToDoubleArrayRow());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.DoubleType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> df = spark.createDataFrame(javaRddRow, schema);

		Script script = dml("N=M*2").in("M", df).out("N");
		Tuple1<Matrix> tuple = ml.execute(script).getTuple("N");
		double[][] n = tuple._1().to2DDoubleArray();
		Assert.assertEquals(2.0, n[0][0], 0);
		Assert.assertEquals(4.0, n[0][1], 0);
		Assert.assertEquals(6.0, n[0][2], 0);
		Assert.assertEquals(8.0, n[1][0], 0);
		Assert.assertEquals(10.0, n[1][1], 0);
		Assert.assertEquals(12.0, n[1][2], 0);
		Assert.assertEquals(14.0, n[2][0], 0);
		Assert.assertEquals(16.0, n[2][1], 0);
		Assert.assertEquals(18.0, n[2][2], 0);
	}

	@Test
	public void testGetTuple2DML() {
		System.out.println("MLContextTest - Get Tuple2<Matrix,Double> DML");

		double[][] m = new double[][] { { 1, 2 }, { 3, 4 } };

		Script script = dml("N=M*2;s=sum(N)").in("M", m).out("N", "s");
		Tuple2<Matrix, Double> tuple = ml.execute(script).getTuple("N", "s");
		double[][] n = tuple._1().to2DDoubleArray();
		double s = tuple._2();
		Assert.assertArrayEquals(new double[] { 2, 4 }, n[0], 0.0);
		Assert.assertArrayEquals(new double[] { 6, 8 }, n[1], 0.0);
		Assert.assertEquals(20.0, s, 0.0);
	}

	@Test
	public void testGetTuple3DML() {
		System.out.println("MLContextTest - Get Tuple3<Long,Double,Boolean> DML");

		Script script = dml("a=1+2;b=a+0.5;c=TRUE;").out("a", "b", "c");
		Tuple3<Long, Double, Boolean> tuple = ml.execute(script).getTuple("a", "b", "c");
		long a = tuple._1();
		double b = tuple._2();
		boolean c = tuple._3();
		Assert.assertEquals(3, a);
		Assert.assertEquals(3.5, b, 0.0);
		Assert.assertEquals(true, c);
	}

	@Test
	public void testGetTuple4DML() {
		System.out.println("MLContextTest - Get Tuple4<Long,Double,Boolean,String> DML");

		Script script = dml("a=1+2;b=a+0.5;c=TRUE;d=\"yes it's \"+c").out("a", "b", "c", "d");
		Tuple4<Long, Double, Boolean, String> tuple = ml.execute(script).getTuple("a", "b", "c", "d");
		long a = tuple._1();
		double b = tuple._2();
		boolean c = tuple._3();
		String d = tuple._4();
		Assert.assertEquals(3, a);
		Assert.assertEquals(3.5, b, 0.0);
		Assert.assertEquals(true, c);
		Assert.assertEquals("yes it's TRUE", d);
	}

}
