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

package org.apache.sysml.test.integration.functions.mlcontext;

import static org.apache.sysml.api.mlcontext.ScriptFactory.dml;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.sysml.api.mlcontext.FrameFormat;
import org.apache.sysml.api.mlcontext.FrameMetadata;
import org.apache.sysml.api.mlcontext.MLContext;
import org.apache.sysml.api.linalg.Matrix;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.util.DataConverter;
import org.apache.sysml.runtime.util.UtilFunctions;
import org.apache.sysml.test.integration.AutomatedTestBase;
import org.apache.sysml.test.integration.TestConfiguration;
import org.apache.sysml.test.utils.TestUtils;
import org.junit.Test;


public class DataFrameVectorScriptTest extends AutomatedTestBase 
{
	private final static String TEST_DIR = "functions/mlcontext/";
	private final static String TEST_NAME = "DataFrameConversion";
	private final static String TEST_CLASS_DIR = TEST_DIR + DataFrameVectorScriptTest.class.getSimpleName() + "/";

	//schema restriction: single vector included
	private final static ValueType[] schemaStrings = new ValueType[]{ValueType.OBJECT, ValueType.STRING, ValueType.STRING, ValueType.STRING};
	private final static ValueType[] schemaDoubles = new ValueType[]{ValueType.DOUBLE, ValueType.DOUBLE, ValueType.OBJECT, ValueType.DOUBLE};
	private final static ValueType[] schemaMixed1 = new ValueType[]{ValueType.OBJECT, ValueType.INT, ValueType.STRING, ValueType.DOUBLE, ValueType.INT};
	private final static ValueType[] schemaMixed2 = new ValueType[]{ValueType.STRING, ValueType.OBJECT, ValueType.DOUBLE};
	
	private final static int rows1 = 2245;
	private final static int colsVector = 7;
	private final static double sparsity1 = 0.9;
	private final static double sparsity2 = 0.1;
	private final static double eps=0.0000000001;

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"A", "B"}));
	}

	@Test
	public void testVectorStringsConversionIDDenseUnknown() {
		testDataFrameScriptInput(schemaStrings, true, false, true);
	}
	
	@Test
	public void testVectorDoublesConversionIDDenseUnknown() {
		testDataFrameScriptInput(schemaDoubles, true, false, true);
	}
	
	@Test
	public void testVectorMixed1ConversionIDDenseUnknown() {
		testDataFrameScriptInput(schemaMixed1, true, false, true);
	}
	
	@Test
	public void testVectorMixed2ConversionIDDenseUnknown() {
		testDataFrameScriptInput(schemaMixed2, true, false, true);
	}
	
	@Test
	public void testVectorStringsConversionIDDense() {
		testDataFrameScriptInput(schemaStrings, true, false, false);
	}
	
	@Test
	public void testVectorDoublesConversionIDDense() {
		testDataFrameScriptInput(schemaDoubles, true, false, false);
	}
	
	@Test
	public void testVectorMixed1ConversionIDDense() {
		testDataFrameScriptInput(schemaMixed1, true, false, false);
	}
	
	@Test
	public void testVectorMixed2ConversionIDDense() {
		testDataFrameScriptInput(schemaMixed2, true, false, false);
	}

	@Test
	public void testVectorStringsConversionIDSparseUnknown() {
		testDataFrameScriptInput(schemaStrings, true, true, true);
	}
	
	@Test
	public void testVectorDoublesConversionIDSparseUnknown() {
		testDataFrameScriptInput(schemaDoubles, true, true, true);
	}
	
	@Test
	public void testVectorMixed1ConversionIDSparseUnknown() {
		testDataFrameScriptInput(schemaMixed1, true, true, true);
	}
	
	@Test
	public void testVectorMixed2ConversionIDSparseUnknown() {
		testDataFrameScriptInput(schemaMixed2, true, true, true);
	}
	
	@Test
	public void testVectorStringsConversionIDSparse() {
		testDataFrameScriptInput(schemaStrings, true, true, false);
	}
	
	@Test
	public void testVectorDoublesConversionIDSparse() {
		testDataFrameScriptInput(schemaDoubles, true, true, false);
	}
	
	@Test
	public void testVectorMixed1ConversionIDSparse() {
		testDataFrameScriptInput(schemaMixed1, true, true, false);
	}
	
	@Test
	public void testVectorMixed2ConversionIDSparse() {
		testDataFrameScriptInput(schemaMixed2, true, true, false);
	}

	@Test
	public void testVectorStringsConversionDenseUnknown() {
		testDataFrameScriptInput(schemaStrings, false, false, true);
	}
	
	@Test
	public void testVectorDoublesConversionDenseUnknown() {
		testDataFrameScriptInput(schemaDoubles, false, false, true);
	}
	
	@Test
	public void testVectorMixed1ConversionDenseUnknown() {
		testDataFrameScriptInput(schemaMixed1, false, false, true);
	}
	
	@Test
	public void testVectorMixed2ConversionDenseUnknown() {
		testDataFrameScriptInput(schemaMixed2, false, false, true);
	}
	
	@Test
	public void testVectorStringsConversionDense() {
		testDataFrameScriptInput(schemaStrings, false, false, false);
	}
	
	@Test
	public void testVectorDoublesConversionDense() {
		testDataFrameScriptInput(schemaDoubles, false, false, false);
	}
	
	@Test
	public void testVectorMixed1ConversionDense() {
		testDataFrameScriptInput(schemaMixed1, false, false, false);
	}
	
	@Test
	public void testVectorMixed2ConversionDense() {
		testDataFrameScriptInput(schemaMixed2, false, false, false);
	}

	@Test
	public void testVectorStringsConversionSparseUnknown() {
		testDataFrameScriptInput(schemaStrings, false, true, true);
	}
	
	@Test
	public void testVectorDoublesConversionSparseUnknown() {
		testDataFrameScriptInput(schemaDoubles, false, true, true);
	}
	
	@Test
	public void testVectorMixed1ConversionSparseUnknown() {
		testDataFrameScriptInput(schemaMixed1, false, true, true);
	}
	
	@Test
	public void testVectorMixed2ConversionSparseUnknown() {
		testDataFrameScriptInput(schemaMixed2, false, true, true);
	}
	
	@Test
	public void testVectorStringsConversionSparse() {
		testDataFrameScriptInput(schemaStrings, false, true, false);
	}
	
	@Test
	public void testVectorDoublesConversionSparse() {
		testDataFrameScriptInput(schemaDoubles, false, true, false);
	}
	
	@Test
	public void testVectorMixed1ConversionSparse() {
		testDataFrameScriptInput(schemaMixed1, false, true, false);
	}
	
	@Test
	public void testVectorMixed2ConversionSparse() {
		testDataFrameScriptInput(schemaMixed2, false, true, false);
	}
	
	/**
	 * 
	 * @param schema
	 * @param containsID
	 * @param dense
	 * @param unknownDims
	 */
	private void testDataFrameScriptInput(ValueType[] schema, boolean containsID, boolean dense, boolean unknownDims) {
		
		//TODO fix inconsistency ml context vs jmlc register Xf
		
		JavaSparkContext sc = null;
		MLContext ml = null;
		
		try
		{
			//generate input data and setup metadata
			int cols = schema.length + colsVector - 1;
			double sparsity = dense ? sparsity1 : sparsity2; 
			double[][] A = TestUtils.round(getRandomMatrix(rows1, cols, -10, 1000, sparsity, 2373)); 
			MatrixBlock mbA = DataConverter.convertToMatrixBlock(A);
			int blksz = ConfigurationManager.getBlocksize();
			MatrixCharacteristics mc1 = new MatrixCharacteristics(rows1, cols, blksz, blksz, mbA.getNonZeros());
			MatrixCharacteristics mc2 = unknownDims ? new MatrixCharacteristics() : new MatrixCharacteristics(mc1);
			
			//setup spark context
			SparkConf conf = SparkExecutionContext.createSystemMLSparkConf()
					.setAppName("MLContextFrameTest").setMaster("local");
			sc = new JavaSparkContext(conf);
			SparkSession sparkSession = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
			
			//create input data frame
			Dataset<Row> df = createDataFrame(sparkSession, mbA, containsID, schema);

			// Create full frame metadata, and empty frame metadata
			FrameMetadata meta = new FrameMetadata(containsID ? FrameFormat.DF_WITH_INDEX :
				FrameFormat.DF, mc2.getRows(), mc2.getCols());
			FrameMetadata metaEmpty = new FrameMetadata();

			//create mlcontext
			ml = new MLContext(sc);
			ml.setExplain(true);
			
			//run scripts and obtain result
			Script script1 = dml(
					"Xm = as.matrix(Xf);")
				.in("Xf", df, meta).out("Xm");
			Script script2 = dml(
					"Xm = as.matrix(Xf);")
					.in("Xf", df, metaEmpty).out("Xm");  // empty metadata
			Matrix Xm1 = ml.execute(script1).getMatrix("Xm");
			Matrix Xm2 = ml.execute(script2).getMatrix("Xm");
			MatrixBlock mbB1 = Xm1.toBinaryBlockMatrix().getMatrixBlock();
			MatrixBlock mbB2 = Xm2.toBinaryBlockMatrix().getMatrixBlock();

			//compare frame blocks
			double[][] B1 = DataConverter.convertToDoubleMatrix(mbB1);
			double[][] B2 = DataConverter.convertToDoubleMatrix(mbB2);
			TestUtils.compareMatrices(A, B1, rows1, cols, eps);
			TestUtils.compareMatrices(A, B2, rows1, cols, eps);
		}
		catch( Exception ex ) {
			ex.printStackTrace();
			throw new RuntimeException(ex);
		}
		finally {
			// stop spark context to allow single jvm tests (otherwise the
			// next test that tries to create a SparkContext would fail)
			if( sc != null )
				sc.stop();
			// clear status mlcontext and spark exec context
			if( ml != null )
				ml.close();
		}
	}

	@SuppressWarnings("resource")
	private Dataset<Row> createDataFrame(SparkSession sparkSession, MatrixBlock mb, boolean containsID, ValueType[] schema) 
		throws DMLRuntimeException
	{
		//create in-memory list of rows
		List<Row> list = new ArrayList<Row>();		 
		int off = (containsID ? 1 : 0);
		int clen = mb.getNumColumns() + off - colsVector + 1;
		
		for( int i=0; i<mb.getNumRows(); i++ ) {
			Object[] row = new Object[clen];
			if( containsID )
				row[0] = (double)i+1;
			for( int j=0, j2=0; j<mb.getNumColumns(); j++, j2++ ) {
				if( schema[j2] != ValueType.OBJECT ) {
					row[j2+off] = UtilFunctions
						.doubleToObject(schema[j2], mb.quickGetValue(i, j));
				}
				else {
					double[] tmp = DataConverter.convertToDoubleVector(
							mb.sliceOperations(i, i, j, j+colsVector-1, new MatrixBlock()));
					row[j2+off] = new DenseVector(tmp);
					j += colsVector-1;
				}
			}
			list.add(RowFactory.create(row));
		}
		
		//create data frame schema
		List<StructField> fields = new ArrayList<StructField>();
		if( containsID )
			fields.add(DataTypes.createStructField(RDDConverterUtils.DF_ID_COLUMN, 
					DataTypes.DoubleType, true));
		for( int j=0; j<schema.length; j++ ) {
			DataType dt = null;
			switch(schema[j]) {
				case STRING: dt = DataTypes.StringType; break;
				case DOUBLE: dt = DataTypes.DoubleType; break;
				case INT:    dt = DataTypes.LongType; break;
				case OBJECT: dt = new VectorUDT(); break;
				default: throw new RuntimeException("Unsupported value type.");
			}
			fields.add(DataTypes.createStructField("C"+(j+1), dt, true));
		}
		StructType dfSchema = DataTypes.createStructType(fields);
				
		//create rdd and data frame
		JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
		JavaRDD<Row> rowRDD = sc.parallelize(list);
		return sparkSession.createDataFrame(rowRDD, dfSchema);
	}
}
