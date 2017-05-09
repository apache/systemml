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

package org.apache.sysml.runtime.instructions.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.sysml.hops.AggBinaryOp.SparkAggType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.utils.NativeHelper;

/**
 * Cpmm: cross-product matrix multiplication operation (distributed matrix multiply
 * by join over common dimension and subsequent aggregation of partial results).
 * 
 * NOTE: There is additional optimization potential by preventing aggregation for a single
 * block on the common dimension. However, in such a case we would never pick cpmm because
 * this would result in a degree of parallelism of 1.	
 * 
 */
public class CpmmSPInstruction extends BinarySPInstruction 
{
	
	private SparkAggType _aggtype;
	
	public CpmmSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, SparkAggType aggtype, String opcode, String istr )
	{
		super(op, in1, in2, out, opcode, istr);
		_sptype = SPINSTRUCTION_TYPE.CPMM;
		_aggtype = aggtype;
	}

	public static CpmmSPInstruction parseInstruction( String str ) 
		throws DMLRuntimeException 
	{
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if ( opcode.equalsIgnoreCase("cpmm")) {
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand out = new CPOperand(parts[3]);
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			SparkAggType aggtype = SparkAggType.valueOf(parts[4]);
			
			return new CpmmSPInstruction(aggbin, in1, in2, out, aggtype, opcode, str);
		} 
		else {
			throw new DMLRuntimeException("AggregateBinaryInstruction.parseInstruction():: Unknown opcode " + opcode);
		}
		
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{	
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		
		//get rdd inputs
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable( input1.getName() );
		JavaPairRDD<MatrixIndexes,MatrixBlock> in2 = sec.getBinaryBlockRDDHandleForVariable( input2.getName() );
		
		//process core cpmm matrix multiply 
		JavaPairRDD<Long, IndexedMatrixValue> tmp1 = in1.mapToPair(new CpmmIndexFunction(true));
		JavaPairRDD<Long, IndexedMatrixValue> tmp2 = in2.mapToPair(new CpmmIndexFunction(false));
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = tmp1
				   .join(tmp2)                              // join over common dimension
				   .mapToPair(new CpmmMultiplyFunction(NativeHelper.isNativeLibraryLoaded()));  // compute block multiplications
				   
		//process cpmm aggregation and handle outputs				
		if( _aggtype == SparkAggType.SINGLE_BLOCK )
		{
			MatrixBlock out2 = RDDAggregateUtils.sumStable(out);
			
			//put output block into symbol table (no lineage because single block)
			//this also includes implicit maintenance of matrix characteristics
			sec.setMatrixOutput(output.getName(), out2);	
		}
		else //DEFAULT: MULTI_BLOCK
		{
			out = RDDAggregateUtils.sumByKeyStable(out, false); 
			
			//put output RDD handle into symbol table
			sec.setRDDHandleForVariable(output.getName(), out);
			sec.addLineageRDD(output.getName(), input1.getName());
			sec.addLineageRDD(output.getName(), input2.getName());		
			
			//update output statistics if not inferred
			updateBinaryMMOutputMatrixCharacteristics(sec, true);
		}
	}

	private static class CpmmIndexFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, Long, IndexedMatrixValue>  
	{
		private static final long serialVersionUID = -1187183128301671162L;

		private boolean _left = false;
		
		public CpmmIndexFunction( boolean left ) {
			_left = left;
		}
		
		@Override
		public Tuple2<Long, IndexedMatrixValue> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) 
			throws Exception 
		{
			IndexedMatrixValue value = new IndexedMatrixValue();
			value.set(arg0._1(), new MatrixBlock(arg0._2()));
			
			Long key = _left ? arg0._1.getColumnIndex() : arg0._1.getRowIndex();
			return new Tuple2<Long, IndexedMatrixValue>(key, value);
		}	
	}

	private static class CpmmMultiplyFunction implements PairFunction<Tuple2<Long, Tuple2<IndexedMatrixValue,IndexedMatrixValue>>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -2009255629093036642L;
		
		private AggregateBinaryOperator _op = null;
		private boolean _useNativeBLAS;
		
		public CpmmMultiplyFunction(boolean useNativeBLAS)
		{
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			_useNativeBLAS = useNativeBLAS;
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<Long, Tuple2<IndexedMatrixValue, IndexedMatrixValue>> arg0)
			throws Exception 
		{
			MatrixBlock blkIn1 = (MatrixBlock)arg0._2()._1().getValue();
			MatrixBlock blkIn2 = (MatrixBlock)arg0._2()._2().getValue();
			MatrixIndexes ixOut = new MatrixIndexes();
			MatrixBlock blkOut = new MatrixBlock();
			
			//core block matrix multiplication 
			blkIn1.aggregateBinaryOperations(blkIn1, blkIn2, blkOut, _op, _useNativeBLAS);
			
			//return target block
			ixOut.setIndexes(arg0._2()._1().getIndexes().getRowIndex(), 
					         arg0._2()._2().getIndexes().getColumnIndex());
			return new Tuple2<MatrixIndexes, MatrixBlock>( ixOut, blkOut );
		}
	}
}
