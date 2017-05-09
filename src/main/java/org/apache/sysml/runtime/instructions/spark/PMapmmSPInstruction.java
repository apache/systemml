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


import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import org.apache.sysml.lops.PMapMult;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBlock;
import org.apache.sysml.runtime.instructions.spark.functions.IsBlockInRange;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.utils.NativeHelper;

/**
 * This pmapmm matrix multiplication instruction is still experimental
 * not integrated in automatic operator selection yet.
 * 
 */
public class PMapmmSPInstruction extends BinarySPInstruction 
{
	private static final int NUM_ROWBLOCKS=4;
	
	public PMapmmSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, 
			                    String opcode, String istr )
	{
		super(op, in1, in2, out, opcode, istr);
		_sptype = SPINSTRUCTION_TYPE.MAPMM;
	}

	public static PMapmmSPInstruction parseInstruction( String str ) 
		throws DMLRuntimeException 
	{
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if ( opcode.equalsIgnoreCase(PMapMult.OPCODE)) {
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand out = new CPOperand(parts[3]);
			
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			return new PMapmmSPInstruction(aggbin, in1, in2, out, opcode, str);
		} 
		else {
			throw new DMLRuntimeException("PMapmmSPInstruction.parseInstruction():: Unknown opcode " + opcode);
		}		
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{	
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		
		//get inputs
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable( input1.getName() );
		JavaPairRDD<MatrixIndexes,MatrixBlock> in2 = sec.getBinaryBlockRDDHandleForVariable( input2.getName() ); 
		MatrixCharacteristics mc1 = sec.getMatrixCharacteristics(input1.getName());		
		
		// This avoids errors such as java.lang.UnsupportedOperationException: Cannot change storage level of an RDD after it was already assigned a level
		// Ideally, we should ensure that we donot redundantly call persist on the same RDD.
		StorageLevel pmapmmStorageLevel = StorageLevel.MEMORY_AND_DISK();
		
		//cache right hand side because accessed many times
		in2 = in2.repartition(sec.getSparkContext().defaultParallelism())
				 .persist(pmapmmStorageLevel);
		
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = null;
		for( int i=0; i<mc1.getRows(); i+=NUM_ROWBLOCKS*mc1.getRowsPerBlock() ) 
		{
			//create broadcast for rdd partition
			JavaPairRDD<MatrixIndexes,MatrixBlock> rdd = in1
					.filter(new IsBlockInRange(i+1, i+NUM_ROWBLOCKS*mc1.getRowsPerBlock(), 1, mc1.getCols(), mc1))
					.mapToPair(new PMapMMRebaseBlocksFunction(i/mc1.getRowsPerBlock()));
			
			int rlen = (int)Math.min(mc1.getRows()-i, NUM_ROWBLOCKS*mc1.getRowsPerBlock());
			PartitionedBlock<MatrixBlock> pmb = SparkExecutionContext.toPartitionedMatrixBlock(rdd, rlen, (int)mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock(), -1L);
			Broadcast<PartitionedBlock<MatrixBlock>> bpmb = sec.getSparkContext().broadcast(pmb);
			
			//matrix multiplication
			JavaPairRDD<MatrixIndexes,MatrixBlock> rdd2 = in2
					.flatMapToPair(new PMapMMFunction(bpmb, i/mc1.getRowsPerBlock(), NativeHelper.isNativeLibraryLoaded()));
			rdd2 = RDDAggregateUtils.sumByKeyStable(rdd2, false);
			rdd2.persist(pmapmmStorageLevel)
			    .count();
			bpmb.unpersist(false);
			
			if( out == null )
				out = rdd2;
			else
				out = out.union(rdd2);
		}
		
		//cache final result
		out = out.persist(pmapmmStorageLevel);
		out.count();
		
		//put output RDD handle into symbol table
		sec.setRDDHandleForVariable(output.getName(), out);
		sec.addLineageRDD(output.getName(), input1.getName());
		sec.addLineageRDD(output.getName(), input2.getName());
			
		//update output statistics if not inferred
		updateBinaryMMOutputMatrixCharacteristics(sec, true);
	}

	private static class PMapMMRebaseBlocksFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 98051757210704132L;

		private int _offset = -1;
		
		public PMapMMRebaseBlocksFunction(int offset){
			_offset = offset;
		}
		
		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			long rix = arg0._1().getRowIndex()-_offset;
			MatrixIndexes ixout = new MatrixIndexes(rix, arg0._1().getColumnIndex());
			return new Tuple2<MatrixIndexes,MatrixBlock>(ixout, arg0._2());
		}
	}

	private static class PMapMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -4520080421816885321L;

		private AggregateBinaryOperator _op = null;
		private Broadcast<PartitionedBlock<MatrixBlock>> _pbc = null;
		private long _offset = -1;
		private boolean _useNativeBLAS;
		
		public PMapMMFunction( Broadcast<PartitionedBlock<MatrixBlock>> binput, long offset, boolean useNativeBLAS )
		{
			_pbc = binput;
			_offset = offset;
			_useNativeBLAS = useNativeBLAS;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
			throws Exception 
		{
			PartitionedBlock<MatrixBlock> pm = _pbc.value();
			
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			MatrixIndexes ixOut = new MatrixIndexes();
			MatrixBlock blkOut = new MatrixBlock();
			
			ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes, MatrixBlock>>();
			
			//get the right hand side matrix
			for( int i=1; i<=pm.getNumRowBlocks(); i++ ) {
				MatrixBlock left = pm.getBlock(i, (int)ixIn.getRowIndex());
			
				//execute matrix-vector mult
				OperationsOnMatrixValues.performAggregateBinary( 
						new MatrixIndexes(i,ixIn.getRowIndex()), left, ixIn, blkIn, ixOut, blkOut, _op, _useNativeBLAS);						
				
				//output new tuple
				ixOut.setIndexes(_offset+i, ixOut.getColumnIndex());
				ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut));
			}
			
			return ret.iterator();
		}
	}
}
