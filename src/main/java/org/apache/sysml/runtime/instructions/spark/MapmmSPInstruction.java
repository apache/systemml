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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.sysml.hops.AggBinaryOp.SparkAggType;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.lops.MapMult;
import org.apache.sysml.lops.MapMult.CacheType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.data.LazyIterableIterator;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.instructions.spark.functions.FilterNonEmptyBlocksFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.utils.NativeHelper;

public class MapmmSPInstruction extends BinarySPInstruction 
{	
	private CacheType _type = null;
	private boolean _outputEmpty = true;
	private SparkAggType _aggtype;
	
	public MapmmSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, CacheType type, 
			boolean outputEmpty, SparkAggType aggtype, String opcode, String istr )
	{
		super(op, in1, in2, out, opcode, istr);
		_sptype = SPINSTRUCTION_TYPE.MAPMM;
		
		_type = type;
		_outputEmpty = outputEmpty;
		_aggtype = aggtype;
	}

	public static MapmmSPInstruction parseInstruction( String str ) 
		throws DMLRuntimeException 
	{
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if(!opcode.equalsIgnoreCase(MapMult.OPCODE)) 
			throw new DMLRuntimeException("MapmmSPInstruction.parseInstruction():: Unknown opcode " + opcode);
			
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[3]);
		CacheType type = CacheType.valueOf(parts[4]);
		boolean outputEmpty = Boolean.parseBoolean(parts[5]);
		SparkAggType aggtype = SparkAggType.valueOf(parts[6]);
		
		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		return new MapmmSPInstruction(aggbin, in1, in2, out, type, outputEmpty, aggtype, opcode, str);		
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{	
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		
		CacheType type = _type;
		String rddVar = type.isRight() ? input1.getName() : input2.getName();
		String bcastVar = type.isRight() ? input2.getName() : input1.getName();
		MatrixCharacteristics mcRdd = sec.getMatrixCharacteristics(rddVar);
		MatrixCharacteristics mcBc = sec.getMatrixCharacteristics(bcastVar);
		
		//get input rdd
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable(rddVar);
		
		//investigate if a repartitioning - including a potential flip of broadcast and rdd 
		//inputs - is required to ensure moderately sized output partitions (2GB limitation)
		if( requiresFlatMapFunction(type, mcBc) &&
			requiresRepartitioning(type, mcRdd, mcBc, in1.getNumPartitions()) ) 
		{
			int numParts = getNumRepartitioning(type, mcRdd, mcBc);
			int numParts2 = getNumRepartitioning(type.getFlipped(), mcBc, mcRdd);
			if( numParts2 > numParts ) { //flip required
				type = type.getFlipped();
				rddVar = type.isRight() ? input1.getName() : input2.getName();
				bcastVar = type.isRight() ? input2.getName() : input1.getName();
				mcRdd = sec.getMatrixCharacteristics(rddVar);
				mcBc = sec.getMatrixCharacteristics(bcastVar);
				in1 = sec.getBinaryBlockRDDHandleForVariable(rddVar);
				LOG.warn("Mapmm: Switching rdd ('"+bcastVar+"') and broadcast ('"+rddVar+"') inputs "
						+ "for repartitioning because this allows better control of output partition "
						+ "sizes ("+numParts+" < "+numParts2+").");	
			}
		}
		
		//get inputs
		PartitionedBroadcast<MatrixBlock> in2 = sec.getBroadcastForVariable(bcastVar); 
		
		//empty input block filter
		if( !_outputEmpty )
			in1 = in1.filter(new FilterNonEmptyBlocksFunction());
		
		//execute mapmm and aggregation if necessary and put output into symbol table
		if( _aggtype == SparkAggType.SINGLE_BLOCK )
		{
			JavaRDD<MatrixBlock> out = in1.map(new RDDMapMMFunction2(type, in2, NativeHelper.isNativeLibraryLoaded()));
			MatrixBlock out2 = RDDAggregateUtils.sumStable(out);
			
			//put output block into symbol table (no lineage because single block)
			//this also includes implicit maintenance of matrix characteristics
			sec.setMatrixOutput(output.getName(), out2);
		}
		else //MULTI_BLOCK or NONE
		{
			JavaPairRDD<MatrixIndexes,MatrixBlock> out = null;
			if( requiresFlatMapFunction(type, mcBc) ) {
				if( requiresRepartitioning(type, mcRdd, mcBc, in1.getNumPartitions()) ) {
					int numParts = getNumRepartitioning(type, mcRdd, mcBc);
					LOG.warn("Mapmm: Repartition input rdd '"+rddVar+"' from "+in1.getNumPartitions()+" to "
							+numParts+" partitions to satisfy size restrictions of output partitions.");
					in1 = in1.repartition(numParts);
				}
				out = in1.flatMapToPair( new RDDFlatMapMMFunction(type, in2, NativeHelper.isNativeLibraryLoaded()) );
			}
			else if( preservesPartitioning(mcRdd, type) )
				out = in1.mapPartitionsToPair(new RDDMapMMPartitionFunction(type, in2, NativeHelper.isNativeLibraryLoaded()), true);
			else
				out = in1.mapToPair( new RDDMapMMFunction(type, in2, NativeHelper.isNativeLibraryLoaded()) );
			
			//empty output block filter
			if( !_outputEmpty )
				out = out.filter(new FilterNonEmptyBlocksFunction());
			
			if( _aggtype == SparkAggType.MULTI_BLOCK )
				out = RDDAggregateUtils.sumByKeyStable(out, false);
		
			//put output RDD handle into symbol table
			sec.setRDDHandleForVariable(output.getName(), out);
			sec.addLineageRDD(output.getName(), rddVar);
			sec.addLineageBroadcast(output.getName(), bcastVar);
			
			//update output statistics if not inferred
			updateBinaryMMOutputMatrixCharacteristics(sec, true);
		}
	}

	private static boolean preservesPartitioning( MatrixCharacteristics mcIn, CacheType type )
	{
		if( type == CacheType.LEFT )
			return mcIn.dimsKnown() && mcIn.getRows() <= mcIn.getRowsPerBlock();
		else // RIGHT
			return mcIn.dimsKnown() && mcIn.getCols() <= mcIn.getColsPerBlock();
	}
	
	/**
	 * Indicates if there is a need to apply a flatmap rdd operation because a single 
	 * input block creates multiple output blocks.
	 * 
	 * @param type cache type
	 * @param mcBc matrix characteristics
	 * @return true if single input block creates multiple output blocks
	 */
	private static boolean requiresFlatMapFunction( CacheType type, MatrixCharacteristics mcBc)
	{
		return    (type == CacheType.LEFT && mcBc.getRows() > mcBc.getRowsPerBlock())
			   || (type == CacheType.RIGHT && mcBc.getCols() > mcBc.getColsPerBlock());
	}
	
	/**
	 * Indicates if there is a need to repartition the input RDD in order to increase the
	 * degree of parallelism or reduce the output partition size (e.g., Spark still has a
	 * 2GB limitation of partitions)
	 * 
	 * @param type cache type
	 * @param mcRdd rdd matrix characteristics
	 * @param mcBc ?
	 * @param numPartitions number of partitions
	 * @return true need to repartition input RDD
	 */
	private static boolean requiresRepartitioning( CacheType type, MatrixCharacteristics mcRdd, MatrixCharacteristics mcBc, int numPartitions ) {
		//note: as repartitioning requires data shuffling, we try to be very conservative here
		//approach: we repartition, if there is a "outer-product-like" mm (single block common dimension),
		//the size of output partitions (assuming dense) exceeds a size of 1GB 
		
		boolean isLeft = (type == CacheType.LEFT);
		boolean isOuter = isLeft ? 
				(mcRdd.getRows() <= mcRdd.getRowsPerBlock()) :
				(mcRdd.getCols() <= mcRdd.getColsPerBlock());
		boolean isLargeOutput = (OptimizerUtils.estimatePartitionedSizeExactSparsity(isLeft?mcBc.getRows():mcRdd.getRows(),
				isLeft?mcRdd.getCols():mcBc.getCols(), isLeft?mcBc.getRowsPerBlock():mcRdd.getRowsPerBlock(),
				isLeft?mcRdd.getColsPerBlock():mcBc.getColsPerBlock(), 1.0) / numPartitions) > 1024*1024*1024; 
		return isOuter && isLargeOutput && mcRdd.dimsKnown() && mcBc.dimsKnown();
	}

	/**
	 * Computes the number of target partitions for repartitioning input rdds in case of 
	 * outer-product-like mm.
	 * 
	 * @param type cache type
	 * @param mcRdd rdd matrix characteristics
	 * @param mcBc ?
	 * @return number of target partitions for repartitioning
	 */
	private static int getNumRepartitioning( CacheType type, MatrixCharacteristics mcRdd, MatrixCharacteristics mcBc ) {
		boolean isLeft = (type == CacheType.LEFT);
		long sizeOutput = (OptimizerUtils.estimatePartitionedSizeExactSparsity(isLeft?mcBc.getRows():mcRdd.getRows(),
				isLeft?mcRdd.getCols():mcBc.getCols(), isLeft?mcBc.getRowsPerBlock():mcRdd.getRowsPerBlock(),
				isLeft?mcRdd.getColsPerBlock():mcBc.getColsPerBlock(), 1.0)); 
		long numParts = sizeOutput / InfrastructureAnalyzer.getHDFSBlockSize();
		return (int)Math.min(numParts, (isLeft?mcRdd.getNumColBlocks():mcRdd.getNumRowBlocks()));
	}

	private static class RDDMapMMFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 8197406787010296291L;

		private final CacheType _type;
		private final AggregateBinaryOperator _op;
		private final PartitionedBroadcast<MatrixBlock> _pbc;
		private boolean _useNativeBLAS;
		
		public RDDMapMMFunction( CacheType type, PartitionedBroadcast<MatrixBlock> binput, boolean useNativeBLAS )
		{
			_type = type;
			_pbc = binput;
			_useNativeBLAS = useNativeBLAS;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}
		
		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			MatrixIndexes ixOut = new MatrixIndexes();
			MatrixBlock blkOut = new MatrixBlock();
			
			if( _type == CacheType.LEFT )
			{
				//get the right hand side matrix
				MatrixBlock left = _pbc.getBlock(1, (int)ixIn.getRowIndex());
				
				//execute matrix-vector mult
				OperationsOnMatrixValues.performAggregateBinary( 
						new MatrixIndexes(1,ixIn.getRowIndex()), left, ixIn, blkIn, ixOut, blkOut, _op, _useNativeBLAS);						
			}
			else //if( _type == CacheType.RIGHT )
			{
				//get the right hand side matrix
				MatrixBlock right = _pbc.getBlock((int)ixIn.getColumnIndex(), 1);
				
				//execute matrix-vector mult
				OperationsOnMatrixValues.performAggregateBinary(
						ixIn, blkIn, new MatrixIndexes(ixIn.getColumnIndex(),1), right, ixOut, blkOut, _op, _useNativeBLAS);					
			}
			
			//output new tuple
			return new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut);
		}
	}

	/**
	 * Similar to RDDMapMMFunction but with single output block 
	 */
	private static class RDDMapMMFunction2 implements Function<Tuple2<MatrixIndexes, MatrixBlock>, MatrixBlock> 
	{
		private static final long serialVersionUID = -2753453898072910182L;
		
		private final CacheType _type;
		private final AggregateBinaryOperator _op;
		private final PartitionedBroadcast<MatrixBlock> _pbc;
		private boolean _useNativeBLAS;
		
		public RDDMapMMFunction2( CacheType type, PartitionedBroadcast<MatrixBlock> binput, boolean useNativeBLAS )
		{
			_type = type;
			_pbc = binput;
			_useNativeBLAS = useNativeBLAS;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}
		
		@Override
		public MatrixBlock call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();
			
			if( _type == CacheType.LEFT )
			{
				//get the right hand side matrix
				MatrixBlock left = _pbc.getBlock(1, (int)ixIn.getRowIndex());
				
				//execute matrix-vector mult
				return (MatrixBlock) OperationsOnMatrixValues.performAggregateBinaryIgnoreIndexes( 
						left, blkIn, new MatrixBlock(), _op, _useNativeBLAS);						
			}
			else //if( _type == CacheType.RIGHT )
			{
				//get the right hand side matrix
				MatrixBlock right = _pbc.getBlock((int)ixIn.getColumnIndex(), 1);
				
				//execute matrix-vector mult
				return (MatrixBlock) OperationsOnMatrixValues.performAggregateBinaryIgnoreIndexes(
						blkIn, right, new MatrixBlock(), _op, _useNativeBLAS);
			}
		}
	}

	private static class RDDMapMMPartitionFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes, MatrixBlock>>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 1886318890063064287L;
	
		private final CacheType _type;
		private final AggregateBinaryOperator _op;
		private final PartitionedBroadcast<MatrixBlock> _pbc;
		private boolean _useNativeBLAS;
		
		public RDDMapMMPartitionFunction( CacheType type, PartitionedBroadcast<MatrixBlock> binput, boolean useNativeBLAS )
		{
			_type = type;
			_pbc = binput;
			_useNativeBLAS = useNativeBLAS;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}
		
		@Override
		public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0)
			throws Exception 
		{
			return new MapMMPartitionIterator(arg0, _useNativeBLAS);
		}
		
		/**
		 * Lazy mapmm iterator to prevent materialization of entire partition output in-memory.
		 * The implementation via mapPartitions is required to preserve partitioning information,
		 * which is important for performance. 
		 */
		private class MapMMPartitionIterator extends LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>>
		{
			boolean _useNativeBLAS;
			public MapMMPartitionIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> in, boolean useNativeBLAS) {
				super(in);
				_useNativeBLAS = useNativeBLAS;
			}

			@Override
			protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg)
				throws Exception
			{
				MatrixIndexes ixIn = arg._1();
				MatrixBlock blkIn = arg._2();
				MatrixBlock blkOut = new MatrixBlock();
		
				if( _type == CacheType.LEFT )
				{
					//get the right hand side matrix
					MatrixBlock left = _pbc.getBlock(1, (int)ixIn.getRowIndex());
					
					//execute index preserving matrix multiplication
					left.aggregateBinaryOperations(left, blkIn, blkOut, _op, _useNativeBLAS);						
				}
				else //if( _type == CacheType.RIGHT )
				{
					//get the right hand side matrix
					MatrixBlock right = _pbc.getBlock((int)ixIn.getColumnIndex(), 1);

					//execute index preserving matrix multiplication
					blkIn.aggregateBinaryOperations(blkIn, right, blkOut, _op, _useNativeBLAS);	
				}
			
				return new Tuple2<MatrixIndexes,MatrixBlock>(ixIn, blkOut);
			}			
		}
	}

	private static class RDDFlatMapMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -6076256569118957281L;
		
		private final CacheType _type;
		private final AggregateBinaryOperator _op;
		private final PartitionedBroadcast<MatrixBlock> _pbc;
		private boolean _useNativeBLAS;
		
		public RDDFlatMapMMFunction( CacheType type, PartitionedBroadcast<MatrixBlock> binput, boolean useNativeBLAS )
		{
			_type = type;
			_pbc = binput;
			_useNativeBLAS = useNativeBLAS;
			
			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}
		
		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<Tuple2<MatrixIndexes, MatrixBlock>>();
			
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			if( _type == CacheType.LEFT )
			{
				//for all matching left-hand-side blocks
				int len = _pbc.getNumRowBlocks();
				for( int i=1; i<=len; i++ ) 
				{
					MatrixBlock left = _pbc.getBlock(i, (int)ixIn.getRowIndex());
					MatrixIndexes ixOut = new MatrixIndexes();
					MatrixBlock blkOut = new MatrixBlock();
					
					//execute matrix-vector mult
					OperationsOnMatrixValues.performAggregateBinary( 
							new MatrixIndexes(i,ixIn.getRowIndex()), left, ixIn, blkIn, ixOut, blkOut, _op, _useNativeBLAS);	
					
					ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut));
				}
			}
			else //if( _type == CacheType.RIGHT )
			{
				//for all matching right-hand-side blocks
				int len = _pbc.getNumColumnBlocks();
				for( int j=1; j<=len; j++ ) 
				{
					//get the right hand side matrix
					MatrixBlock right = _pbc.getBlock((int)ixIn.getColumnIndex(), j);
					MatrixIndexes ixOut = new MatrixIndexes();
					MatrixBlock blkOut = new MatrixBlock();
					
					//execute matrix-vector mult
					OperationsOnMatrixValues.performAggregateBinary(
							ixIn, blkIn, new MatrixIndexes(ixIn.getColumnIndex(),j), right, ixOut, blkOut, _op, _useNativeBLAS);					
				
					ret.add(new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut));
				}
			}
			
			return ret.iterator();
		}
	}
}
