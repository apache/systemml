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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.PartitionPruningRDD;

import scala.Function1;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.runtime.AbstractFunction1;

import org.apache.sysml.hops.AggBinaryOp.SparkAggType;
import org.apache.sysml.lops.LeftIndex.LixCacheType;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.data.LazyIterableIterator;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.instructions.spark.functions.IsBlockInRange;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.util.IndexRange;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * This class implements the matrix indexing functionality inside CP.  
 * Example instructions: 
 *     rangeReIndex:mVar1:Var2:Var3:Var4:Var5:mVar6
 *         input=mVar1, output=mVar6, 
 *         bounds = (Var2,Var3,Var4,Var5)
 *         rowindex_lower: Var2, rowindex_upper: Var3 
 *         colindex_lower: Var4, colindex_upper: Var5
 *     leftIndex:mVar1:mVar2:Var3:Var4:Var5:Var6:mVar7
 *         triggered by "mVar1[Var3:Var4, Var5:Var6] = mVar2"
 *         the result is stored in mVar7
 *  
 */
public class MatrixIndexingSPInstruction  extends IndexingSPInstruction
{
	private final LixCacheType _type;

	public MatrixIndexingSPInstruction(Operator op, CPOperand in, CPOperand rl, CPOperand ru, CPOperand cl, CPOperand cu, 
			                          CPOperand out, SparkAggType aggtype, String opcode, String istr)
	{
		super(op, in, rl, ru, cl, cu, out, aggtype, opcode, istr);
		_type = LixCacheType.NONE;
	}
	
	public MatrixIndexingSPInstruction(Operator op, CPOperand lhsInput, CPOperand rhsInput, CPOperand rl, CPOperand ru, CPOperand cl, CPOperand cu, 
			                          CPOperand out, LixCacheType type, String opcode, String istr)
	{
		super(op, lhsInput, rhsInput, rl, ru, cl, cu, out, opcode, istr);
		_type = type;
	}
	
	@Override
	public void processInstruction(ExecutionContext ec)
			throws DMLRuntimeException 
	{	
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		String opcode = getOpcode();
		
		//get indexing range
		long rl = ec.getScalarInput(rowLower.getName(), rowLower.getValueType(), rowLower.isLiteral()).getLongValue();
		long ru = ec.getScalarInput(rowUpper.getName(), rowUpper.getValueType(), rowUpper.isLiteral()).getLongValue();
		long cl = ec.getScalarInput(colLower.getName(), colLower.getValueType(), colLower.isLiteral()).getLongValue();
		long cu = ec.getScalarInput(colUpper.getName(), colUpper.getValueType(), colUpper.isLiteral()).getLongValue();
		IndexRange ixrange = new IndexRange(rl, ru, cl, cu);
		
		//right indexing
		if( opcode.equalsIgnoreCase("rangeReIndex") )
		{
			//update and check output dimensions
			MatrixCharacteristics mcIn = sec.getMatrixCharacteristics(input1.getName());
			MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(output.getName());
			mcOut.set(ru-rl+1, cu-cl+1, mcIn.getRowsPerBlock(), mcIn.getColsPerBlock());
			checkValidOutputDimensions(mcOut);
			
			//execute right indexing operation (partitioning-preserving if possible)
			JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable( input1.getName() );
			
			if( isSingleBlockLookup(mcIn, ixrange) ) {
				sec.setMatrixOutput(output.getName(), singleBlockIndexing(in1, mcIn, mcOut, ixrange));
			}
			else if( isMultiBlockLookup(in1, mcIn, mcOut, ixrange) ) {
				sec.setMatrixOutput(output.getName(), multiBlockIndexing(in1, mcIn, mcOut, ixrange));
			}
			else { //rdd output for general case
				JavaPairRDD<MatrixIndexes,MatrixBlock> out = generalCaseRightIndexing(in1, mcIn, mcOut, ixrange, _aggType);
					
				//put output RDD handle into symbol table
				sec.setRDDHandleForVariable(output.getName(), out);
				sec.addLineageRDD(output.getName(), input1.getName());	
			}
		}
		//left indexing
		else if ( opcode.equalsIgnoreCase("leftIndex") || opcode.equalsIgnoreCase("mapLeftIndex"))
		{
			String rddVar = (_type==LixCacheType.LEFT) ? input2.getName() : input1.getName();
			String bcVar = (_type==LixCacheType.LEFT) ? input1.getName() : input2.getName();
			JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable( rddVar );
			PartitionedBroadcast<MatrixBlock> broadcastIn2 = null;
			JavaPairRDD<MatrixIndexes,MatrixBlock> in2 = null;
			JavaPairRDD<MatrixIndexes,MatrixBlock> out = null;
			
			//update and check output dimensions
			MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(output.getName());
			MatrixCharacteristics mcLeft = ec.getMatrixCharacteristics(input1.getName());
			mcOut.set(mcLeft.getRows(), mcLeft.getCols(), mcLeft.getRowsPerBlock(), mcLeft.getColsPerBlock());
			checkValidOutputDimensions(mcOut);
			
			//note: always matrix rhs, scalars are preprocessed via cast to 1x1 matrix
			MatrixCharacteristics mcRight = ec.getMatrixCharacteristics(input2.getName());
				
			//sanity check matching index range and rhs dimensions
			if(!mcRight.dimsKnown()) {
				throw new DMLRuntimeException("The right input matrix dimensions are not specified for MatrixIndexingSPInstruction");
			}
			if(!(ru-rl+1 == mcRight.getRows() && cu-cl+1 == mcRight.getCols())) {
				throw new DMLRuntimeException("Invalid index range of leftindexing: ["+rl+":"+ru+","+cl+":"+cu+"] vs ["+mcRight.getRows()+"x"+mcRight.getCols()+"]." );
			}
			
			if(opcode.equalsIgnoreCase("mapLeftIndex")) 
			{
				broadcastIn2 = sec.getBroadcastForVariable( bcVar );
				
				//partitioning-preserving mappartitions (key access required for broadcast loopkup)
				out = in1.mapPartitionsToPair(
						new LeftIndexPartitionFunction(broadcastIn2, ixrange, _type, mcOut), true);
			}
			else { //general case
				// zero-out lhs
				in1 = in1.mapToPair(new ZeroOutLHS(false, ixrange, mcLeft));
				
				// slice rhs, shift and merge with lhs
				in2 = sec.getBinaryBlockRDDHandleForVariable( input2.getName() )
					    .flatMapToPair(new SliceRHSForLeftIndexing(ixrange, mcLeft));
				out = RDDAggregateUtils.mergeByKey(in1.union(in2));
			}
			
			sec.setRDDHandleForVariable(output.getName(), out);
			sec.addLineageRDD(output.getName(), rddVar);
			if( broadcastIn2 != null)
				sec.addLineageBroadcast(output.getName(), bcVar);
			if(in2 != null) 
				sec.addLineageRDD(output.getName(), input2.getName());
		}
		else
			throw new DMLRuntimeException("Invalid opcode (" + opcode +") encountered in MatrixIndexingSPInstruction.");		
	}


	public static MatrixBlock inmemoryIndexing(JavaPairRDD<MatrixIndexes,MatrixBlock> in1, 
			 MatrixCharacteristics mcIn, MatrixCharacteristics mcOut, IndexRange ixrange) throws DMLRuntimeException {
		if( isSingleBlockLookup(mcIn, ixrange) ) {
			return singleBlockIndexing(in1, mcIn, mcOut, ixrange);
		}
		else if( isMultiBlockLookup(in1, mcIn, mcOut, ixrange) ) {
			return multiBlockIndexing(in1, mcIn, mcOut, ixrange);
		}
		else
			throw new DMLRuntimeException("Incorrect usage of inmemoryIndexing");
	}
	
	private static MatrixBlock multiBlockIndexing(JavaPairRDD<MatrixIndexes,MatrixBlock> in1, 
			 MatrixCharacteristics mcIn, MatrixCharacteristics mcOut, IndexRange ixrange) throws DMLRuntimeException {
		//create list of all required matrix indexes
		List<MatrixIndexes> filter = new ArrayList<MatrixIndexes>();
		long rlix = UtilFunctions.computeBlockIndex(ixrange.rowStart, mcIn.getRowsPerBlock());
		long ruix = UtilFunctions.computeBlockIndex(ixrange.rowEnd, mcIn.getRowsPerBlock());
		long clix = UtilFunctions.computeBlockIndex(ixrange.colStart, mcIn.getColsPerBlock());
		long cuix = UtilFunctions.computeBlockIndex(ixrange.colEnd, mcIn.getColsPerBlock());
		for( long r=rlix; r<=ruix; r++ )
			for( long c=clix; c<=cuix; c++ )
				filter.add( new MatrixIndexes(r,c) );
		
		//wrap PartitionPruningRDD around input to exploit pruning for out-of-core datasets
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = createPartitionPruningRDD(in1, filter);
		out = out.filter(new IsBlockInRange(ixrange.rowStart, ixrange.rowEnd, ixrange.colStart, ixrange.colEnd, mcOut)) //filter unnecessary blocks 
				 .mapToPair(new SliceBlock2(ixrange, mcOut));       //slice relevant blocks
		
		//collect output without shuffle to avoid side-effects with custom PartitionPruningRDD
		MatrixBlock mbout = SparkExecutionContext.toMatrixBlock(out, (int)mcOut.getRows(), 
				(int)mcOut.getCols(), mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), -1);
		return mbout;
	}
	
	private static MatrixBlock singleBlockIndexing(JavaPairRDD<MatrixIndexes,MatrixBlock> in1, 
			 MatrixCharacteristics mcIn, MatrixCharacteristics mcOut, IndexRange ixrange) throws DMLRuntimeException {
		//single block output via lookup (on partitioned inputs, this allows for single partition
		//access to avoid a full scan of the input; note that this is especially important for 
		//out-of-core datasets as entire partitions are read, not just keys as in the in-memory setting.
		long rix = UtilFunctions.computeBlockIndex(ixrange.rowStart, mcIn.getRowsPerBlock());
		long cix = UtilFunctions.computeBlockIndex(ixrange.colStart, mcIn.getColsPerBlock());
		List<MatrixBlock> list = in1.lookup(new MatrixIndexes(rix, cix));
		if( list.size() != 1 )
			throw new DMLRuntimeException("Block lookup returned "+list.size()+" blocks (expected 1).");
		
		MatrixBlock tmp = list.get(0);
		MatrixBlock mbout = (tmp.getNumRows()==mcOut.getRows() && tmp.getNumColumns()==mcOut.getCols()) ? 
				tmp : tmp.sliceOperations( //reference full block or slice out sub-block
				UtilFunctions.computeCellInBlock(ixrange.rowStart, mcIn.getRowsPerBlock()), 
				UtilFunctions.computeCellInBlock(ixrange.rowEnd, mcIn.getRowsPerBlock()), 
				UtilFunctions.computeCellInBlock(ixrange.colStart, mcIn.getColsPerBlock()), 
				UtilFunctions.computeCellInBlock(ixrange.colEnd, mcIn.getColsPerBlock()), new MatrixBlock());
		return mbout;
	}
	
	public static JavaPairRDD<MatrixIndexes,MatrixBlock> generalCaseRightIndexing(JavaPairRDD<MatrixIndexes,MatrixBlock> in1, 
			 MatrixCharacteristics mcIn, MatrixCharacteristics mcOut, IndexRange ixrange, SparkAggType aggType) {
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = null;
		
		if( isPartitioningPreservingRightIndexing(mcIn, ixrange) ) {
			out = in1.mapPartitionsToPair(
					new SliceBlockPartitionFunction(ixrange, mcOut), true);
		}
		else if( aggType == SparkAggType.NONE
			|| OptimizerUtils.isIndexingRangeBlockAligned(ixrange, mcIn) ) {
			out = in1.filter(new IsBlockInRange(ixrange.rowStart, ixrange.rowEnd, ixrange.colStart, ixrange.colEnd, mcOut))
		             .mapToPair(new SliceSingleBlock(ixrange, mcOut));
		}
		else {
			out = in1.filter(new IsBlockInRange(ixrange.rowStart, ixrange.rowEnd, ixrange.colStart, ixrange.colEnd, mcOut))
		             .flatMapToPair(new SliceMultipleBlocks(ixrange, mcOut));
			out = RDDAggregateUtils.mergeByKey(out);
		}
		return out;
	}
	
	private static void checkValidOutputDimensions(MatrixCharacteristics mcOut) 
		throws DMLRuntimeException
	{
		if(!mcOut.dimsKnown()) {
			throw new DMLRuntimeException("MatrixIndexingSPInstruction: The updated output dimensions are invalid: " + mcOut);
		}
	}

	private static boolean isPartitioningPreservingRightIndexing(MatrixCharacteristics mcIn, IndexRange ixrange) {
		return ( mcIn.dimsKnown() &&
				(ixrange.rowStart==1 && ixrange.rowEnd==mcIn.getRows() && mcIn.getCols()<=mcIn.getColsPerBlock() )   //1-1 column block indexing
			  ||(ixrange.colStart==1 && ixrange.colEnd==mcIn.getCols() && mcIn.getRows()<=mcIn.getRowsPerBlock() )); //1-1 row block indexing
	}
	
	/**
	 * Indicates if the given index range only covers a single blocks of the inputs matrix.
	 * In this case, we perform a key lookup which is very efficient in case of existing
	 * partitioner, especially for out-of-core datasets.
	 * 
	 * @param mcIn matrix characteristics
	 * @param ixrange index range
	 * @return true if index range covers a single block of the input matrix
	 */
	public static boolean isSingleBlockLookup(MatrixCharacteristics mcIn, IndexRange ixrange) {
		return UtilFunctions.computeBlockIndex(ixrange.rowStart, mcIn.getRowsPerBlock())
			== UtilFunctions.computeBlockIndex(ixrange.rowEnd, mcIn.getRowsPerBlock())
			&& UtilFunctions.computeBlockIndex(ixrange.colStart, mcIn.getColsPerBlock())
			== UtilFunctions.computeBlockIndex(ixrange.colEnd, mcIn.getColsPerBlock());
	}
	
	/**
	 * Indicates if the given index range and input matrix exhibit the following properties:
	 * (1) existing hash partitioner, (2) out-of-core input matrix (larger than aggregated memory), 
	 * (3) aligned indexing range (which does not required aggregation), and (4) the output fits 
	 * twice in memory (in order to collect the result). 
	 * 
	 * @param in input matrix
	 * @param mcIn input matrix characteristics
	 * @param mcOut output matrix characteristics
	 * @param ixrange index range
	 * @return true if index range requires a multi-block lookup
	 */
	public static boolean isMultiBlockLookup(JavaPairRDD<?,?> in, MatrixCharacteristics mcIn, MatrixCharacteristics mcOut, IndexRange ixrange) {
		return SparkUtils.isHashPartitioned(in)                          //existing partitioner
			&& OptimizerUtils.estimatePartitionedSizeExactSparsity(mcIn) //out-of-core dataset
			   > SparkExecutionContext.getDataMemoryBudget(true, true)
			&& OptimizerUtils.isIndexingRangeBlockAligned(ixrange, mcIn) //no block aggregation
			&& OptimizerUtils.estimateSize(mcOut) < OptimizerUtils.getLocalMemBudget()/2; //outputs fits in memory
	}

	private static class SliceRHSForLeftIndexing implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 5724800998701216440L;
		
		private IndexRange _ixrange = null; 
		private int _brlen = -1; 
		private int _bclen = -1;
		private long _rlen = -1;
		private long _clen = -1;
		
		public SliceRHSForLeftIndexing(IndexRange ixrange, MatrixCharacteristics mcLeft) {
			_ixrange = ixrange;
			_rlen = mcLeft.getRows();
			_clen = mcLeft.getCols();
			_brlen = mcLeft.getRowsPerBlock();
			_bclen = mcLeft.getColsPerBlock();
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> rightKV) 
			throws Exception 
		{
			IndexedMatrixValue in = SparkUtils.toIndexedMatrixBlock(rightKV);			
			ArrayList<IndexedMatrixValue> out = new ArrayList<IndexedMatrixValue>();
			OperationsOnMatrixValues.performShift(in, _ixrange, _brlen, _bclen, _rlen, _clen, out);
			return SparkUtils.fromIndexedMatrixBlock(out).iterator();
		}		
	}

	private static class ZeroOutLHS implements PairFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes,MatrixBlock> 
	{
		private static final long serialVersionUID = -3581795160948484261L;
		
		private boolean _complement = false;
		private IndexRange _ixrange = null;
		private int _brlen = -1;
		private int _bclen = -1;
		
		public ZeroOutLHS(boolean complement, IndexRange range, MatrixCharacteristics mcLeft) {
			_complement = complement;
			_ixrange = range;
			_brlen = mcLeft.getRowsPerBlock();
			_bclen = mcLeft.getColsPerBlock();
		}
		
		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> kv) 
			throws Exception 
		{
			if( !UtilFunctions.isInBlockRange(kv._1(), _brlen, _bclen, _ixrange) ) {
				return kv;
			}
			
			IndexRange range = UtilFunctions.getSelectedRangeForZeroOut(new IndexedMatrixValue(kv._1, kv._2), _brlen, _bclen, _ixrange);
			if(range.rowStart == -1 && range.rowEnd == -1 && range.colStart == -1 && range.colEnd == -1) {
				throw new Exception("Error while getting range for zero-out");
			}
			
			MatrixBlock zeroBlk = (MatrixBlock) kv._2.zeroOutOperations(new MatrixBlock(), range, _complement);
			return new Tuple2<MatrixIndexes, MatrixBlock>(kv._1, zeroBlk);
		}
	}

	private static class LeftIndexPartitionFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes,MatrixBlock>>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 1757075506076838258L;
		
		private final PartitionedBroadcast<MatrixBlock> _binput;
		private final IndexRange _ixrange;
		private final LixCacheType _type;
		private final int _brlen;
		private final int _bclen;
		
		public LeftIndexPartitionFunction(PartitionedBroadcast<MatrixBlock> binput, IndexRange ixrange, LixCacheType type, MatrixCharacteristics mc) 
		{
			_binput = binput;
			_ixrange = ixrange;
			_type = type;
			_brlen = mc.getRowsPerBlock();
			_bclen = mc.getColsPerBlock();
		}

		@Override
		public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0)
			throws Exception 
		{
			return new LeftIndexPartitionIterator(arg0);
		}

		private class LeftIndexPartitionIterator extends LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>>
		{
			public LeftIndexPartitionIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> in) {
				super(in);
			}
			
			@Override
			protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg) 
				throws Exception 
			{
				if(_type==LixCacheType.RIGHT && !UtilFunctions.isInBlockRange(arg._1(), _brlen, _bclen, _ixrange)) {
					return arg;
				}
				
				if( _type == LixCacheType.LEFT ) 
				{
					// LixCacheType.LEFT guarantees aligned blocks, so for each rhs inputs block
					// the the corresponding left block and perform blockwise left indexing
					MatrixIndexes ix = arg._1();
					MatrixBlock right = arg._2();

					int rl = UtilFunctions.computeCellInBlock(_ixrange.rowStart, _brlen);
					int ru = (int)Math.min(_ixrange.rowEnd, rl+right.getNumRows())-1;
					int cl = UtilFunctions.computeCellInBlock(_ixrange.colStart, _brlen);
					int cu = (int)Math.min(_ixrange.colEnd, cl+right.getNumColumns())-1;
					
					MatrixBlock left = _binput.getBlock((int)ix.getRowIndex(), (int)ix.getColumnIndex());
					MatrixBlock tmp = left.leftIndexingOperations(right, 
							rl, ru, cl, cu, new MatrixBlock(), UpdateType.COPY);
					
					return new Tuple2<MatrixIndexes, MatrixBlock>(ix, tmp);
				}
				else //LixCacheType.RIGHT
				{
					// Calculate global index of left hand side block
					long lhs_rl = Math.max(_ixrange.rowStart, (arg._1.getRowIndex()-1)*_brlen + 1);
					long lhs_ru = Math.min(_ixrange.rowEnd, arg._1.getRowIndex()*_brlen);
					long lhs_cl = Math.max(_ixrange.colStart, (arg._1.getColumnIndex()-1)*_bclen + 1);
					long lhs_cu = Math.min(_ixrange.colEnd, arg._1.getColumnIndex()*_bclen);
					
					// Calculate global index of right hand side block
					long rhs_rl = lhs_rl - _ixrange.rowStart + 1;
					long rhs_ru = rhs_rl + (lhs_ru - lhs_rl);
					long rhs_cl = lhs_cl - _ixrange.colStart + 1;
					long rhs_cu = rhs_cl + (lhs_cu - lhs_cl);
					
					// Provide global zero-based index to sliceOperations
					MatrixBlock slicedRHSMatBlock = _binput.sliceOperations(rhs_rl, rhs_ru, rhs_cl, rhs_cu, new MatrixBlock());
					
					// Provide local zero-based index to leftIndexingOperations
					int lhs_lrl = UtilFunctions.computeCellInBlock(lhs_rl, _brlen);
					int lhs_lru = UtilFunctions.computeCellInBlock(lhs_ru, _brlen);
					int lhs_lcl = UtilFunctions.computeCellInBlock(lhs_cl, _bclen);
					int lhs_lcu = UtilFunctions.computeCellInBlock(lhs_cu, _bclen);
					MatrixBlock ret = arg._2.leftIndexingOperations(slicedRHSMatBlock, lhs_lrl, lhs_lru, lhs_lcl, lhs_lcu, new MatrixBlock(), UpdateType.COPY);
					return new Tuple2<MatrixIndexes, MatrixBlock>(arg._1, ret);
				}
			}
		}
	}

	private static class SliceSingleBlock implements PairFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -6724027136506200924L;
		
		private final IndexRange _ixrange;
		private final int _brlen; 
		private final int _bclen;
		
		public SliceSingleBlock(IndexRange ixrange, MatrixCharacteristics mcOut) {
			_ixrange = ixrange;
			_brlen = mcOut.getRowsPerBlock();
			_bclen = mcOut.getColsPerBlock();
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> kv) 
			throws Exception 
		{	
			//get inputs (guaranteed to fall into indexing range)
			MatrixIndexes ix = kv._1();
			MatrixBlock block = kv._2();
			
			//compute local index range 
			long grix = UtilFunctions.computeCellIndex(ix.getRowIndex(), _brlen, 0);
			long gcix = UtilFunctions.computeCellIndex(ix.getColumnIndex(), _bclen, 0);
			int lrl = (int)((_ixrange.rowStart<grix) ? 0 : _ixrange.rowStart - grix);
			int lcl = (int)((_ixrange.colStart<gcix) ? 0 : _ixrange.colStart - gcix);
			int lru = (int)Math.min(block.getNumRows()-1, _ixrange.rowEnd - grix);
			int lcu = (int)Math.min(block.getNumColumns()-1, _ixrange.colEnd - gcix);
			
			//compute output index
			MatrixIndexes ixOut = new MatrixIndexes(
				ix.getRowIndex() - (_ixrange.rowStart-1)/_brlen, 
				ix.getColumnIndex() - (_ixrange.colStart-1)/_bclen);
			
			//create return matrix block (via shallow copy or slice)
			if( lrl == 0 && lru == block.getNumRows()-1
				&& lcl == 0 && lcu == block.getNumColumns()-1 ) {
				return new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, block);
			}
			else {
				return new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, 
					block.sliceOperations(lrl, lru, lcl, lcu, new MatrixBlock()));
			}
		}		
	}
	
	private static class SliceMultipleBlocks implements PairFlatMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 5733886476413136826L;
		
		private final IndexRange _ixrange;
		private final int _brlen; 
		private final int _bclen;
		
		public SliceMultipleBlocks(IndexRange ixrange, MatrixCharacteristics mcOut) {
			_ixrange = ixrange;
			_brlen = mcOut.getRowsPerBlock();
			_bclen = mcOut.getColsPerBlock();
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> kv) 
			throws Exception 
		{	
			IndexedMatrixValue in = SparkUtils.toIndexedMatrixBlock(kv);
			ArrayList<IndexedMatrixValue> outlist = new ArrayList<IndexedMatrixValue>();
			OperationsOnMatrixValues.performSlice(in, _ixrange, _brlen, _bclen, outlist);
			return SparkUtils.fromIndexedMatrixBlock(outlist).iterator();
		}		
	}

	/**
	 * Equivalent to SliceBlock except a different function signature.
	 */
	private static class SliceBlock2 implements PairFunction<Tuple2<MatrixIndexes,MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 7481889252529447770L;
		
		private IndexRange _ixrange;
		private int _brlen; 
		private int _bclen;
		
		public SliceBlock2(IndexRange ixrange, MatrixCharacteristics mcOut) {
			_ixrange = ixrange;
			_brlen = mcOut.getRowsPerBlock();
			_bclen = mcOut.getColsPerBlock();
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> kv) 
			throws Exception 
		{	
			IndexedMatrixValue in = new IndexedMatrixValue(kv._1(), kv._2());
			ArrayList<IndexedMatrixValue> outlist = new ArrayList<IndexedMatrixValue>();
			OperationsOnMatrixValues.performSlice(in, _ixrange, _brlen, _bclen, outlist);
			return SparkUtils.fromIndexedMatrixBlock(outlist.get(0));
		}		
	}

	private static class SliceBlockPartitionFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes,MatrixBlock>>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -8111291718258309968L;
		
		private IndexRange _ixrange;
		private int _brlen; 
		private int _bclen;
		
		public SliceBlockPartitionFunction(IndexRange ixrange, MatrixCharacteristics mcOut) {
			_ixrange = ixrange;
			_brlen = mcOut.getRowsPerBlock();
			_bclen = mcOut.getColsPerBlock();
		}

		@Override
		public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0)
			throws Exception 
		{
			return new SliceBlockPartitionIterator(arg0);
		}	
		
		private class SliceBlockPartitionIterator extends LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>>
		{
			public SliceBlockPartitionIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> in) {
				super(in);
			}

			@Override
			protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg)
				throws Exception
			{
				IndexedMatrixValue in = SparkUtils.toIndexedMatrixBlock(arg);
				
				ArrayList<IndexedMatrixValue> outlist = new ArrayList<IndexedMatrixValue>();
				OperationsOnMatrixValues.performSlice(in, _ixrange, _brlen, _bclen, outlist);
				
				assert(outlist.size() == 1); //1-1 row/column block indexing
				return SparkUtils.fromIndexedMatrixBlock(outlist.get(0));
			}			
		}
	}
	
	/**
	 * Wraps the input RDD into a PartitionPruningRDD, which acts as a filter
	 * of required partitions. The distinct set of required partitions is determined
	 * via the partitioner of the input RDD.
	 * 
	 * @param in input matrix as {@code JavaPairRDD<MatrixIndexes,MatrixBlock>}
	 * @param filter partition filter
	 * @return matrix as {@code JavaPairRDD<MatrixIndexes,MatrixBlock>}
	 */
	private static JavaPairRDD<MatrixIndexes,MatrixBlock> createPartitionPruningRDD( 
			JavaPairRDD<MatrixIndexes,MatrixBlock> in, List<MatrixIndexes> filter )
	{
		//build hashset of required partition ids
		HashSet<Integer> flags = new HashSet<Integer>();
		Partitioner partitioner = in.rdd().partitioner().get();
		for( MatrixIndexes key : filter )
			flags.add(partitioner.getPartition(key));

		//create partition pruning rdd
		Function1<Object,Object> f = new PartitionPruningFunction(flags);
		PartitionPruningRDD<Tuple2<MatrixIndexes, MatrixBlock>> ppRDD = 
				PartitionPruningRDD.create(in.rdd(), f);

		//wrap output into java pair rdd
		return new JavaPairRDD<MatrixIndexes,MatrixBlock>(ppRDD, 
				ClassManifestFactory.fromClass(MatrixIndexes.class), 
				ClassManifestFactory.fromClass(MatrixBlock.class));
	}
	
	/**
	 * Filter function required to create a PartitionPruningRDD.
	 */
	private static class PartitionPruningFunction extends AbstractFunction1<Object,Object> implements Serializable
	{
		private static final long serialVersionUID = -9114299718258329951L;

		private HashSet<Integer> _filterFlags = null;

		public PartitionPruningFunction(HashSet<Integer> flags) {
			_filterFlags = flags;
		}

		@Override
		public Boolean apply(Object partIndex) {
			return _filterFlags.contains((Integer)partIndex);
		}
	}
}
