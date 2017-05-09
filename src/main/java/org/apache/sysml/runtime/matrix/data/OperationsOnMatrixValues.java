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


package org.apache.sysml.runtime.matrix.data;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.compress.CompressedMatrixBlock;
import org.apache.sysml.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.apache.sysml.runtime.functionobjects.Builtin;
import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.lops.PartialAggregate.CorrectionLocationType;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;
import org.apache.sysml.runtime.matrix.operators.UnaryOperator;
import org.apache.sysml.runtime.util.IndexRange;
import org.apache.sysml.runtime.util.UtilFunctions;


public class OperationsOnMatrixValues 
{
	
	public static void performScalarIgnoreIndexes(MatrixValue valueIn, MatrixValue valueOut, ScalarOperator op) 
		throws DMLRuntimeException
	{
		valueIn.scalarOperations(op, valueOut);
	}
	
	public static void performUnaryIgnoreIndexes(MatrixValue valueIn, MatrixValue valueOut, UnaryOperator op) 
		throws DMLRuntimeException
	{
		valueIn.unaryOperations(op, valueOut);
	}

	public static void performReorg(MatrixIndexes indexesIn, MatrixValue valueIn, MatrixIndexes indexesOut, 
			         MatrixValue valueOut, ReorgOperator op, int startRow, int startColumn, int length) 
		throws DMLRuntimeException
	{
		//operate on the value indexes first
		op.fn.execute(indexesIn, indexesOut);
		
		//operation on the cells inside the value
		valueIn.reorgOperations(op, valueOut, startRow, startColumn, length);
	}

	public static void performAppend(MatrixValue valueIn1, MatrixValue valueIn2,
			ArrayList<IndexedMatrixValue> outlist, int blockRowFactor, int blockColFactor,  boolean cbind, boolean m2IsLast, int nextNCol) 
	throws DMLRuntimeException
	{
		valueIn1.appendOperations(valueIn2, outlist, blockRowFactor, blockColFactor, cbind, m2IsLast, nextNCol);
	}
	
	public static void performZeroOut(MatrixIndexes indexesIn, MatrixValue valueIn, 
			MatrixIndexes indexesOut, MatrixValue valueOut, IndexRange range, boolean complementary) 
	throws DMLRuntimeException
	{
		valueIn.zeroOutOperations(valueOut, range, complementary);
		indexesOut.setIndexes(indexesIn);
	}
	
	// ------------- Ternary Operations -------------
	// tertiary where all three inputs are matrices
	public static void performTernary(MatrixIndexes indexesIn1, MatrixValue valueIn1, MatrixIndexes indexesIn2, MatrixValue valueIn2, 
			MatrixIndexes indexesIn3, MatrixValue valueIn3, CTableMap resultMap, MatrixBlock resultBlock, Operator op ) 
	throws DMLRuntimeException
	{
		//operation on the cells inside the value
		valueIn1.ternaryOperations(op, valueIn2, valueIn3, resultMap, resultBlock);
	}
	
	// tertiary where first two inputs are matrices, and third input is a scalar (double)
	public static void performTernary(MatrixIndexes indexesIn1, MatrixValue valueIn1, MatrixIndexes indexesIn2, MatrixValue valueIn2, 
			double scalarIn3, CTableMap resultMap, MatrixBlock resultBlock, Operator op) 
	throws DMLRuntimeException
	{
		//operation on the cells inside the value
		valueIn1.ternaryOperations(op, valueIn2, scalarIn3, false, resultMap, resultBlock);
	}
	
	// tertiary where first input is a matrix, and second and third inputs are scalars (double)
	public static void performTernary(MatrixIndexes indexesIn1, MatrixValue valueIn1, double scalarIn2, 
			double scalarIn3, CTableMap resultMap, MatrixBlock resultBlock, Operator op ) 
	throws DMLRuntimeException
	{
		//operation on the cells inside the value
		valueIn1.ternaryOperations(op, scalarIn2, scalarIn3, resultMap, resultBlock);
	}
	
	// tertiary where first input is a matrix, and second is scalars (double)
	public static void performTernary(MatrixIndexes indexesIn1, MatrixValue valueIn1, double scalarIn2, boolean left,
			int brlen, CTableMap resultMap, MatrixBlock resultBlock, Operator op ) 
	throws DMLRuntimeException
	{
		//operation on the cells inside the value
		valueIn1.ternaryOperations(op, indexesIn1, scalarIn2, left, brlen, resultMap, resultBlock);
	}
	
	// tertiary where first and third inputs are matrices, and second is a scalars (double)
	public static void performTernary(MatrixIndexes indexesIn1, MatrixValue valueIn1, double scalarIn2, 
			MatrixIndexes indexesIn3, MatrixValue valueIn3, CTableMap resultMap, MatrixBlock resultBlock, Operator op ) 
	throws DMLRuntimeException
	{
		//operation on the cells inside the value
		valueIn1.ternaryOperations(op, scalarIn2, valueIn3, resultMap, resultBlock);
	}
	// -----------------------------------------------------
	
	//binary operations are those that the indexes of both cells have to be matched
	public static void performBinaryIgnoreIndexes(MatrixValue value1, MatrixValue value2, 
			MatrixValue valueOut, BinaryOperator op) 
	throws DMLRuntimeException
	{
		value1.binaryOperations(op, value2, valueOut);
	}

	public static void startAggregation(MatrixValue valueOut, MatrixValue correction, AggregateOperator op, 
			int rlen, int clen, boolean sparseHint, boolean imbededCorrection)
		throws DMLRuntimeException
	{
		int outRow=0, outCol=0, corRow=0, corCol=0;
		if(op.correctionExists)
		{
			if(!imbededCorrection)
			{
				switch(op.correctionLocation)
				{
				case NONE:
					outRow=rlen;
					outCol=clen;
					corRow=rlen;
					corCol=clen;
					break;
				case LASTROW:
					outRow=rlen-1;
					outCol=clen;
					corRow=1;
					corCol=clen;
					break;
				case LASTCOLUMN:
					if(op.increOp.fn instanceof Builtin 
					   && ( ((Builtin)(op.increOp.fn)).bFunc == Builtin.BuiltinCode.MAXINDEX 
					        || ((Builtin)(op.increOp.fn)).bFunc == Builtin.BuiltinCode.MININDEX) )
					{
						outRow = rlen;
						outCol = 1;
						corRow = rlen;
						corCol = 1;
					}
					else{
						outRow=rlen;
						outCol=clen-1;
						corRow=rlen;
						corCol=1;
					}
					break;
				case LASTTWOROWS:
					outRow=rlen-2;
					outCol=clen;
					corRow=2;
					corCol=clen;
					break;
				case LASTTWOCOLUMNS:
					outRow=rlen;
					outCol=clen-2;
					corRow=rlen;
					corCol=2;
					break;
				case LASTFOURROWS:
					outRow=rlen-4;
					outCol=clen;
					corRow=4;
					corCol=clen;
					break;
				case LASTFOURCOLUMNS:
					outRow=rlen;
					outCol=clen-4;
					corRow=rlen;
					corCol=4;
					break;
				default:
						throw new DMLRuntimeException("unrecognized correctionLocation: "+op.correctionLocation);
				}
			}else
			{
				outRow=rlen;
				outCol=clen;
				corRow=rlen;
				corCol=clen;
			}
			
			//set initial values according to operator
			if(op.initialValue==0) {
				valueOut.reset(outRow, outCol, sparseHint);
				correction.reset(corRow, corCol, false);
			}
			else {
				valueOut.reset(outRow, outCol, op.initialValue);
				correction.reset(corRow, corCol, op.initialValue);
			}
			
		}
		else
		{
			if(op.initialValue==0)
				valueOut.reset(rlen, clen, sparseHint);
			else
				valueOut.reset(rlen, clen, op.initialValue);
		}
	}
	
	public static void incrementalAggregation(MatrixValue valueAgg, MatrixValue correction, MatrixValue valueAdd, 
			AggregateOperator op, boolean imbededCorrection) throws DMLRuntimeException
	{
		if(op.correctionExists)
		{
			if(!imbededCorrection || op.correctionLocation==CorrectionLocationType.NONE)
				valueAgg.incrementalAggregate(op, correction, valueAdd);
			else
				valueAgg.incrementalAggregate(op, valueAdd);
		}
		else
			valueAgg.binaryOperationsInPlace(op.increOp, valueAdd);
	}
	
	public static void performAggregateUnary(MatrixIndexes indexesIn, MatrixValue valueIn, MatrixIndexes indexesOut, 
			MatrixValue valueOut, AggregateUnaryOperator op,int brlen, int bclen)
		throws DMLRuntimeException
	{
		//operate on the value indexes first
		op.indexFn.execute(indexesIn, indexesOut);
		
		//perform on the value
		valueIn.aggregateUnaryOperations(op, valueOut, brlen, bclen, indexesIn);
	}
	
	public static void performAggregateBinary(MatrixIndexes indexes1, MatrixValue value1, MatrixIndexes indexes2, MatrixValue value2, 
			MatrixIndexes indexesOut, MatrixValue valueOut, AggregateBinaryOperator op, boolean useNativeBLAS)
	throws DMLRuntimeException
	{
		//compute output index
		indexesOut.setIndexes(indexes1.getRowIndex(), indexes2.getColumnIndex());
		
		//perform on the value
		if( value2 instanceof CompressedMatrixBlock )
			value2.aggregateBinaryOperations(value1, value2, valueOut, op, useNativeBLAS);
		else //default
			value1.aggregateBinaryOperations(indexes1, value1, indexes2, value2, valueOut, op, useNativeBLAS);
	}

	public static MatrixValue performAggregateBinaryIgnoreIndexes(
			MatrixValue value1, MatrixValue value2,
			MatrixValue valueOut, AggregateBinaryOperator op, boolean useNativeBLAS) 
	throws DMLRuntimeException {
			
		//perform on the value
		if( value2 instanceof CompressedMatrixBlock )
			value2.aggregateBinaryOperations(value1, value2, valueOut, op, useNativeBLAS);
		else
			value1.aggregateBinaryOperations(value1, value2, valueOut, op, useNativeBLAS);
		return valueOut;
	}

	@SuppressWarnings("rawtypes")
	public static ArrayList performSlice(IndexRange ixrange, int brlen, int bclen, int iix, int jix, CacheBlock in) 
		throws DMLRuntimeException
	{
		if( in instanceof MatrixBlock )
			return performSlice(ixrange, brlen, bclen, iix, jix, (MatrixBlock)in);
		else if( in instanceof FrameBlock )
			return performSlice(ixrange, brlen, bclen, iix, jix, (FrameBlock)in);
		throw new DMLRuntimeException("Unsupported cache block type: "+in.getClass().getName());
	}
	
	
	@SuppressWarnings("rawtypes")
	public static ArrayList performSlice(IndexRange ixrange, int brlen, int bclen, int iix, int jix, MatrixBlock in) 
			throws DMLRuntimeException
	{
		IndexedMatrixValue imv = new IndexedMatrixValue(new MatrixIndexes(iix, jix), (MatrixBlock)in);
		ArrayList<IndexedMatrixValue> outlist = new ArrayList<IndexedMatrixValue>();
		performSlice(imv, ixrange, brlen, bclen, outlist);
	
		return SparkUtils.fromIndexedMatrixBlockToPair(outlist);
	}

	public static void performSlice(IndexedMatrixValue in, IndexRange ixrange, int brlen, int bclen, ArrayList<IndexedMatrixValue> outlist) 
		throws DMLRuntimeException
	{
		long cellIndexTopRow = UtilFunctions.computeCellIndex(in.getIndexes().getRowIndex(), brlen, 0);
		long cellIndexBottomRow = UtilFunctions.computeCellIndex(in.getIndexes().getRowIndex(), brlen, in.getValue().getNumRows()-1);
		long cellIndexLeftCol = UtilFunctions.computeCellIndex(in.getIndexes().getColumnIndex(), bclen, 0);
		long cellIndexRightCol = UtilFunctions.computeCellIndex(in.getIndexes().getColumnIndex(), bclen, in.getValue().getNumColumns()-1);
		
		long cellIndexOverlapTop = Math.max(cellIndexTopRow, ixrange.rowStart);
		long cellIndexOverlapBottom = Math.min(cellIndexBottomRow, ixrange.rowEnd);
		long cellIndexOverlapLeft = Math.max(cellIndexLeftCol, ixrange.colStart);
		long cellIndexOverlapRight = Math.min(cellIndexRightCol, ixrange.colEnd);
		
		//check if block is outside the indexing range
		if(cellIndexOverlapTop>cellIndexOverlapBottom || cellIndexOverlapLeft>cellIndexOverlapRight) {
			return;
		}
		
		IndexRange tmpRange = new IndexRange(
			UtilFunctions.computeCellInBlock(cellIndexOverlapTop, brlen), 
			UtilFunctions.computeCellInBlock(cellIndexOverlapBottom, brlen), 
			UtilFunctions.computeCellInBlock(cellIndexOverlapLeft, bclen), 
			UtilFunctions.computeCellInBlock(cellIndexOverlapRight, bclen));
		
		int rowCut=UtilFunctions.computeCellInBlock(ixrange.rowStart, brlen);
		int colCut=UtilFunctions.computeCellInBlock(ixrange.colStart, bclen);
		
		int rowsInLastBlock = (int)((ixrange.rowEnd-ixrange.rowStart+1)%brlen);
		if(rowsInLastBlock==0) 
			rowsInLastBlock=brlen;
		int colsInLastBlock = (int)((ixrange.colEnd-ixrange.colStart+1)%bclen);
		if(colsInLastBlock==0) 
			colsInLastBlock=bclen;
		
		long resultBlockIndexTop=UtilFunctions.computeBlockIndex(cellIndexOverlapTop-ixrange.rowStart+1, brlen);
		long resultBlockIndexBottom=UtilFunctions.computeBlockIndex(cellIndexOverlapBottom-ixrange.rowStart+1, brlen);
		long resultBlockIndexLeft=UtilFunctions.computeBlockIndex(cellIndexOverlapLeft-ixrange.colStart+1, bclen);
		long resultBlockIndexRight=UtilFunctions.computeBlockIndex(cellIndexOverlapRight-ixrange.colStart+1, bclen);
		
		int boundaryRlen = brlen;
		int boundaryClen = bclen;
		long finalBlockIndexBottom=UtilFunctions.computeBlockIndex(ixrange.rowEnd-ixrange.rowStart+1, brlen);
		long finalBlockIndexRight=UtilFunctions.computeBlockIndex(ixrange.colEnd-ixrange.colStart+1, bclen);
		if(resultBlockIndexBottom==finalBlockIndexBottom)
			boundaryRlen=rowsInLastBlock;
		if(resultBlockIndexRight==finalBlockIndexRight)
			boundaryClen=colsInLastBlock;
			
		//allocate space for the output value
		for(long r=resultBlockIndexTop; r<=resultBlockIndexBottom; r++)
			for(long c=resultBlockIndexLeft; c<=resultBlockIndexRight; c++)
			{
				IndexedMatrixValue out=new IndexedMatrixValue(new MatrixIndexes(), new MatrixBlock());
				out.getIndexes().setIndexes(r, c);
				outlist.add(out);
			}
		
		//execute actual slice operation
		in.getValue().sliceOperations(outlist, tmpRange, rowCut, colCut, brlen, bclen, boundaryRlen, boundaryClen);
	}

	public static void performShift(IndexedMatrixValue in, IndexRange ixrange, int brlen, int bclen, long rlen, long clen, ArrayList<IndexedMatrixValue> outlist) 
		throws DMLRuntimeException
	{
		MatrixIndexes ix = in.getIndexes();
		MatrixBlock mb = (MatrixBlock)in.getValue();
		
		long start_lhs_globalRowIndex = ixrange.rowStart + (ix.getRowIndex()-1)*brlen;
		long start_lhs_globalColIndex = ixrange.colStart + (ix.getColumnIndex()-1)*bclen;
		long end_lhs_globalRowIndex = start_lhs_globalRowIndex + mb.getNumRows() - 1;
		long end_lhs_globalColIndex = start_lhs_globalColIndex + mb.getNumColumns() - 1;
		
		long start_lhs_rowIndex = UtilFunctions.computeBlockIndex(start_lhs_globalRowIndex, brlen);
		long end_lhs_rowIndex = UtilFunctions.computeBlockIndex(end_lhs_globalRowIndex, brlen);
		long start_lhs_colIndex = UtilFunctions.computeBlockIndex(start_lhs_globalColIndex, bclen);
		long end_lhs_colIndex = UtilFunctions.computeBlockIndex(end_lhs_globalColIndex, bclen);
		
		for(long leftRowIndex = start_lhs_rowIndex; leftRowIndex <= end_lhs_rowIndex; leftRowIndex++) {
			for(long leftColIndex = start_lhs_colIndex; leftColIndex <= end_lhs_colIndex; leftColIndex++) {
				
				// Calculate global index of right hand side block
				long lhs_rl = Math.max((leftRowIndex-1)*brlen+1, start_lhs_globalRowIndex);
				long lhs_ru = Math.min(leftRowIndex*brlen, end_lhs_globalRowIndex);
				long lhs_cl = Math.max((leftColIndex-1)*bclen+1, start_lhs_globalColIndex);
				long lhs_cu = Math.min(leftColIndex*bclen, end_lhs_globalColIndex);
				
				int lhs_lrl = UtilFunctions.computeCellInBlock(lhs_rl, brlen);
				int lhs_lru = UtilFunctions.computeCellInBlock(lhs_ru, brlen);
				int lhs_lcl = UtilFunctions.computeCellInBlock(lhs_cl, bclen);
				int lhs_lcu = UtilFunctions.computeCellInBlock(lhs_cu, bclen);
				
				long rhs_rl = lhs_rl - ixrange.rowStart + 1;
				long rhs_ru = rhs_rl + (lhs_ru - lhs_rl);
				long rhs_cl = lhs_cl - ixrange.colStart + 1;
				long rhs_cu = rhs_cl + (lhs_cu - lhs_cl);
				
				int rhs_lrl = UtilFunctions.computeCellInBlock(rhs_rl, brlen);
				int rhs_lru = UtilFunctions.computeCellInBlock(rhs_ru, brlen);
				int rhs_lcl = UtilFunctions.computeCellInBlock(rhs_cl, bclen);
				int rhs_lcu = UtilFunctions.computeCellInBlock(rhs_cu, bclen);
				
				MatrixBlock slicedRHSBlk = mb.sliceOperations(rhs_lrl, rhs_lru, rhs_lcl, rhs_lcu, new MatrixBlock());
				
				int lbrlen = UtilFunctions.computeBlockSize(rlen, leftRowIndex, brlen);
				int lbclen = UtilFunctions.computeBlockSize(clen, leftColIndex, bclen);
				MatrixBlock resultBlock = new MatrixBlock(lbrlen, lbclen, false);
				resultBlock = resultBlock.leftIndexingOperations(slicedRHSBlk, lhs_lrl, lhs_lru, lhs_lcl, lhs_lcu, null, UpdateType.COPY);
				outlist.add(new IndexedMatrixValue(new MatrixIndexes(leftRowIndex, leftColIndex), resultBlock));
			}
		}
	}

	public static void performMapGroupedAggregate( Operator op, IndexedMatrixValue inTarget, MatrixBlock groups, int ngroups, int brlen, int bclen, ArrayList<IndexedMatrixValue> outlist ) throws DMLRuntimeException
	{
		MatrixIndexes ix = inTarget.getIndexes();
		MatrixBlock target = (MatrixBlock)inTarget.getValue();
		
		//execute grouped aggregate operations
		MatrixBlock out = groups.groupedAggOperations(target, null, new MatrixBlock(), ngroups, op);
		
		if( out.getNumRows()<=brlen && out.getNumColumns()<=bclen )
		{
			//single output block
			outlist.add( new IndexedMatrixValue(new MatrixIndexes(1,ix.getColumnIndex()), out) );	
		}
		else
		{
			//multiple output blocks (by op def, single column block )				
			for(int blockRow = 0; blockRow < (int)Math.ceil(out.getNumRows()/(double)brlen); blockRow++)
			{
				int maxRow = (blockRow*brlen + brlen < out.getNumRows()) ? brlen : out.getNumRows() - blockRow*brlen;			
				int row_offset = blockRow*brlen;

				//copy submatrix to block
				MatrixBlock tmp = out.sliceOperations( row_offset, row_offset+maxRow-1, 
						             0, out.getNumColumns()-1, new MatrixBlock() );
				
				//append block to result cache
				outlist.add(new IndexedMatrixValue(new MatrixIndexes(blockRow+1,ix.getColumnIndex()), tmp));			
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static ArrayList performSlice(IndexRange ixrange, int brlen, int bclen, int iix, int jix, FrameBlock in) 
			throws DMLRuntimeException
	{
		Pair<Long, FrameBlock> lfp = new Pair<Long, FrameBlock>(new Long(((iix-1)*brlen)+1), in);
		ArrayList<Pair<Long, FrameBlock>> outlist = new ArrayList<Pair<Long, FrameBlock>>();
		performSlice(lfp, ixrange, brlen, bclen, outlist);
	
		return outlist;
	}

	
	/**
	 * This function will get slice of the input frame block overlapping in overall slice(Range), slice has requested for.
	 * 
	 * @param in ?
	 * @param ixrange index range
	 * @param brlen number of rows in a block
	 * @param bclen number of columns in a block
	 * @param outlist list of pairs of frame blocks
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public static void performSlice(Pair<Long,FrameBlock> in, IndexRange ixrange, int brlen, int bclen, ArrayList<Pair<Long,FrameBlock>> outlist) 
		throws DMLRuntimeException
	{
		long index = in.getKey();
		FrameBlock block = in.getValue();
		
		// Get Block indexes (rows and columns boundaries)
		long cellIndexTopRow = index;
		long cellIndexBottomRow = index+block.getNumRows()-1;
		long cellIndexLeftCol = 1;
		long cellIndexRightCol = block.getNumColumns();
		
		// Calculate block boundaries with range of slice to be performed (Global index)
		long cellIndexOverlapTop = Math.max(cellIndexTopRow, ixrange.rowStart);
		long cellIndexOverlapBottom = Math.min(cellIndexBottomRow, ixrange.rowEnd);
		long cellIndexOverlapLeft = Math.max(cellIndexLeftCol, ixrange.colStart);
		long cellIndexOverlapRight = Math.min(cellIndexRightCol, ixrange.colEnd);
		
		//check if block is outside the indexing range
		if(cellIndexOverlapTop>cellIndexOverlapBottom || cellIndexOverlapLeft>cellIndexOverlapRight) {
			return;
		}
		
		// Create IndexRange for the slice to be performed on this block.
		IndexRange tmpRange = new IndexRange(
				cellIndexOverlapTop - index,
				cellIndexOverlapBottom - index,
				cellIndexOverlapLeft -1,
				cellIndexOverlapRight - 1);
		
		// Get Top Row and Left column cutting point. 
		int rowCut=(int)(ixrange.rowStart-index);
		
		// Get indices for result block
		long resultBlockIndexTop=UtilFunctions.computeBlockIndex(cellIndexOverlapTop, brlen);
		long resultBlockIndexBottom=UtilFunctions.computeBlockIndex(cellIndexOverlapBottom, brlen);
		
		//allocate space for the output value
		for(long r=resultBlockIndexTop; r<=resultBlockIndexBottom; r++)
		{
			ValueType[] schema = Arrays.copyOfRange(block.getSchema(), (int)tmpRange.colStart, (int)tmpRange.colEnd+1);
			long iResultIndex = Math.max(((r-1)*brlen - ixrange.rowStart + 1), 0);
			Pair<Long,FrameBlock> out=new Pair<Long,FrameBlock>(new Long(iResultIndex+1), new FrameBlock(schema));
			outlist.add(out);
		}
		
		//execute actual slice operation
		block.sliceOperations(outlist, tmpRange, rowCut);
	}

	public static void performShift(Pair<Long,FrameBlock> in, IndexRange ixrange, int brlenLeft, int clenLeft/*, int bclen*/, long rlen, long clen, ArrayList<Pair<Long,FrameBlock>> outlist) 
		throws DMLRuntimeException
	{
		Long ix = in.getKey();
		FrameBlock fb = in.getValue();
		long start_lhs_globalRowIndex = ixrange.rowStart + (ix-1);
		long start_lhs_globalColIndex = ixrange.colStart;
		long end_lhs_globalRowIndex = start_lhs_globalRowIndex + fb.getNumRows() - 1;
		long end_lhs_globalColIndex = ixrange.colEnd;
		
		long start_lhs_rowIndex = UtilFunctions.computeBlockIndex(start_lhs_globalRowIndex, brlenLeft);
		long end_lhs_rowIndex = UtilFunctions.computeBlockIndex(end_lhs_globalRowIndex, brlenLeft);

		for(long leftRowIndex = start_lhs_rowIndex; leftRowIndex <= end_lhs_rowIndex; leftRowIndex++) {
				
			// Calculate global index of right hand side block
			long lhs_rl = Math.max((leftRowIndex-1)*brlenLeft+1, start_lhs_globalRowIndex);
			long lhs_ru = Math.min(leftRowIndex*brlenLeft, end_lhs_globalRowIndex);
			long lhs_cl = start_lhs_globalColIndex;
			long lhs_cu = end_lhs_globalColIndex;
			
			int lhs_lrl = UtilFunctions.computeCellInBlock(lhs_rl, brlenLeft);
			int lhs_lru = UtilFunctions.computeCellInBlock(lhs_ru, brlenLeft);
			int lhs_lcl = (int)lhs_cl-1;
			int lhs_lcu = (int)lhs_cu-1;
			
			long rhs_rl = lhs_rl - (ixrange.rowStart-1) - (ix-1);
			long rhs_ru = rhs_rl + (lhs_ru - lhs_rl);
			long rhs_cl = lhs_cl - ixrange.colStart + 1;
			long rhs_cu = rhs_cl + (lhs_cu - lhs_cl);
			
			// local indices are 0 (zero) based.
			int rhs_lrl = (int) (UtilFunctions.computeCellInBlock(rhs_rl, fb.getNumRows()));
			int rhs_lru = (int) (UtilFunctions.computeCellInBlock(rhs_ru, fb.getNumRows()));
			int rhs_lcl = (int)rhs_cl-1;
			int rhs_lcu = (int)rhs_cu-1;
																				
			FrameBlock slicedRHSBlk = fb.sliceOperations(rhs_lrl, rhs_lru, rhs_lcl, rhs_lcu, new FrameBlock());
			
			int lbclen = clenLeft;
			
			ValueType[] schemaPartialLeft = UtilFunctions.nCopies(lhs_lcl, ValueType.STRING);
			ValueType[] schemaRHS = Arrays.copyOfRange(fb.getSchema(), (int)(rhs_lcl), (int)(rhs_lcl-lhs_lcl+lhs_lcu+1));
			ValueType[] schema = UtilFunctions.copyOf(schemaPartialLeft, schemaRHS);
			ValueType[] schemaPartialRight = UtilFunctions.nCopies(lbclen-schema.length, ValueType.STRING);
			schema = UtilFunctions.copyOf(schema, schemaPartialRight);
			FrameBlock resultBlock = new FrameBlock(schema);
			int iRHSRows = (int)(leftRowIndex<=rlen/brlenLeft?brlenLeft:rlen-(rlen/brlenLeft)*brlenLeft);
			resultBlock.ensureAllocatedColumns(iRHSRows);
			
			resultBlock = resultBlock.leftIndexingOperations(slicedRHSBlk, lhs_lrl, lhs_lru, lhs_lcl, lhs_lcu, new FrameBlock());
			outlist.add(new Pair<Long, FrameBlock>((leftRowIndex-1)*brlenLeft+1, resultBlock));
		}
	}

}
