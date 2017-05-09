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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.LongStream;

import org.apache.commons.math3.random.Well1024a;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.Hop.OpOp2;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.lops.MMTSJ.MMTSJType;
import org.apache.sysml.lops.MapMultChain.ChainType;
import org.apache.sysml.lops.PartialAggregate.CorrectionLocationType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.apache.sysml.runtime.functionobjects.Builtin;
import org.apache.sysml.runtime.functionobjects.CM;
import org.apache.sysml.runtime.functionobjects.CTable;
import org.apache.sysml.runtime.functionobjects.DiagIndex;
import org.apache.sysml.runtime.functionobjects.Divide;
import org.apache.sysml.runtime.functionobjects.KahanFunction;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.functionobjects.KahanPlusSq;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.functionobjects.ReduceAll;
import org.apache.sysml.runtime.functionobjects.ReduceCol;
import org.apache.sysml.runtime.functionobjects.ReduceRow;
import org.apache.sysml.runtime.functionobjects.RevIndex;
import org.apache.sysml.runtime.functionobjects.SortIndex;
import org.apache.sysml.runtime.functionobjects.SwapIndex;
import org.apache.sysml.runtime.instructions.cp.CM_COV_Object;
import org.apache.sysml.runtime.instructions.cp.KahanObject;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.LibMatrixBincell.BinaryAccessType;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.mapred.MRJobConfiguration;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateTernaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.runtime.matrix.operators.CMOperator;
import org.apache.sysml.runtime.matrix.operators.CMOperator.AggregateOperationTypes;
import org.apache.sysml.runtime.matrix.operators.COVOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.QuaternaryOperator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;
import org.apache.sysml.runtime.matrix.operators.UnaryOperator;
import org.apache.sysml.runtime.util.DataConverter;
import org.apache.sysml.runtime.util.FastBufferedDataInputStream;
import org.apache.sysml.runtime.util.FastBufferedDataOutputStream;
import org.apache.sysml.runtime.util.IndexRange;
import org.apache.sysml.runtime.util.UtilFunctions;



public class MatrixBlock extends MatrixValue implements CacheBlock, Externalizable
{
	private static final long serialVersionUID = 7319972089143154056L;
	
	//sparsity nnz threshold, based on practical experiments on space consumption and performance
	public static final double SPARSITY_TURN_POINT = 0.4;
	//sparsity threshold for ultra-sparse matrix operations (40nnz in a 1kx1k block)
	public static final double ULTRA_SPARSITY_TURN_POINT = 0.00004; 
	//default sparse block type: modified compressed sparse rows, for efficient incremental construction
	public static final SparseBlock.Type DEFAULT_SPARSEBLOCK = SparseBlock.Type.MCSR;
	//default sparse block type for update in place: compressed sparse rows, to prevent serialization
	public static final SparseBlock.Type DEFAULT_INPLACE_SPARSEBLOCK = SparseBlock.Type.CSR;
	//basic header (int rlen, int clen, byte type)
	public static final int HEADER_SIZE = 9;
	
	public enum BlockType{
		EMPTY_BLOCK,  
		ULTRA_SPARSE_BLOCK, //ultra sparse representation, in-mem same as sparse
		SPARSE_BLOCK, //sparse representation, see sparseRows 
		DENSE_BLOCK, //dense representation, see denseBlock			
	}
	
	//matrix meta data
	protected int rlen       = -1;
	protected int clen       = -1;
	protected boolean sparse = true;
	protected long nonZeros   = 0;
	
	//matrix data (sparse or dense)
	protected double[] denseBlock    = null;
	protected SparseBlock sparseBlock = null;
		
	//sparse-block-specific attributes (allocation only)
	protected int estimatedNNzsPerRow = -1; 
	
	//grpaggregate-specific attributes (optional)
	protected int numGroups = -1;
	
	//diag-specific attributes (optional)
	protected boolean diag = false;
	
	
	////////
	// Matrix Constructors
	//
	
	public MatrixBlock() {
		this(0, 0, true, -1);
	}
	
	public MatrixBlock(int rl, int cl, boolean sp) {
		this(rl, cl, sp, -1);
	}
	
	public MatrixBlock(int rl, int cl, long estnnz) {
		this(rl, cl, evalSparseFormatInMemory(rl, cl, estnnz), estnnz);
	}
	
	public MatrixBlock(int rl, int cl, boolean sp, long estnnz) {
		reset(rl, cl, sp, estnnz, 0);
	}
	
	public MatrixBlock(MatrixBlock that) {
		copy(that);
	}
	
	/**
	 * Constructs a sparse {@link MatrixBlock} with a given instance of a {@link SparseBlock} 
	 * @param rl number of rows
	 * @param cl number of columns
	 * @param nnz number of non zeroes
	 * @param sblock sparse block
	 */
	public MatrixBlock(int rl, int cl, long nnz, SparseBlock sblock) {
		this(rl, cl, true, nnz);
		nonZeros = nnz;
		sparseBlock = sblock;
	}
	
	public MatrixBlock(MatrixBlock that, SparseBlock.Type stype, boolean deep) {
		this(that.rlen, that.clen, that.sparse);
		
		//sanity check sparse matrix block
		if( !that.isInSparseFormat() )
			throw new RuntimeException("Sparse matrix block expected.");
		
		//deep copy and change sparse block type
		nonZeros = that.nonZeros;
		estimatedNNzsPerRow = that.estimatedNNzsPerRow;
		sparseBlock = SparseBlockFactory
				.copySparseBlock(stype, that.sparseBlock, deep);
	}
	
	////////
	// Initialization methods
	// (reset, init, allocate, etc)
	
	@Override
	public void reset() {
		reset(rlen, clen, sparse, -1, 0);
	}
	
	@Override
	public void reset(int rl, int cl) {
		reset(rl, cl, sparse, -1, 0);
	}
	
	public void reset(int rl, int cl, long estnnz) {
		reset(rl, cl, evalSparseFormatInMemory(rl, cl, estnnz), estnnz, 0);
	}
	
	@Override
	public void reset(int rl, int cl, boolean sp) {
		reset(rl, cl, sp, -1, 0);
	}
	
	@Override
	public void reset(int rl, int cl, boolean sp, long estnnz) {
		reset(rl, cl, sp, estnnz, 0);
	}
	
	@Override
	public void reset(int rl, int cl, double val) {
		reset(rl, cl, false, -1, val);
	}
	
	/**
	 * Internal canonical reset of dense and sparse matrix blocks. 
	 * 
	 * @param rl      number of rows
	 * @param cl      number of columns
	 * @param sp      sparse representation
	 * @param estnnz  estimated number of non-zeros
	 * @param val     initialization value
	 */
	private void reset(int rl, int cl, boolean sp, long estnnz, double val) {
		//reset basic meta data
		rlen = rl;
		clen = cl;
		sparse = (val == 0) ? sp : false;
		nonZeros = (val == 0) ? 0 : rl*cl;		
		estimatedNNzsPerRow = (estnnz < 0 || !sparse) ? -1 :
			(int)Math.ceil((double)estnnz/(double)rlen);
		
		//reset sparse/dense blocks
		if( sparse ) {
			resetSparse();
		}
		else {
			resetDense(val);
		}
		
		//reset operation-specific attributes
		numGroups = -1;
		diag = false;
	}
	
	private void resetSparse() {
		if(sparseBlock == null)
			return;
		sparseBlock.reset(estimatedNNzsPerRow, clen);
	}
	
	private void resetDense(double val) {
		//handle to dense block allocation
		if( denseBlock != null && denseBlock.length<rlen*clen && val==0)
			denseBlock = null;
		else if( val != 0 )
			allocateDenseBlock(false);
			
		//reset dense block to given value 
		if( denseBlock != null )
			Arrays.fill(denseBlock, 0, rlen*clen, val);
	}
	
	/**
	 * NOTE: This method is designed only for dense representation.
	 * 
	 * @param arr 2d double array matrix
	 * @param r number of rows
	 * @param c number of columns
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public void init(double[][] arr, int r, int c) 
		throws DMLRuntimeException 
	{	
		//input checks 
		if ( sparse )
			throw new DMLRuntimeException("MatrixBlockDSM.init() can be invoked only on matrices with dense representation.");
		if( r*c > rlen*clen )
			throw new DMLRuntimeException("MatrixBlockDSM.init() invoked with too large dimensions ("+r+","+c+") vs ("+rlen+","+clen+")");
		
		//allocate or resize dense block
		allocateDenseBlock();
		
		//copy and compute nnz
		for(int i=0, ix=0; i < r; i++, ix+=clen) 
			System.arraycopy(arr[i], 0, denseBlock, ix, arr[i].length);
		recomputeNonZeros();
	}
	
	/**
	 * NOTE: This method is designed only for dense representation.
	 * 
	 * @param arr double array matrix
	 * @param r number of rows
	 * @param c number of columns
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public void init(double[] arr, int r, int c) 
		throws DMLRuntimeException 
	{	
		//input checks 
		if ( sparse )
			throw new DMLRuntimeException("MatrixBlockDSM.init() can be invoked only on matrices with dense representation.");
		if( r*c > rlen*clen )
			throw new DMLRuntimeException("MatrixBlockDSM.init() invoked with too large dimensions ("+r+","+c+") vs ("+rlen+","+clen+")");
		
		//allocate or resize dense block
		allocateDenseBlock();
		
		//copy and compute nnz 
		System.arraycopy(arr, 0, denseBlock, 0, arr.length);
		recomputeNonZeros();
	}

	public boolean isAllocated()
	{
		if( sparse )
			return (sparseBlock!=null);
		else
			return (denseBlock!=null);
	}

	public void allocateDenseBlock() 
		throws RuntimeException 
	{
		allocateDenseBlock( true );
	}

	public void allocateDenseOrSparseBlock() {
		if( sparse )
			allocateSparseRowsBlock();
		else
			allocateDenseBlock();
	}
	
	public void allocateDenseBlock(boolean clearNNZ) 
			throws RuntimeException 
	{
		long limit = (long)rlen * clen;
		
		//check max size constraint (16GB dense), since java arrays are limited to 2^(32-1) elements)
		if( limit > Integer.MAX_VALUE ) {
			String execType = OptimizerUtils.isSparkExecutionMode() ? "SPARK" : "MR";
			throw new RuntimeException("Dense in-memory matrix block ("+rlen+"x"+clen+") exceeds supported size of "+Integer.MAX_VALUE+" elements (16GB). " +
					                   "Please, report this issue and reduce the JVM heapsize to execute this operation in "+execType+".");
		}
		
		//allocate block if non-existing or too small (guaranteed to be 0-initialized),
		if(denseBlock == null || denseBlock.length < limit) {
			denseBlock = new double[(int)limit];
		}
		
		//clear nnz if necessary
		if( clearNNZ ) {
			nonZeros = 0;
		}
		
		sparse = false;
	}

	public void allocateSparseRowsBlock() {
		allocateSparseRowsBlock(true);
	}

	public void allocateSparseRowsBlock(boolean clearNNZ) {
		//allocate block if non-existing or too small (guaranteed to be 0-initialized)
		//but do not replace existing block even if not in default type
		if( sparseBlock == null || sparseBlock.numRows()<rlen ) {
			sparseBlock = SparseBlockFactory.createSparseBlock(DEFAULT_SPARSEBLOCK, rlen);
		}
		
		//clear nnz if necessary
		if( clearNNZ ) {
			nonZeros = 0;
		}
	}
	
	public void allocateAndResetSparseRowsBlock(boolean clearNNZ, SparseBlock.Type stype)
	{
		//allocate block if non-existing or too small (guaranteed to be 0-initialized)
		if( sparseBlock == null || sparseBlock.numRows()<rlen
			|| !SparseBlockFactory.isSparseBlockType(sparseBlock, stype))  {
			sparseBlock = SparseBlockFactory.createSparseBlock(stype, rlen);
		}
		else {
			sparseBlock.reset(estimatedNNzsPerRow, clen);
		}
		
		//clear nnz if necessary
		if( clearNNZ ) {
			nonZeros = 0;
		}
	}
	
	
	/**
	 * This should be called only in the read and write functions for CP
	 * This function should be called before calling any setValueDenseUnsafe()
	 * 
	 * @param rl number of rows
	 * @param cl number of columns
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public void allocateDenseBlockUnsafe(int rl, int cl) 
		throws DMLRuntimeException
	{
		sparse=false;
		rlen=rl;
		clen=cl;
		
		//allocate dense block
		allocateDenseBlock();
	}
	
	
	/**
	 * Allows to cleanup all previously allocated sparserows or denseblocks.
	 * This is for example required in reading a matrix with many empty blocks 
	 * via distributed cache into in-memory list of blocks - not cleaning blocks 
	 * from non-empty blocks would significantly increase the total memory consumption.
	 * 
	 * @param dense if true, set dense block to null
	 * @param sparse if true, set sparse block to null
	 */
	public void cleanupBlock( boolean dense, boolean sparse ) {
		if(dense)
			denseBlock = null;
		if(sparse)
			sparseBlock = null;
	}
	
	////////
	// Metadata information 
	
	public int getNumRows() {
		return rlen;
	}
	
	/**
	 * NOTE: setNumRows() and setNumColumns() are used only in tertiaryInstruction (for contingency tables)
	 * and pmm for meta corrections.
	 * 
	 * @param r number of rows
	 */
	public void setNumRows(int r) {
		rlen = r;
	}
	
	public int getNumColumns() {
		return clen;
	}
	
	public void setNumColumns(int c) {
		clen = c;
	}
	
	public long getNonZeros() {
		return nonZeros;
	}
	
	public void setNonZeros(long nnz) {
		nonZeros = nnz;
	}
	
	public boolean isVector() {
		return (rlen == 1 || clen == 1);
	}
	
	@Override
	public boolean isEmpty() {
		return isEmptyBlock(false);
	}
	
	public boolean isEmptyBlock() {
		return isEmptyBlock(true);
	}
	
	
	public boolean isEmptyBlock(boolean safe)
	{
		boolean ret = false;
		if( sparse && sparseBlock==null )
			ret = true;
		else if( !sparse && denseBlock==null ) 	
			ret = true;
		if( nonZeros==0 )
		{
			//prevent under-estimation
			if(safe)
				recomputeNonZeros();
			ret = (nonZeros==0);
		}
		return ret;
	}
	
	public void setDiag() {
		diag = true;
	}
	
	public boolean isDiag() {
		return diag;
	}
	
	////////
	// Data handling
	
	public double[] getDenseBlock() {
		if( sparse )
			return null;
		return denseBlock;
	}
	
	public SparseBlock getSparseBlock() {
		if( !sparse )
			return null;
		return sparseBlock;
	}

	public Iterator<IJV> getSparseBlockIterator() {
		//check for valid format, should have been checked from outside
		if( !sparse )
			throw new RuntimeException("getSparseBlockInterator should not be called for dense format");
		
		//check for existing sparse block: return empty list
		if( sparseBlock==null )
			return new ArrayList<IJV>().iterator();
		
		//get iterator over sparse block
		if( rlen == sparseBlock.numRows() )
			return sparseBlock.getIterator();
		else
			return sparseBlock.getIterator(rlen);
	}

	public Iterator<IJV> getSparseBlockIterator(int rl, int ru) {
		//check for valid format, should have been checked from outside
		if( !sparse )
			throw new RuntimeException("getSparseBlockInterator should not be called for dense format");
		
		//check for existing sparse block: return empty list
		if( sparseBlock==null )
			return new ArrayList<IJV>().iterator();
		
		//get iterator over sparse block
		return sparseBlock.getIterator(rl, ru);
	}
	
	@Override
	public double getValue(int r, int c) 
	{
		//matrix bounds check 
		if( r >= rlen || c >= clen )
			throw new RuntimeException("indexes ("+r+","+c+") out of range ("+rlen+","+clen+")");
		
		return quickGetValue(r, c);
	}
	
	@Override
	public void setValue(int r, int c, double v) 
	{
		//matrix bounds check 
		if( r >= rlen || c >= clen )
			throw new RuntimeException("indexes ("+r+","+c+") out of range ("+rlen+","+clen+")");

		quickSetValue(r, c, v);
	}

	public double quickGetValue(int r, int c) 
	{
		if(sparse)
		{
			if( sparseBlock==null )
				return 0;
			return sparseBlock.get(r, c);
		}
		else
		{
			if( denseBlock==null )
				return 0;
			return denseBlock[r*clen+c]; 
		}
	}

	public void quickSetValue(int r, int c, double v) 
	{
		if(sparse)
		{
			//early abort
			if( (sparseBlock==null || sparseBlock.isEmpty(r)) && v==0 )
				return;
			
			//allocation on demand
			allocateSparseRowsBlock(false);
			sparseBlock.allocate(r, estimatedNNzsPerRow, clen);
			
			//set value and maintain nnz
			if( sparseBlock.set(r, c, v) )
				nonZeros += (v!=0) ? 1 : -1;
		}
		else
		{
			//early abort
			if( denseBlock==null && v==0 )
				return;		
			
			//allocate and init dense block (w/o overwriting nnz)
			allocateDenseBlock(false);
			
			//set value and maintain nnz
			int index=r*clen+c;
			if( denseBlock[index]==0 )
				nonZeros++;
			denseBlock[index] = v;
			if( v==0 )
				nonZeros--;
		}
	}
	
	public double getValueDenseUnsafe(int r, int c) 
	{
		if(denseBlock==null)
			return 0;
		return denseBlock[r*clen+c]; 
	}
	

	/**
	 * This can be only called when you know you have properly allocated spaces for a dense representation
	 * and r and c are in the the range of the dimension
	 * Note: this function won't keep track of the nozeros
	 * 
	 * @param r row
	 * @param c column
	 * @param v value
	 */
	public void setValueDenseUnsafe(int r, int c, double v) 
	{
		denseBlock[r*clen+c]=v;		
	}
	
	public double getValueSparseUnsafe(int r, int c) 
	{
		if(sparseBlock==null || sparseBlock.isEmpty(r))
			return 0;
		return sparseBlock.get(r, c);	
	}
	
	/**
	 * Append value is only used when values are appended at the end of each row for the sparse representation
	 * This can only be called, when the caller knows the access pattern of the block
	 * 	 
	 * @param r row
	 * @param c column
	 * @param v value
	 */
	public void appendValue(int r, int c, double v)
	{
		//early abort (append guarantees no overwrite)
		if( v == 0 ) 
			return;

		if( !sparse ) //DENSE 
		{
			//allocate on demand (w/o overwriting nnz)
			allocateDenseBlock(false);
			
			//set value and maintain nnz
			denseBlock[r*clen+c] = v;
			nonZeros++;
		}
		else //SPARSE
		{
			//allocation on demand (w/o overwriting nnz)
			allocateSparseRowsBlock(false);
			sparseBlock.allocate(r, estimatedNNzsPerRow, clen);
			
			//set value and maintain nnz
			sparseBlock.append(r, c, v);
			nonZeros++;
		}
	}

	public void appendRow(int r, SparseRow row)
	{
		if(row == null)
			return;
		
		if(sparse) {
			//allocation on demand
			allocateSparseRowsBlock(false);
			sparseBlock.set(r, row, true);
			nonZeros += row.size();
		}
		else {
			int[] cols = row.indexes();
			double[] vals = row.values();
			for(int i=0; i<row.size(); i++)
				quickSetValue(r, cols[i], vals[i]);
		}
	}

	public void appendToSparse( MatrixBlock that, int rowoffset, int coloffset ) {
		appendToSparse(that, rowoffset, coloffset, true);
	}
	
	public void appendToSparse( MatrixBlock that, int rowoffset, int coloffset, boolean deep ) 
	{
		if( that==null || that.isEmptyBlock(false) )
			return; //nothing to append
		
		//init sparse rows if necessary
		allocateSparseRowsBlock(false);
		
		if( that.sparse ) //SPARSE <- SPARSE
		{
			SparseBlock b = that.sparseBlock;
			for( int i=0; i<that.rlen; i++ )
			{
				if( b.isEmpty(i) ) continue;
				int aix = rowoffset+i;
					
				//single block append (avoid re-allocations)
				if( sparseBlock.isEmpty(aix) && coloffset==0
					&& b instanceof SparseBlockMCSR ) { 
					sparseBlock.set(aix, b.get(i), deep);
				}
				else { //general case
					int pos = b.pos(i);
					int len = b.size(i);
					int[] ix = b.indexes(i);
					double[] val = b.values(i);
					sparseBlock.allocate(aix, sparseBlock.size(aix)+len);
					for( int j=pos; j<pos+len; j++ )
						sparseBlock.append(aix, coloffset+ix[j], val[j]);	
				}
			}
		}
		else //SPARSE <- DENSE
		{
			for( int i=0; i<that.rlen; i++ )
			{
				int aix = rowoffset+i;
				for( int j=0, bix=i*that.clen; j<that.clen; j++ )
				{
					double val = that.denseBlock[bix+j];
					if( val != 0 ) {
						//create sparserow only if required
						sparseBlock.allocate(aix, estimatedNNzsPerRow,clen);
						sparseBlock.append(aix, coloffset+j, val);
					}
				}
			}
		}
	}
	
	/**
	 * Sorts all existing sparse rows by column indexes.
	 */
	public void sortSparseRows() {
		if( !sparse || sparseBlock==null )
			return;		
		sparseBlock.sort();
	}
	
	/**
	 * Sorts all existing sparse rows in range [rl,ru) by 
	 * column indexes. 
	 * 
	 * @param rl row lower bound, inclusive 
	 * @param ru row upper bound, exclusive
	 */
	public void sortSparseRows(int rl, int ru) {
		if( !sparse || sparseBlock==null )
			return;		
		for( int i=rl; i<ru; i++ )
			sparseBlock.sort(i);
	}
	
	/**
	 * Utility function for computing the min non-zero value. 
	 * 
	 * @return minimum non-zero value
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public double minNonZero() 
		throws DMLRuntimeException
	{
		//check for empty block and return immediately
		if( isEmptyBlock() )
			return -1;
		
		//NOTE: usually this method is only applied on dense vectors and hence not really tuned yet.
		double min = Double.MAX_VALUE;
		for( int i=0; i<rlen; i++ )
			for( int j=0; j<clen; j++ ){
				double val = quickGetValue(i, j);
				if( val != 0 )
					min = Math.min(min, val);
			}
		
		return min;
	}
	
	/**
	 * Wrapper method for reduceall-min of a matrix.
	 * 
	 * @return ?
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public double min() 
		throws DMLRuntimeException
	{
		//construct operator
		AggregateOperator aop = new AggregateOperator(Double.MAX_VALUE, Builtin.getBuiltinFnObject("min"));
		AggregateUnaryOperator auop = new AggregateUnaryOperator( aop, ReduceAll.getReduceAllFnObject());
		
		//execute operation
		MatrixBlock out = new MatrixBlock(1, 1, false);
		LibMatrixAgg.aggregateUnaryMatrix(this, out, auop);
		
		return out.quickGetValue(0, 0);
	}
	
	/**
	 * Wrapper method for reduceall-max of a matrix.
	 * 
	 * @return ?
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public double max() 
		throws DMLRuntimeException
	{
		//construct operator
		AggregateOperator aop = new AggregateOperator(-Double.MAX_VALUE, Builtin.getBuiltinFnObject("max"));
		AggregateUnaryOperator auop = new AggregateUnaryOperator( aop, ReduceAll.getReduceAllFnObject());
		
		//execute operation
		MatrixBlock out = new MatrixBlock(1, 1, false);
		LibMatrixAgg.aggregateUnaryMatrix(this, out, auop);
		
		return out.quickGetValue(0, 0);
	}
	
	/**
	 * Wrapper method for reduceall-sum of a matrix.
	 * 
	 * @return Sum of the values in the matrix.
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public double sum() 
		throws DMLRuntimeException
	{
		KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
		return sumWithFn(kplus);
	}

	/**
	 * Wrapper method for reduceall-sumSq of a matrix.
	 *
	 * @return Sum of the squared values in the matrix.
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public double sumSq()
			throws DMLRuntimeException
	{
		KahanPlusSq kplusSq = KahanPlusSq.getKahanPlusSqFnObject();
		return sumWithFn(kplusSq);
	}

	/**
	 * Wrapper method for reduceall-sum of a matrix using the given
	 * Kahan function for summation.
	 *
	 * @param kfunc A Kahan function object to use for summation.
	 * @return Sum of the values in the matrix with the given
	 *         function applied.
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	private double sumWithFn(KahanFunction kfunc)
			throws DMLRuntimeException
	{
		//construct operator
		CorrectionLocationType corrLoc = CorrectionLocationType.LASTCOLUMN;
		ReduceAll reduceAllObj = ReduceAll.getReduceAllFnObject();
		AggregateOperator aop = new AggregateOperator(0, kfunc, true, corrLoc);
		AggregateUnaryOperator auop = new AggregateUnaryOperator(aop, reduceAllObj);

		//execute operation
		MatrixBlock out = new MatrixBlock(1, 2, false);
		LibMatrixAgg.aggregateUnaryMatrix(this, out, auop);

		return out.quickGetValue(0, 0);
	}

	////////
	// sparsity handling functions
	
	/**
	 * Returns the current representation (true for sparse).
	 * 
	 * @return true if sparse
	 */
	public boolean isInSparseFormat()
	{
		return sparse;
	}

	public boolean isUltraSparse()
	{
		double sp = ((double)nonZeros/rlen)/clen;
		//check for sparse representation in order to account for vectors in dense
		return sparse && sp<ULTRA_SPARSITY_TURN_POINT && nonZeros<40;
	}

	/**
	 * Evaluates if this matrix block should be in sparse format in
	 * memory. Note that this call does not change the representation - 
	 * for this please call examSparsity.
	 * 
	 * @return true if matrix block should be in sparse format in memory
	 */
	public boolean evalSparseFormatInMemory()
	{
		long lrlen = (long) rlen;
		long lclen = (long) clen;
		long lnonZeros = (long) nonZeros;
			
		//ensure exact size estimates for write
		if( lnonZeros<=0 ) {
			recomputeNonZeros();
			lnonZeros = (long) nonZeros;
		}	
		
		//decide on in-memory representation
		return evalSparseFormatInMemory(lrlen, lclen, lnonZeros);
	}
	
	@SuppressWarnings("unused")
	private boolean evalSparseFormatInMemory(boolean transpose)
	{
		int lrlen = (transpose) ? clen : rlen;
		int lclen = (transpose) ? rlen : clen;
		long lnonZeros = (long) nonZeros;
		
		//ensure exact size estimates for write
		if( lnonZeros<=0 ) {
			recomputeNonZeros();
			lnonZeros = (long) nonZeros;
		}	
		
		//decide on in-memory representation
		return evalSparseFormatInMemory(lrlen, lclen, lnonZeros);
	}
	
	/**
	 * Evaluates if this matrix block should be in sparse format on
	 * disk. This applies to any serialized matrix representation, i.e.,
	 * when writing to in-memory buffer pool pages or writing to local fs
	 * or hdfs. 
	 * 
	 * @return true if matrix block should be in sparse format on disk
	 */
	public boolean evalSparseFormatOnDisk()
	{
		long lrlen = (long) rlen;
		long lclen = (long) clen;

		//ensure exact size estimates for write
		if( nonZeros <= 0 ) {
			recomputeNonZeros();
		}	
		
		//decide on in-memory representation
		return evalSparseFormatOnDisk(lrlen, lclen, nonZeros);
	}
	
	/**
	 * Evaluates if this matrix block should be in sparse format in
	 * memory. Depending on the current representation, the state of the
	 * matrix block is changed to the right representation if necessary. 
	 * Note that this consumes for the time of execution memory for both 
	 * representations.  
	 * 
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public void examSparsity() 
		throws DMLRuntimeException
	{
		//determine target representation
		boolean sparseDst = evalSparseFormatInMemory(); 
				
		//check for empty blocks (e.g., sparse-sparse)
		if( isEmptyBlock(false) )
			cleanupBlock(true, true);
		
		//change representation if required (also done for 
		//empty blocks in order to set representation flags)
		if( sparse && !sparseDst)
			sparseToDense();
		else if( !sparse && sparseDst )
			denseToSparse();
	}
	
	/**
	 * Evaluates if a matrix block with the given characteristics should be in sparse format 
	 * in memory.
	 * 
	 * @param nrows number of rows
	 * @param ncols number of columns
	 * @param nnz number of non-zeros
	 * @return true if matrix block shold be in sparse format in memory
	 */
	public static boolean evalSparseFormatInMemory( final long nrows, final long ncols, final long nnz )
	{				
		//evaluate sparsity threshold
		double lsparsity = (double)nnz/nrows/ncols;
		boolean lsparse = (lsparsity < SPARSITY_TURN_POINT);
		
		//compare size of sparse and dense representation in order to prevent
		//that the sparse size exceed the dense size since we use the dense size
		//as worst-case estimate if unknown (and it requires less io from 
		//main memory).
		double sizeSparse = estimateSizeSparseInMemory(nrows, ncols, lsparsity);
		double sizeDense = estimateSizeDenseInMemory(nrows, ncols);
		
		return lsparse && (sizeSparse<sizeDense);
	}
	
	/**
	 * Evaluates if a matrix block with the given characteristics should be in sparse format 
	 * on disk (or in any other serialized representation).
	 * 
	 * @param nrows number of rows
	 * @param ncols number of columns
	 * @param nnz number of non-zeros
	 * @return true if matrix block shold be in sparse format on disk
	 */
	public static boolean evalSparseFormatOnDisk( final long nrows, final long ncols, final long nnz )
	{
		//evaluate sparsity threshold
		double lsparsity = ((double)nnz/nrows)/ncols;
		boolean lsparse = (lsparsity < SPARSITY_TURN_POINT);
		
		double sizeUltraSparse = estimateSizeUltraSparseOnDisk( nrows, ncols, nnz );
		double sizeSparse = estimateSizeSparseOnDisk(nrows, ncols, nnz);
		double sizeDense = estimateSizeDenseOnDisk(nrows, ncols);
		
		return lsparse && (sizeSparse<sizeDense || sizeUltraSparse<sizeDense);		
	}
	
	
	////////
	// basic block handling functions	

	void denseToSparse() 
	{	
		//set target representation
		sparse = true;
		
		//early abort on empty blocks
		if(denseBlock==null)
			return;
		
		//allocate sparse target block (reset required to maintain nnz again)
		allocateSparseRowsBlock();
		reset();
		
		//copy dense to sparse with (1) row pre-allocation to avoid repeated 
		//allocation on append, and (2) nnz re-computation 
		double[] a = denseBlock;
		SparseBlock c = sparseBlock;
		final int m = rlen;
		final int n = clen;
		
		long nnz = 0;
		for( int i=0, aix=0; i<m; i++, aix+=n ) {
			//recompute nnz per row (not via recomputeNonZeros as sparse allocated)
			int lnnz = 0;
			for(int j=0; j<n; j++)
				lnnz += (a[aix+j]!=0) ? 1 : 0;
			if( lnnz <= 0 ) continue;
			
			//allocate sparse row and append non-zero values
			c.allocate(i, lnnz); 
			for(int j=0; j<n; j++) {
				double val = a[aix+j];
				if( val != 0 )
					c.append(i, j, val);
			}
			nnz += lnnz;
		}
				
		//update nnz and cleanup dense block
		nonZeros = nnz;
		denseBlock = null;
	}

	public void sparseToDense() 
		throws DMLRuntimeException 
	{	
		//set target representation
		sparse = false;
		
		//early abort on empty blocks
		if(sparseBlock==null)
			return;
		
		int limit=rlen*clen;
		if ( limit < 0 ) {
			throw new DMLRuntimeException("Unexpected error in sparseToDense().. limit < 0: " + rlen + ", " + clen + ", " + limit);
		}
		
		//allocate dense target block, but keep nnz (no need to maintain)
		allocateDenseBlock(false);
		Arrays.fill(denseBlock, 0, limit, 0);
		
		//copy sparse to dense
		SparseBlock a = sparseBlock;
		double[] c = denseBlock;
		
		for( int i=0, cix=0; i<rlen; i++, cix+=clen)
			if( !a.isEmpty(i) ) {
				int apos = a.pos(i);
				int alen = a.size(i);
				int[] aix = a.indexes(i);
				double[] avals = a.values(i);
				for(int j=apos; j<apos+alen; j++)
					if( avals[j] != 0 )
						c[ cix+aix[j] ] = avals[j];
			}
		
		//cleanup sparse rows
		sparseBlock = null;
	}

	/**
	 * Recomputes and materializes the number of non-zero values
	 * of the entire matrix block.
	 * 
	 */
	public void recomputeNonZeros()
	{
		if( sparse && sparseBlock!=null ) //SPARSE (max long)
		{
			//note: rlen might be <= sparseBlock.numRows()
			nonZeros = sparseBlock.size(0, sparseBlock.numRows());
		}
		else if( !sparse && denseBlock!=null ) //DENSE (max int)
		{
			double[] a = denseBlock;
			final int limit=rlen*clen;
			int nnz = 0;
			for(int i=0; i<limit; i++)
				nnz += (a[i]!=0) ? 1 : 0;
			nonZeros = nnz;
		}
	}
	
	/**
	 * Recomputes the number of non-zero values of a specified 
	 * range of the matrix block. NOTE: This call does not materialize
	 * the compute result in any form.
	 * 
	 * @param rl 	row lower index, 0-based, inclusive
	 * @param ru 	row upper index, 0-based, inclusive
	 * @param cl 	column lower index, 0-based, inclusive
	 * @param cu 	column upper index, 0-based, inclusive
	 * @return the number of non-zero values
	 */
	public long recomputeNonZeros(int rl, int ru, int cl, int cu)
	{
		if( sparse && sparseBlock!=null ) //SPARSE (max long)
		{
			long nnz = 0;
			if( cl==0 && cu==clen-1 ) { //specific case: all cols
				nnz = sparseBlock.size(rl, ru+1);
			}
			else if( cl==cu ) { //specific case: one column
				final int rlimit = Math.min( ru+1, rlen);
				for(int i=rl; i<rlimit; i++)
					if(!sparseBlock.isEmpty(i))
						nnz += (sparseBlock.get(i, cl)!=0) ? 1 : 0;
			}
			else { //general case
				nnz = sparseBlock.size(rl, ru+1, cl, cu+1);
			}
			return nnz;
		}
		else if( !sparse && denseBlock!=null ) //DENSE (max int)
		{
			double[] a = denseBlock;
			final int n = clen;
			int nnz = 0;
			if( cl==0 && cu==n-1 ) { //specific case: all cols
				for( int i=rl*n; i<(ru+1)*n; i++ )
					nnz += (a[i]!=0) ? 1 : 0;
			}
			else {
				for( int i=rl, ix=rl*n; i<=ru; i++, ix+=n )
					for( int j=cl; j<=cu; j++ )
						nnz += (a[ix+j]!=0) ? 1 : 0;
			}
			return nnz;
		}
		
		return 0; //empty block
	}
	
	/**
	 * Basic debugging primitive to check correctness of nnz.
	 * This method is not intended for production use.
	 */
	public void checkNonZeros() {
		//take non-zeros before and after recompute nnz
		long nnzBefore = getNonZeros();
		recomputeNonZeros();
		long nnzAfter = getNonZeros();
		
		//raise exception if non-zeros don't match up
		if( nnzBefore != nnzAfter )
			throw new RuntimeException("Number of non zeros incorrect: "+nnzBefore+" vs "+nnzAfter);
	}
	
	/**
	 * Basic debugging primitive to check sparse block column ordering.
	 * This method is not intended for production use. 
	 */
	public void checkSparseRows() {
		if( !sparse || sparseBlock == null )
			return;
		
		//check ordering of column indexes per sparse row
		for( int i=0; i<rlen; i++ )
			if( !sparseBlock.isEmpty(i) ) {
				int apos = sparseBlock.pos(i);
				int alen = sparseBlock.size(i);
				int[] aix = sparseBlock.indexes(i);
				for( int k=apos+1; k<apos+alen; k++ )
					if( aix[k-1] >= aix[k] )
						throw new RuntimeException("Wrong sparse row ordering: "+k+" "+aix[k-1]+" "+aix[k]);
			}
	}

	@Override
	public void copy(MatrixValue thatValue) 
	{
		MatrixBlock that = checkType(thatValue);
		
		//copy into automatically determined representation
		copy( that, that.evalSparseFormatInMemory() );
	}
	
	@Override
	public void copy(MatrixValue thatValue, boolean sp) 
	{
		MatrixBlock that = checkType(thatValue);
		
		if( this == that ) //prevent data loss (e.g., on sparse-dense conversion)
			throw new RuntimeException( "Copy must not overwrite itself!" );
		
		this.rlen=that.rlen;
		this.clen=that.clen;
		this.sparse=sp;
		estimatedNNzsPerRow=(int)Math.ceil((double)thatValue.getNonZeros()/(double)rlen);
		if(this.sparse && that.sparse)
			copySparseToSparse(that);
		else if(this.sparse && !that.sparse)
			copyDenseToSparse(that);
		else if(!this.sparse && that.sparse)
			copySparseToDense(that);
		else
			copyDenseToDense(that);
	}
	
	private void copySparseToSparse(MatrixBlock that)
	{
		this.nonZeros=that.nonZeros;
		if( that.isEmptyBlock(false) ) {
			resetSparse();
			return;
		}
	
		allocateSparseRowsBlock(false);
		for(int i=0; i<Math.min(that.sparseBlock.numRows(), rlen); i++)
		{
			if(!that.sparseBlock.isEmpty(i)) {
				sparseBlock.set(i, that.sparseBlock.get(i), true);				
			}
			else if(!this.sparseBlock.isEmpty(i)) {
				this.sparseBlock.reset(i,estimatedNNzsPerRow, clen);
			}
		}
	}
	
	private void copyDenseToDense(MatrixBlock that)
	{
		nonZeros = that.nonZeros;
		int limit = rlen*clen;
		
		//plain reset to 0 for empty input
		if( that.isEmptyBlock(false) )
		{
			if(denseBlock!=null)
				Arrays.fill(denseBlock, 0, limit, 0);
			return;
		}
		
		//allocate and init dense block (w/o overwriting nnz)
		allocateDenseBlock(false);
		
		//actual copy 
		System.arraycopy(that.denseBlock, 0, denseBlock, 0, limit);
	}
	
	private void copySparseToDense(MatrixBlock that)
	{
		this.nonZeros=that.nonZeros;
		if( that.isEmptyBlock(false) )
		{
			if(denseBlock!=null)
				Arrays.fill(denseBlock, 0);
			return;
		}
		
		//allocate and init dense block (w/o overwriting nnz)
		allocateDenseBlock(false);
		
		int start=0;
		for(int r=0; r<Math.min(that.sparseBlock.numRows(), rlen); r++, start+=clen)
		{
			if(that.sparseBlock.isEmpty(r)) 
				continue;
			int pos = that.sparseBlock.pos(r);
			int len = that.sparseBlock.size(r);
			int[] aix = that.sparseBlock.indexes(r);
			double[] avals = that.sparseBlock.values(r);
			
			for(int i=pos; i<pos+len; i++) {
				denseBlock[start+aix[i]]=avals[i];
			}
		}
	}
	
	private void copyDenseToSparse(MatrixBlock that)
	{
		nonZeros = that.nonZeros;
		if( that.isEmptyBlock(false) )
		{
			resetSparse();
			return;
		}
		
		allocateSparseRowsBlock(false);
	
		for(int i=0, ix=0; i<rlen; i++)
		{			
			sparseBlock.reset(i, estimatedNNzsPerRow, clen);
			
			for(int j=0; j<clen; j++)
			{
				double val = that.denseBlock[ix++];
				if( val != 0 ) {
					//create sparse row only if required
					sparseBlock.allocate(i, estimatedNNzsPerRow, clen);
					sparseBlock.append(i, j, val);
				}
			}
		}
	}
	
	
	/**
	 * In-place copy of matrix src into the index range of the existing current matrix.
	 * Note that removal of existing nnz in the index range and nnz maintenance is 
	 * only done if 'awareDestNZ=true', 
	 * 
	 * @param rl row lower index, 0-based
	 * @param ru row upper index, 0-based
	 * @param cl column lower index, 0-based
	 * @param cu column upper index, 0-based
	 * @param src matrix block
	 * @param awareDestNZ
	 *           true, forces (1) to remove existing non-zeros in the index range of the 
	 *                 destination if not present in src and (2) to internally maintain nnz
	 *           false, assume empty index range in destination and do not maintain nnz
	 *                  (the invoker is responsible to recompute nnz after all copies are done) 
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public void copy(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ ) 
		throws DMLRuntimeException 
	{	
		if(sparse && src.sparse)
			copySparseToSparse(rl, ru, cl, cu, src, awareDestNZ);
		else if(sparse && !src.sparse)
			copyDenseToSparse(rl, ru, cl, cu, src, awareDestNZ);
		else if(!sparse && src.sparse)
			copySparseToDense(rl, ru, cl, cu, src, awareDestNZ);
		else
			copyDenseToDense(rl, ru, cl, cu, src, awareDestNZ);
	}

	private void copySparseToSparse(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ)
	{	
		//handle empty src and dest
		if( src.isEmptyBlock(false) )
		{
			if( awareDestNZ && sparseBlock != null )
				copyEmptyToSparse(rl, ru, cl, cu, true);
			return;		
		}
		
		if(sparseBlock==null)
			allocateSparseRowsBlock(false);
		else if( awareDestNZ ) {
			copyEmptyToSparse(rl, ru, cl, cu, true);
			//explicit clear if awareDestNZ because more efficient since
			//src will have multiple columns and only few overwriting values
		}
		
		SparseBlock a = src.sparseBlock;
		SparseBlock b = sparseBlock;
		
		//copy values
		for( int i=0; i<src.rlen; i++ )
		{
			if( !a.isEmpty(i) )
			{
				int apos = a.pos(i); 
				int alen = a.size(i);
				int[] aix = a.indexes(i);
				double[] avals = a.values(i);		
				
				if( b.isEmpty(rl+i)  )
				{
					b.allocate(rl+i, estimatedNNzsPerRow, clen);
					for( int j=apos; j<apos+alen; j++ )
						b.append(rl+i, cl+aix[j], avals[j]);
					
					if( awareDestNZ )
						nonZeros += b.size(rl+i);
				}
				else if( awareDestNZ ) //general case (w/ awareness NNZ)
				{
					int lnnz = b.size(rl+i);
					if( cl==cu && cl==aix[apos] ) {
						b.set(rl+i, cl, avals[apos] );
					}
					else {
						//TODO perf sparse row
						b.deleteIndexRange(rl+i, cl, cu+1);
						for( int j=apos; j<apos+alen; j++ )
							b.set(rl+i, cl+aix[j], avals[j]);
					}
					nonZeros += (b.size(rl+i) - lnnz);
				}	
				else //general case (w/o awareness NNZ)
				{		
					//TODO perf sparse row					
					for( int j=apos; j<apos+alen; j++ )
						b.set(rl+i, cl+aix[j], avals[j]);
				}				
			}
		}
	}
	
	private void copySparseToDense(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ) 
		throws DMLRuntimeException
	{	
		//handle empty src and dest
		if( src.isEmptyBlock(false) ) {
			if( awareDestNZ && denseBlock != null ) {
				nonZeros -= recomputeNonZeros(rl, ru, cl, cu);
				copyEmptyToDense(rl, ru, cl, cu);
			}
			return;		
		}
		if(denseBlock==null)
			allocateDenseBlock();
		else if( awareDestNZ ) {
			nonZeros -= recomputeNonZeros(rl, ru, cl, cu);
			copyEmptyToDense(rl, ru, cl, cu);
		}

		//copy values
		SparseBlock a = src.sparseBlock;
		for( int i=0, ix=rl*clen; i<src.rlen; i++, ix+=clen )
		{	
			if( !a.isEmpty(i) )
			{
				int apos = a.pos(i);
				int alen = a.size(i);
				int[] aix = a.indexes(i);
				double[] avals = a.values(i);
				
				for( int j=apos; j<apos+alen; j++ )
					denseBlock[ix+cl+aix[j]] = avals[j];
				
				if(awareDestNZ)
					nonZeros += alen;
			}
		}
	}

	private void copyDenseToSparse(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ)
	{
		//handle empty src and dest
		if( src.isEmptyBlock(false) ) {
			if( awareDestNZ && sparseBlock != null )
				copyEmptyToSparse(rl, ru, cl, cu, true);
			return;		
		}
		
		//allocate output block
		//no need to clear for awareDestNZ since overwritten  
		allocateSparseRowsBlock(false);
		
		//copy values
		SparseBlock a = sparseBlock;
		for( int i=0, ix=0; i<src.rlen; i++, ix+=src.clen )
		{
			int rix = rl + i;
			if( a instanceof SparseBlockMCSR 
				&& a.isEmpty(rix) ) //special case MCSR append
			{
				//count nnz per row (fits likely in L1 cache)
				int lnnz = 0;
				for( int j=0; j<src.clen; j++ )
					lnnz += (src.denseBlock[ix+j]!=0) ? 1 : 0;
					
				//allocate row once and copy values
				if( lnnz > 0 ) {	
					a.allocate(rix, lnnz);
					for( int j=0; j<src.clen; j++ ) {
						double val = src.denseBlock[ix+j];
						if( val != 0 )
							a.append(rix, cl+j, val); 
					}
					if( awareDestNZ )
						nonZeros += lnnz;
				}
			}
			else if( awareDestNZ ) //general case (w/ awareness NNZ)
			{
				int lnnz = a.size(rix);
				if( cl==cu ) {
					double val = src.denseBlock[ix];
					a.set(rix, cl, val);
				}
				else {
					a.setIndexRange(rix, cl, cu+1, src.denseBlock, ix, src.clen);
				}
				nonZeros += (a.size(rix) - lnnz);
			}	
			else //general case (w/o awareness NNZ)
			{
				for( int j=0; j<src.clen; j++ ) {
					double val = src.denseBlock[ix+j];
					if( val != 0 ) 
						a.set(rix, cl+j, val);
				}
			}
		}
	}
	
	private void copyDenseToDense(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ) 
		throws DMLRuntimeException
	{	
		//handle empty src and dest
		if( src.isEmptyBlock(false) ) {
			if( awareDestNZ && denseBlock != null ) {
				nonZeros -= recomputeNonZeros(rl, ru, cl, cu);
				copyEmptyToDense(rl, ru, cl, cu);
			}
			return;		
		}
		
		//allocate output block
		//no need to clear for awareDestNZ since overwritten 
		allocateDenseBlock(false);
		
		if( awareDestNZ )
			nonZeros = nonZeros - recomputeNonZeros(rl, ru, cl, cu) + src.nonZeros;
		
		//copy values
		int rowLen = cu-cl+1;				
		if(clen == src.clen) //optimization for equal width
			System.arraycopy(src.denseBlock, 0, denseBlock, rl*clen+cl, src.rlen*src.clen);
		else
			for( int i=0, ix1=0, ix2=rl*clen+cl; i<src.rlen; i++, ix1+=src.clen, ix2+=clen ) {
				System.arraycopy(src.denseBlock, ix1, denseBlock, ix2, rowLen);
			}
	}
	
	private void copyEmptyToSparse(int rl, int ru, int cl, int cu, boolean updateNNZ ) 
	{
		SparseBlock a = sparseBlock;
		
		if( cl==cu ) //specific case: column vector
		{
			for( int i=rl; i<=ru; i++ )
				if( !a.isEmpty(i) ) {
					boolean update = a.set(i, cl, 0);
					if( updateNNZ )
						nonZeros -= update ? 1 : 0;							
				}			
		}
		else
		{
			for( int i=rl; i<=ru; i++ )
				if( !a.isEmpty(i) ) {
					int lnnz = a.size(i);
					a.deleteIndexRange(i, cl, cu+1);
					if( updateNNZ )
						nonZeros += (a.size(i)-lnnz);
				}	
		}
	}
	
	private void copyEmptyToDense(int rl, int ru, int cl, int cu)
	{
		int rowLen = cu-cl+1;				
		if(clen == rowLen) //optimization for equal width
			Arrays.fill(denseBlock, rl*clen+cl, ru*clen+cu+1, 0);
		else
			for( int i=rl, ix2=rl*clen+cl; i<=ru; i++, ix2+=clen )
				Arrays.fill(denseBlock, ix2, ix2+rowLen, 0);
	}

	public void merge(CacheBlock that, boolean appendOnly) 
			throws DMLRuntimeException
	{
		merge((MatrixBlock)that, appendOnly);
	}

	
	/**
	 * Merge disjoint: merges all non-zero values of the given input into the current
	 * matrix block. Note that this method does NOT check for overlapping entries;
	 * it's the callers reponsibility of ensuring disjoint matrix blocks.  
	 * 
	 * The appendOnly parameter is only relevant for sparse target blocks; if true,
	 * we only append values and do not sort sparse rows for each call; this is useful
	 * whenever we merge iterators of matrix blocks into one target block.
	 * 
	 * @param that matrix block
	 * @param appendOnly ?
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public void merge(MatrixBlock that, boolean appendOnly) 
		throws DMLRuntimeException
	{
		//check for empty input source (nothing to merge)
		if( that == null || that.isEmptyBlock(false) )
			return;
		
		//check dimensions (before potentially copy to prevent implicit dimension change) 
		//this also does a best effort check for disjoint input blocks via the number of non-zeros
		if( rlen != that.rlen || clen != that.clen )
			throw new DMLRuntimeException("Dimension mismatch on merge disjoint (target="+rlen+"x"+clen+", source="+that.rlen+"x"+that.clen+")");
		if( (long)nonZeros+ that.nonZeros > (long)rlen*clen )
			throw new DMLRuntimeException("Number of non-zeros mismatch on merge disjoint (target="+rlen+"x"+clen+", nnz target="+nonZeros+", nnz source="+that.nonZeros+")");
		
		//check for empty target (copy in full)
		if( isEmptyBlock(false) ) {
			copy(that);
			return;
		}
		
		//core matrix block merge (guaranteed non-empty source/target, nnz maintenance not required)
		long nnz = nonZeros + that.nonZeros;
		if( sparse )
			mergeIntoSparse(that, appendOnly);
		else
			mergeIntoDense(that);	
		
		//maintain number of nonzeros
		nonZeros = nnz;
	}

	private void mergeIntoDense(MatrixBlock that)
	{
		if( that.sparse ) //DENSE <- SPARSE
		{
			double[] a = denseBlock;
			SparseBlock b = that.sparseBlock;
			int m = rlen;
			int n = clen;
			
			for( int i=0, aix=0; i<m; i++, aix+=n )
				if( !b.isEmpty(i) )
				{
					int bpos = b.pos(i);
					int blen = b.size(i);
					int[] bix = b.indexes(i);
					double[] bval = b.values(i);
					for( int j=bpos; j<bpos+blen; j++ )
						if( bval[j] != 0 )
							a[ aix + bix[j] ] = bval[j];
				}
		}
		else //DENSE <- DENSE
		{
			double[] a = denseBlock;
			double[] b = that.denseBlock;
			int len = rlen * clen;
			
			for( int i=0; i<len; i++ )
				a[i] = ( b[i] != 0 ) ? b[i] : a[i];
		}
	}

	private void mergeIntoSparse(MatrixBlock that, boolean appendOnly)
	{
		if( that.sparse ) //SPARSE <- SPARSE
		{
			SparseBlock a = sparseBlock;
			SparseBlock b = that.sparseBlock;
			int m = rlen;
			
			for( int i=0; i<m; i++ ) 
			{
				if( !b.isEmpty(i) )
				{
					if( a.isEmpty(i) ) {
						//copy entire sparse row (no sort required)
						a.set(i, b.get(i), true); 
					}
					else
					{
						boolean appended = false;
						int bpos = b.pos(i);
						int blen = b.size(i);
						int[] bix = b.indexes(i);
						double[] bval = b.values(i);
						for( int j=bpos; j<bpos+blen; j++ ) {
							if( bval[j] != 0 ) {
								a.append(i, bix[j], bval[j]);
								appended = true;
							}
						}
						//only sort if value appended
						if( !appendOnly && appended )
							a.sort(i);		
					}
				}
			}
		}
		else //SPARSE <- DENSE
		{
			SparseBlock a = sparseBlock;
			double[] b = that.denseBlock;
			int m = rlen;
			int n = clen;
			
			for( int i=0, bix=0; i<m; i++, bix+=n )
			{
				boolean appended = false;
				for( int j=0; j<n; j++ ) {
					if( b[bix+j] != 0 ) {
						appendValue(i, j, b[bix+j]);
						appended = true;
					}
				}
				//only sort if value appended
				if( !appendOnly && appended )
					a.sort(i);
			}
		}
	}
	
	////////
	// Input/Output functions
	
	@Override
	public void readFields(DataInput in) 
		throws IOException 
	{
		//read basic header (int rlen, int clen, byte type)
		rlen = in.readInt();
		clen = in.readInt();
		byte bformat = in.readByte();
		
		//check type information
		if( bformat<0 || bformat>=BlockType.values().length )
			throw new IOException("invalid format: '"+bformat+"' (need to be 0-"+BlockType.values().length+").");
		
		BlockType format=BlockType.values()[bformat];
		try 
		{
			switch(format)
			{
				case ULTRA_SPARSE_BLOCK:
					nonZeros = readNnzInfo( in, true );
					sparse = evalSparseFormatInMemory(rlen, clen, nonZeros);
					cleanupBlock(true, !(sparse && sparseBlock instanceof SparseBlockCSR));
					if( sparse )
						readUltraSparseBlock(in);
					else
						readUltraSparseToDense(in);
					break;
				case SPARSE_BLOCK:
					nonZeros = readNnzInfo( in, false );
					sparse = evalSparseFormatInMemory(rlen, clen, nonZeros);
					cleanupBlock(sparse, !sparse); 
					if( sparse )
						readSparseBlock(in);
					else
						readSparseToDense(in);
					break;
				case DENSE_BLOCK:
					sparse = false;
					cleanupBlock(false, true); //reuse dense
					readDenseBlock(in); //always dense in-mem if dense on disk
					break;
				case EMPTY_BLOCK:
					sparse = true;
					cleanupBlock(true, !(sparseBlock instanceof SparseBlockCSR));
					if( sparseBlock != null )
						sparseBlock.reset();
					nonZeros = 0;
					break;
			}
		}
		catch(DMLRuntimeException ex)
		{
			throw new IOException("Error reading block of type '"+format.toString()+"'.", ex);
		}
	}

	private void readDenseBlock(DataInput in) 
		throws IOException, DMLRuntimeException 
	{
		allocateDenseBlock(true); //allocate block, clear nnz
		
		int limit = rlen*clen;
		
		if( in instanceof MatrixBlockDataInput ) //fast deserialize
		{
			MatrixBlockDataInput mbin = (MatrixBlockDataInput)in;
			nonZeros = mbin.readDoubleArray(limit, denseBlock);
		}
		else if( in instanceof DataInputBuffer && MRJobConfiguration.USE_BINARYBLOCK_SERIALIZATION ) 
		{
			//workaround because sequencefile.reader.next(key, value) does not yet support serialization framework
			DataInputBuffer din = (DataInputBuffer)in;
			FastBufferedDataInputStream mbin = null;
			try {
				mbin = new FastBufferedDataInputStream(din);
				nonZeros = mbin.readDoubleArray(limit, denseBlock);	
			}
			finally {
				IOUtilFunctions.closeSilently(mbin);
			}
		}
		else //default deserialize
		{
			for( int i=0; i<limit; i++ )
			{
				denseBlock[i]=in.readDouble();
				if(denseBlock[i]!=0)
					nonZeros++;
			}
		}
	}

	private void readSparseBlock(DataInput in) 
		throws IOException 
	{			
		allocateSparseRowsBlock(false); 
		resetSparse(); //reset all sparse rows
		
		if( in instanceof MatrixBlockDataInput ) //fast deserialize
		{
			MatrixBlockDataInput mbin = (MatrixBlockDataInput)in;
			nonZeros = mbin.readSparseRows(rlen, sparseBlock);
		}
		else if( in instanceof DataInputBuffer  && MRJobConfiguration.USE_BINARYBLOCK_SERIALIZATION ) 
		{
			//workaround because sequencefile.reader.next(key, value) does not yet support serialization framework
			DataInputBuffer din = (DataInputBuffer)in;
			FastBufferedDataInputStream mbin = null;
			try {
				mbin = new FastBufferedDataInputStream(din);
				nonZeros = mbin.readSparseRows(rlen, sparseBlock);	
			}
			finally {
				IOUtilFunctions.closeSilently(mbin);
			}
		}
		else //default deserialize
		{
			for(int r=0; r<rlen; r++) {
				int rnnz = in.readInt(); //row nnz
				if( rnnz > 0 ) {
					sparseBlock.reset(r, rnnz, clen);
					for(int j=0; j<rnnz; j++) //col index/value pairs
						sparseBlock.append(r, in.readInt(), in.readDouble());		
				}
			}
		}
	}

	private void readSparseToDense(DataInput in) 
		throws IOException, DMLRuntimeException 
	{
		allocateDenseBlock(false); //allocate block
		Arrays.fill(denseBlock, 0);
		
		for(int r=0; r<rlen; r++)
		{
			int nr = in.readInt();
			for( int j=0; j<nr; j++ )
			{
				int c = in.readInt();
				double val = in.readDouble(); 
				denseBlock[r*clen+c] = val;
			}
		}
	}

	private void readUltraSparseBlock(DataInput in) 
		throws IOException 
	{
		//allocate ultra-sparse block in CSR to avoid unnecessary size overhead 
		//and to allow efficient reset without repeated sparse row allocation
		
		//adjust size and ensure reuse block is in CSR format
		allocateAndResetSparseRowsBlock(false, SparseBlock.Type.CSR);
		
		if( clen > 1 ) //ULTRA-SPARSE BLOCK
		{ 
			//block: read ijv-triples (ordered by row and column) via custom 
			//init to avoid repeated updates of row pointers per append
			SparseBlockCSR sblockCSR = (SparseBlockCSR) sparseBlock;
			sblockCSR.initUltraSparse((int)nonZeros, in);
		}
		else //ULTRA-SPARSE COL
		{
			//col: read iv-pairs (should never happen since always dense)
			for(long i=0; i<nonZeros; i++) {
				int r = in.readInt();
				double val = in.readDouble();		
				sparseBlock.allocate(r, 1, 1);
				sparseBlock.append(r, 0, val);
			}
		}	
	}

	private void readUltraSparseToDense(DataInput in) 
		throws IOException, DMLRuntimeException 
	{	
		allocateDenseBlock(false); //allocate block
		Arrays.fill(denseBlock, 0);
		
		if( clen > 1 ) //ULTRA-SPARSE BLOCK
		{ 
			//block: read ijv-triples
			for(long i=0; i<nonZeros; i++) {
				int r = in.readInt();
				int c = in.readInt();
				double val = in.readDouble();			
				denseBlock[r*clen+c] = val;
			}
		}
		else //ULTRA-SPARSE COL
		{
			//col: read iv-pairs
			for(long i=0; i<nonZeros; i++) {
				int r = in.readInt();
				double val = in.readDouble();			
				denseBlock[r] = val;
			}
		}
	}
	
	@Override
	public void write(DataOutput out) 
		throws IOException 
	{
		//determine format
		boolean sparseSrc = sparse;
		boolean sparseDst = evalSparseFormatOnDisk();
		
		//write first part of header
		out.writeInt(rlen);
		out.writeInt(clen);
		
		if( sparseSrc )
		{
			//write sparse to *
			if( sparseBlock==null || nonZeros==0 ) 
				writeEmptyBlock(out);
			else if( nonZeros<rlen && sparseDst ) 
				writeSparseToUltraSparse(out); 
			else if( sparseDst ) 
				writeSparseBlock(out);
			else
				writeSparseToDense(out);
		}
		else
		{
			//write dense to *
			if( denseBlock==null || nonZeros==0 ) 
				writeEmptyBlock(out);
			else if( nonZeros<rlen && sparseDst )
				writeDenseToUltraSparse(out);
			else if( sparseDst )
				writeDenseToSparse(out);
			else
				writeDenseBlock(out);
		}
	}

	private void writeEmptyBlock(DataOutput out) 
		throws IOException
	{
		//empty blocks do not need to materialize row information
		out.writeByte( BlockType.EMPTY_BLOCK.ordinal() );
	}

	private void writeDenseBlock(DataOutput out) 
		throws IOException 
	{
		out.writeByte( BlockType.DENSE_BLOCK.ordinal() );
		
		int limit=rlen*clen;
		if( out instanceof MatrixBlockDataOutput ) //fast serialize
			((MatrixBlockDataOutput)out).writeDoubleArray(limit, denseBlock);
		else //general case (if fast serialize not supported)
			for(int i=0; i<limit; i++)
				out.writeDouble(denseBlock[i]);
	}

	private void writeSparseBlock(DataOutput out) 
		throws IOException 
	{
		out.writeByte( BlockType.SPARSE_BLOCK.ordinal() );
		writeNnzInfo( out, false );
		
		if( out instanceof MatrixBlockDataOutput ) //fast serialize
			((MatrixBlockDataOutput)out).writeSparseRows(rlen, sparseBlock);
		else //general case (if fast serialize not supported)
		{
			int r=0;
			for(;r<Math.min(rlen, sparseBlock.numRows()); r++)
			{
				if( sparseBlock.isEmpty(r) )
					out.writeInt(0);
				else
				{
					int pos = sparseBlock.pos(r);
					int nr = sparseBlock.size(r);
					int[] cols = sparseBlock.indexes(r);
					double[] values=sparseBlock.values(r);
					
					out.writeInt(nr);
					for(int j=pos; j<pos+nr; j++) {
						out.writeInt(cols[j]);
						out.writeDouble(values[j]);
					}					
				}	
			}
			for(;r<rlen; r++)
				out.writeInt(0);
		}
	}

	private void writeSparseToUltraSparse(DataOutput out) 
		throws IOException 
	{
		out.writeByte( BlockType.ULTRA_SPARSE_BLOCK.ordinal() );
		writeNnzInfo( out, true );
		
		long wnnz = 0;
		if( clen > 1 ) //ULTRA-SPARSE BLOCK
		{
			//block: write ijv-triples
			for(int r=0;r<Math.min(rlen, sparseBlock.numRows()); r++)
				if( !sparseBlock.isEmpty(r) )
				{
					int apos = sparseBlock.pos(r);
					int alen = sparseBlock.size(r);
					int[] aix = sparseBlock.indexes(r);
					double[] avals = sparseBlock.values(r);
					
					for(int j=apos; j<apos+alen; j++) {
						//ultra-sparse block: write ijv-triples
						out.writeInt(r);
						out.writeInt(aix[j]);
						out.writeDouble(avals[j]);
						wnnz++;
					}
				}	
		}
		else //ULTRA-SPARSE COL
		{
			//block: write iv-pairs (should never happen since always dense)
			for(int r=0;r<Math.min(rlen, sparseBlock.numRows()); r++)
				if(!sparseBlock.isEmpty(r) ) {
					int pos = sparseBlock.pos(r);
					out.writeInt(r);
					out.writeDouble(sparseBlock.values(r)[pos]);
					wnnz++;
				}
		}
				
		//validity check (nnz must exactly match written nnz)
		if( nonZeros != wnnz ) {
			throw new IOException("Invalid number of serialized non-zeros: "+wnnz+" (expected: "+nonZeros+")");
		}
	}

	private void writeSparseToDense(DataOutput out) 
		throws IOException 
	{
		//write block type 'dense'
		out.writeByte( BlockType.DENSE_BLOCK.ordinal() );
		
		//write data (from sparse to dense)
		if( sparseBlock==null ) //empty block
			for( int i=0; i<rlen*clen; i++ )
				out.writeDouble(0);
		else //existing sparse block
		{
			SparseBlock a = sparseBlock;
			for( int i=0; i<rlen; i++ )
			{
				if( i<a.numRows() && !a.isEmpty(i) )
				{
					int apos = a.pos(i);
					int alen = a.size(i);
					int[] aix = a.indexes(i);
					double[] avals = a.values(i);
					//foreach non-zero value, fill with 0s if required
					for( int j=0, j2=0; j2<alen; j++, j2++ ) {
						for( ; j<aix[apos+j2]; j++ )
							out.writeDouble( 0 );
						out.writeDouble( avals[apos+j2] );
					}					
					//remaining 0 values in row
					for( int j=aix[apos+alen-1]+1; j<clen; j++)
						out.writeDouble( 0 );
				}
				else //empty row
					for( int j=0; j<clen; j++ )
						out.writeDouble( 0 );	
			}
		}
	}

	private void writeDenseToUltraSparse(DataOutput out) throws IOException 
	{
		out.writeByte( BlockType.ULTRA_SPARSE_BLOCK.ordinal() );
		writeNnzInfo( out, true );

		long wnnz = 0;
		
		if( clen > 1 ) //ULTRA-SPARSE BLOCK
		{
			//block: write ijv-triples
			for(int r=0, ix=0; r<rlen; r++)
				for(int c=0; c<clen; c++, ix++)
					if( denseBlock[ix]!=0 ) {
						out.writeInt(r);
						out.writeInt(c);
						out.writeDouble(denseBlock[ix]);
						wnnz++;
					}
		}
		else //ULTRA-SPARSE COL
		{
			//col: write iv-pairs
			for(int r=0; r<rlen; r++)
				if( denseBlock[r]!=0 ) {
					out.writeInt(r);
					out.writeDouble(denseBlock[r]);
					wnnz++;
				}
		}
		
		//validity check (nnz must exactly match written nnz)
		if( nonZeros != wnnz ) {
			throw new IOException("Invalid number of serialized non-zeros: "+wnnz+" (expected: "+nonZeros+")");
		}
	}

	private void writeDenseToSparse(DataOutput out) 
		throws IOException 
	{	
		out.writeByte( BlockType.SPARSE_BLOCK.ordinal() ); //block type
		writeNnzInfo( out, false );
		
		int start=0;
		for(int r=0; r<rlen; r++)
		{
			//count nonzeros
			int nr=0;
			for(int i=start; i<start+clen; i++)
				if(denseBlock[i]!=0.0)
					nr++;
			out.writeInt(nr);
			for(int c=0; c<clen; c++)
			{
				if(denseBlock[start]!=0.0)
				{
					out.writeInt(c);
					out.writeDouble(denseBlock[start]);
				}
				start++;
			}
		}
	}

	private long readNnzInfo( DataInput in, boolean ultrasparse ) 
		throws IOException
	{
		//note: if ultrasparse, int always sufficient because nnz<rlen
		// where rlen is limited to integer
				
		long lrlen = (long)rlen;
		long lclen = (long)clen;
		
		//read long if required, otherwise int (see writeNnzInfo, consistency required)
		if( lrlen*lclen > Integer.MAX_VALUE  && !ultrasparse) {
			nonZeros = in.readLong();
		}
		else {
			nonZeros = in.readInt(); 
		}
		
		return nonZeros;
	}

	private void writeNnzInfo( DataOutput out, boolean ultrasparse ) 
		throws IOException
	{
		//note: if ultrasparse, int always sufficient because nnz<rlen
		// where rlen is limited to integer
		
		long lrlen = (long)rlen;
		long lclen = (long)clen;
		
		//write long if required, otherwise int
		if( lrlen*lclen > Integer.MAX_VALUE && !ultrasparse) {
			out.writeLong( nonZeros ); 
		}
		else {
			out.writeInt( (int)nonZeros );
		}
	}
	
	/**
	 * Redirects the default java serialization via externalizable to our default 
	 * hadoop writable serialization for efficient broadcast/rdd deserialization. 
	 * 
	 * @param is object input
	 * @throws IOException if IOException occurs
	 */
	public void readExternal(ObjectInput is) 
		throws IOException
	{
		if( is instanceof ObjectInputStream )
		{
			//fast deserialize of dense/sparse blocks
			ObjectInputStream ois = (ObjectInputStream)is;
			FastBufferedDataInputStream fis = new FastBufferedDataInputStream(ois);
			readFields(fis);
		}
		else {
			//default deserialize (general case)
			readFields(is);
		}
	}
	
	/**
	 * Redirects the default java serialization via externalizable to our default 
	 * hadoop writable serialization for efficient broadcast/rdd serialization. 
	 * 
	 * @param os object output
	 * @throws IOException if IOException occurs
	 */
	public void writeExternal(ObjectOutput os) 
		throws IOException
	{
		if( os instanceof ObjectOutputStream ) {
			//fast serialize of dense/sparse blocks
			ObjectOutputStream oos = (ObjectOutputStream)os;
			FastBufferedDataOutputStream fos = new FastBufferedDataOutputStream(oos);
			write(fos);
			fos.flush();
		}
		else {
			//default serialize (general case)
			write(os);	
		}
	}
	
	/**
	 * NOTE: The used estimates must be kept consistent with the respective write functions. 
	 * 
	 * @return exact size on disk
	 */
	public long getExactSizeOnDisk()
	{
		//determine format
		boolean sparseSrc = sparse;
		boolean sparseDst = evalSparseFormatOnDisk();
		
		long lrlen = (long) rlen;
		long lclen = (long) clen;
		long lnonZeros = (long) nonZeros;
		
		//ensure exact size estimates for write
		if( lnonZeros <= 0 )
		{
			recomputeNonZeros();
			lnonZeros = (long) nonZeros;
		}
				
		//get exact size estimate (see write for the corresponding meaning)
		if( sparseSrc )
		{
			//write sparse to *
			if(sparseBlock==null || lnonZeros==0)
				return HEADER_SIZE; //empty block
			else if( lnonZeros<lrlen && sparseDst )
				return estimateSizeUltraSparseOnDisk(lrlen, lclen, lnonZeros); //ultra sparse block
			else if( sparseDst )
				return estimateSizeSparseOnDisk(lrlen, lclen, lnonZeros); //sparse block
			else 
				return estimateSizeDenseOnDisk(lrlen, lclen); //dense block
		}
		else
		{
			//write dense to *
			if(denseBlock==null || lnonZeros==0)
				return HEADER_SIZE; //empty block
			else if( lnonZeros<lrlen && sparseDst )
				return estimateSizeUltraSparseOnDisk(lrlen, lclen, lnonZeros); //ultra sparse block
			else if( sparseDst )
				return estimateSizeSparseOnDisk(lrlen, lclen, lnonZeros); //sparse block
			else
				return estimateSizeDenseOnDisk(lrlen, lclen); //dense block
		}
	}
	
	////////
	// Estimates size and sparsity

	public long estimateSizeInMemory() 
	{
		double sp = OptimizerUtils.getSparsity(rlen, clen, nonZeros);
		return estimateSizeInMemory(rlen, clen, sp);
	}

	public static long estimateSizeInMemory(long nrows, long ncols, double sparsity)
	{
		//determine sparse/dense representation
		boolean sparse = evalSparseFormatInMemory(nrows, ncols, (long)(sparsity*nrows*ncols));
		
		//estimate memory consumption for sparse/dense
		if( sparse )
			return estimateSizeSparseInMemory(nrows, ncols, sparsity);
		else
			return estimateSizeDenseInMemory(nrows, ncols);
	}

	public static long estimateSizeDenseInMemory(long nrows, long ncols)
	{
		// basic variables and references sizes
		double size = 44;
		
		// core dense matrix block (double array)
		size += 8d * nrows * ncols;
		
		// robustness for long overflows
		return (long) Math.min(size, Long.MAX_VALUE);
	}

	public static long estimateSizeSparseInMemory(long nrows, long ncols, double sparsity)
	{
		// basic variables and references sizes
		double size = 44;
		
		// delegate memory estimate to individual sparse blocks
		size += SparseBlockFactory.estimateSizeSparseInMemory(
			DEFAULT_SPARSEBLOCK, nrows, ncols, sparsity);
		
		// robustness for long overflows
		return (long) Math.min(size, Long.MAX_VALUE);
	}

	public long estimateSizeOnDisk()
	{
		return estimateSizeOnDisk(rlen, clen, nonZeros);
	}

	public static long estimateSizeOnDisk( long nrows, long ncols, long nnz )
	{
		//determine sparse/dense representation
		boolean sparse = evalSparseFormatOnDisk(nrows, ncols, nnz);
		
		//estimate memory consumption for sparse/dense 
		if( sparse && nnz<nrows )
			return estimateSizeUltraSparseOnDisk(nrows, ncols, nnz);
		else if( sparse )
			return estimateSizeSparseOnDisk(nrows, ncols, nnz);
		else
			return estimateSizeDenseOnDisk(nrows, ncols);
	}

	private static long estimateSizeDenseOnDisk( long nrows, long ncols)
	{
		//basic header (int rlen, int clen, byte type) 
		long size = HEADER_SIZE;
		//data (all cells double)
		size += nrows * ncols * 8;

		return size;
	}

	private static long estimateSizeSparseOnDisk( long nrows, long ncols, long nnz )
	{
		//basic header: (int rlen, int clen, byte type) 
		long size = HEADER_SIZE;
		//extended header (long nnz)
		size += (nrows*ncols > Integer.MAX_VALUE) ? 8 : 4;
		//data: (int num per row, int-double pair per non-zero value)
		size += nrows * 4 + nnz * 12;	

		return size;
	}

	private static long estimateSizeUltraSparseOnDisk( long nrows, long ncols, long nnz )
	{
		//basic header (int rlen, int clen, byte type) 
		long size = HEADER_SIZE;
		//extended header (int nnz, guaranteed by rlen<nnz)
		size += 4;
		//data (int-int-double triples per non-zero value)
		if( ncols > 1 ) //block: ijv-triples 
			size += nnz * 16; 	
		else //column: iv-pairs
			size += nnz * 12; 
		
		return size;
	}

	public static SparsityEstimate estimateSparsityOnAggBinary(MatrixBlock m1, MatrixBlock m2, AggregateBinaryOperator op)
	{
		//Since MatrixMultLib always uses a dense output (except for ultra-sparse mm)
		//with subsequent check for sparsity, we should always return a dense estimate.
		//Once, we support more aggregate binary operations, we need to change this.
		
		//WARNING: KEEP CONSISTENT WITH LIBMATRIXMULT
		//Note that it is crucial to report the right output representation because
		//in case of block reuse (e.g., mmcj) the output 'reset' refers to either
		//dense or sparse representation and hence would produce incorrect results
		//if we report the wrong representation (i.e., missing reset on ultrasparse mm). 
		
		boolean ultrasparse = (m1.isUltraSparse() || m2.isUltraSparse());
		return new SparsityEstimate(ultrasparse, m1.getNumRows()*m2.getNumRows());
	}

	private static SparsityEstimate estimateSparsityOnBinary(MatrixBlock m1, MatrixBlock m2, BinaryOperator op)
	{
		SparsityEstimate est = new SparsityEstimate();
		
		//estimate dense output for all sparse-unsafe operations, except DIV (because it commonly behaves like
		//sparse-safe but is not due to 0/0->NaN, this is consistent with the current hop sparsity estimate)
		//see also, special sparse-safe case for DIV in LibMatrixBincell 
		if( !op.sparseSafe && !(op.fn instanceof Divide) ) {
			est.sparse = false;
			return est;
		}
		
		BinaryAccessType atype = LibMatrixBincell.getBinaryAccessType(m1, m2);
		boolean outer = (atype == BinaryAccessType.OUTER_VECTOR_VECTOR);
		long m = m1.getNumRows();
		long n = outer ? m2.getNumColumns() : m1.getNumColumns();
		long nz1 = m1.getNonZeros();
		long nz2 = m2.getNonZeros();
		
		//account for matrix vector and vector/vector
		long estnnz = 0;
		if( atype == BinaryAccessType.OUTER_VECTOR_VECTOR )
		{
			//for outer vector operations the sparsity estimate is exactly known
			estnnz = nz1 * nz2; 
		}
		else //DEFAULT CASE
		{		
			if( atype == BinaryAccessType.MATRIX_COL_VECTOR )
				nz2 = nz2 * n;
			else if( atype == BinaryAccessType.MATRIX_ROW_VECTOR )
				nz2 = nz2 * m;
		
			//compute output sparsity consistent w/ the hop compiler
			OpOp2 bop = op.getBinaryOperatorOpOp2();
			double sp1 = OptimizerUtils.getSparsity(m, n, nz1);
			double sp2 = OptimizerUtils.getSparsity(m, n, nz2);
			double spout = OptimizerUtils.getBinaryOpSparsity(sp1, sp2, bop, true);
			estnnz = UtilFunctions.toLong(spout * m * n);
		}
		
		est.sparse = evalSparseFormatInMemory(m, n, estnnz);
		est.estimatedNonZeros = estnnz;
		
		return est;
	}
	
	private boolean estimateSparsityOnSlice(int selectRlen, int selectClen, int finalRlen, int finalClen)
	{
		long ennz = (long)((double)nonZeros/rlen/clen*selectRlen*selectClen);
		return evalSparseFormatInMemory(finalRlen, finalClen, ennz); 
	}
	
	private boolean estimateSparsityOnLeftIndexing(long rlenm1, long clenm1, long nnzm1, long rlenm2, long clenm2, long nnzm2)
	{
		//min bound: nnzm1 - rlenm2*clenm2 + nnzm2
		//max bound: min(rlenm1*rlenm2, nnzm1+nnzm2)
		
		long ennz = Math.min(rlenm1*clenm1, nnzm1+nnzm2);
		return evalSparseFormatInMemory(rlenm1, clenm1, ennz);
	}
	
	private boolean estimateSparsityOnGroupedAgg( long rlen, long groups )
	{
		long ennz = Math.min(groups, rlen);
		return evalSparseFormatInMemory(groups, 1, ennz);
	}
	
	////////
	// CacheBlock implementation
	
	@Override
	public long getInMemorySize() {
		//in-memory size given by header if not allocated
		if( !isAllocated() ) 
			return 44;
		//in-memory size of dense/sparse representation
		double sp = OptimizerUtils.getSparsity(rlen, clen, nonZeros);
		return sparse ? estimateSizeSparseInMemory(rlen, clen, sp) : 
			estimateSizeDenseInMemory(rlen, clen);
	}
	
	@Override
	public long getExactSerializedSize() {
		return getExactSizeOnDisk();
	}
	
	@Override
	public boolean isShallowSerialize() {
		//shallow serialize if dense, dense in serialized form or already in CSR
		return !sparse || !evalSparseFormatOnDisk()
			|| (sparse && sparseBlock instanceof SparseBlockCSR);
	}
	
	@Override
	public void compactEmptyBlock() {
		if( isEmptyBlock(false) && isAllocated() )
			cleanupBlock(true, true);
	}
	
	////////
	// Core block operations (called from instructions)

	@Override
	public MatrixValue scalarOperations(ScalarOperator op, MatrixValue result) 
		throws DMLRuntimeException
	{
		MatrixBlock ret = checkType(result);
		
		// estimate the sparsity structure of result matrix
		boolean sp = this.sparse; // by default, we guess result.sparsity=input.sparsity
		if (!op.sparseSafe)
			sp = false; // if the operation is not sparse safe, then result will be in dense format
		
		//allocate the output matrix block
		if( ret==null )
			ret = new MatrixBlock(rlen, clen, sp, this.nonZeros);
		else
			ret.reset(rlen, clen, sp, this.nonZeros);
		
		//core scalar operations
		LibMatrixBincell.bincellOp(this, ret, op);
		
		return ret;
	}

	@Override
	public MatrixValue unaryOperations(UnaryOperator op, MatrixValue result) 
		throws DMLRuntimeException
	{
		MatrixBlock ret = checkType(result);
		
		// estimate the sparsity structure of result matrix
		boolean sp = this.sparse; // by default, we guess result.sparsity=input.sparsity
		if (!op.sparseSafe)
			sp = false; // if the operation is not sparse safe, then result will be in dense format
		
		//allocate output
		if( ret == null )
			ret = new MatrixBlock(rlen, clen, sp, this.nonZeros);
		else 
			ret.reset(rlen, clen, sp);
		
		//core execute
		if( LibMatrixAgg.isSupportedUnaryOperator(op) ) 
		{
			//e.g., cumsum/cumprod/cummin/cumax
			if( op.getNumThreads() > 1 )
				LibMatrixAgg.cumaggregateUnaryMatrix(this, ret, op, op.getNumThreads());
			else
				LibMatrixAgg.cumaggregateUnaryMatrix(this, ret, op);
		}
		else
		{
			//default execute unary operations
			if(op.sparseSafe)
				sparseUnaryOperations(op, ret);
			else
				denseUnaryOperations(op, ret);
		}
		
		//ensure empty results sparse representation 
		//(no additional memory requirements)
		if( ret.isEmptyBlock(false) )
			ret.examSparsity();
		
		return ret;
	}

	private void sparseUnaryOperations(UnaryOperator op, MatrixBlock ret) 
		throws DMLRuntimeException
	{
		//early abort possible since sparse-safe
		if( isEmptyBlock(false) )
			return;
		
		final int m = rlen;
		final int n = clen;
		
		if( sparse && ret.sparse ) //SPARSE <- SPARSE
		{
			ret.allocateSparseRowsBlock();
			SparseBlock a = sparseBlock;
			SparseBlock c = ret.sparseBlock;
		
			long nnz = 0;
			for(int i=0; i<m; i++) {
				if( a.isEmpty(i) ) continue;
				
				int apos = a.pos(i);
				int alen = a.size(i);
				int[] aix = a.indexes(i);
				double[] avals = a.values(i);
				
				c.allocate(i, alen); //avoid repeated alloc
				for( int j=apos; j<apos+alen; j++ ) {
					double val = op.fn.execute(avals[j]);
					c.append(i, aix[j], val);
					nnz += (val != 0) ? 1 : 0;
				}
			}
			ret.nonZeros = nnz;
		}
		else if( sparse ) //DENSE <- SPARSE
		{
			SparseBlock a = sparseBlock;			
			
			for(int i=0; i<m; i++) {
				if( a.isEmpty(i) ) continue;
			
				int apos = a.pos(i);
				int alen = a.size(i);
				int[] aix = a.indexes(i);
				double[] avals = a.values(i);
				
				for( int j=apos; j<apos+alen; j++ ) {
					double val = op.fn.execute(avals[j]);
					ret.appendValue(i, aix[j], val);
				}
			}
			//nnz maintained on appendValue
		}
		else //DENSE <- DENSE
		{
			//allocate dense output block
			ret.allocateDenseBlock();						
			double[] a = denseBlock;
			double[] c = ret.denseBlock;
			int len = m * n;
			
			//unary op, incl nnz maintenance
			int nnz = 0;
			for( int i=0; i<len; i++ ) {
				c[i] = op.fn.execute(a[i]);
				nnz += (c[i] != 0) ? 1 : 0;
			}
			ret.nonZeros = nnz;
		}
	}

	private void denseUnaryOperations(UnaryOperator op, MatrixBlock ret) 
		throws DMLRuntimeException
	{
		//prepare 0-value init (determine if unnecessarily sparse-unsafe)
		double val0 = op.fn.execute(0);
		
		final int m = rlen;
		final int n = clen;
		
		//early abort possible if unnecessarily sparse unsafe
		//(otherwise full init with val0, no need for computation)
		if( isEmptyBlock(false) ) {
			if( val0 != 0 )
				ret.reset(m, n, val0);
			return;
		}
		
		//redirection to sparse safe operation w/ init by val0
		if( sparse && val0 != 0 )
			ret.reset(m, n, val0);
		sparseUnaryOperations(op, ret);
	}

	@Override
	public void unaryOperationsInPlace(UnaryOperator op) 
		throws DMLRuntimeException
	{
		if(op.sparseSafe)
			sparseUnaryOperationsInPlace(op);
		else
			denseUnaryOperationsInPlace(op);
	}
	
	/**
	 * only apply to non zero cells
	 * 
	 * @param op unary operator
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	private void sparseUnaryOperationsInPlace(UnaryOperator op) 
		throws DMLRuntimeException
	{
		//early abort possible since sparse-safe
		if( isEmptyBlock(false) )
			return;
		
		if(sparse)
		{
			nonZeros=0;
			for(int r=0; r<Math.min(rlen, sparseBlock.numRows()); r++)
			{
				if(sparseBlock.isEmpty(r)) 
					continue;
				
				int apos = sparseBlock.pos(r);
				int alen = sparseBlock.size(r);
				int[] aix = sparseBlock.indexes(r);
				double[] avals = sparseBlock.values(r);
				
				int pos=0;
				for(int i=apos; i<apos+alen; i++)
				{
					double v=op.fn.execute(avals[i]);
					if(v!=0) {
						avals[pos]=v;
						aix[pos]=aix[i];
						pos++;
						nonZeros++;
					}
				}
				//TODO perf sparse block: truncate replaced by deleteIndexrange
				sparseBlock.deleteIndexRange(r, pos, clen);
			}
			
		}
		else
		{
			int limit=rlen*clen;
			nonZeros=0;
			for(int i=0; i<limit; i++)
			{
				denseBlock[i]=op.fn.execute(denseBlock[i]);
				if(denseBlock[i]!=0)
					nonZeros++;
			}
		}
	}
	
	private void denseUnaryOperationsInPlace(UnaryOperator op) 
		throws DMLRuntimeException
	{
		if( sparse ) //SPARSE MATRIX
		{
			double v;
			for(int r=0; r<rlen; r++)
				for(int c=0; c<clen; c++)
				{
					v=op.fn.execute(quickGetValue(r, c));
					quickSetValue(r, c, v);
				}
		}
		else//DENSE MATRIX
		{
			//early abort not possible because not sparsesafe
			if(denseBlock==null)
				allocateDenseBlock();
			
			//compute values in-place and update nnz
			final int limit = rlen*clen;
			int lnnz = 0;
			for( int i=0; i<limit; i++ ) {
				denseBlock[i] = op.fn.execute(denseBlock[i]);	
				if( denseBlock[i]!=0 )
					lnnz++;
			}		
			nonZeros = lnnz;
			
			//IBM JVM bug (JDK6) causes crash for certain inputs (w/ infinities) 
			//nonZeros = 0;
			//for(int i=0; i<limit; i++)
			//{
			//	denseBlock[i]=op.fn.execute(denseBlock[i]);
			//	if(denseBlock[i]!=0)
			//		nonZeros++;
			//}
		}
	}

	public MatrixValue binaryOperations(BinaryOperator op, MatrixValue thatValue, MatrixValue result) 
		throws DMLRuntimeException
	{
		MatrixBlock that = checkType(thatValue);
		MatrixBlock ret = checkType(result);
		if( !LibMatrixBincell.isValidDimensionsBinary(this, that) ) {
			throw new RuntimeException("Block sizes are not matched for binary " +
					"cell operations: "+this.rlen+"x"+this.clen+" vs "+ that.rlen+"x"+that.clen);
		}
		
		//compute output dimensions
		boolean outer = (LibMatrixBincell.getBinaryAccessType(this, that)
				         == BinaryAccessType.OUTER_VECTOR_VECTOR); 
		int rows = rlen;
		int cols = outer ? that.clen : clen;
		
		//estimate output sparsity
		SparsityEstimate resultSparse = estimateSparsityOnBinary(this, that, op);
		if( ret == null )
			ret = new MatrixBlock(rows, cols, resultSparse.sparse, resultSparse.estimatedNonZeros);
		else
			ret.reset(rows, cols, resultSparse.sparse, resultSparse.estimatedNonZeros);
		
		//core binary cell operation
		LibMatrixBincell.bincellOp( this, that, ret, op );
		
		return ret;
	}

	public void binaryOperationsInPlace(BinaryOperator op, MatrixValue thatValue) 
		throws DMLRuntimeException
	{
		MatrixBlock that=checkType(thatValue);
		if( !LibMatrixBincell.isValidDimensionsBinary(this, that) ) {
			throw new RuntimeException("block sizes are not matched for binary " +
					"cell operations: "+this.rlen+"*"+this.clen+" vs "+ that.rlen+"*"+that.clen);
		}
	
		//estimate output sparsity
		SparsityEstimate resultSparse = estimateSparsityOnBinary(this, that, op);
		if(resultSparse.sparse && !this.sparse)
			denseToSparse();
		else if(!resultSparse.sparse && this.sparse)
			sparseToDense();
				
		//core binary cell operation
		LibMatrixBincell.bincellOpInPlace(this, that, op);
	}


	
	public void incrementalAggregate(AggregateOperator aggOp, MatrixValue correction, 
			MatrixValue newWithCorrection)
	throws DMLRuntimeException
	{
		//assert(aggOp.correctionExists); 
		MatrixBlock cor=checkType(correction);
		MatrixBlock newWithCor=checkType(newWithCorrection);
		KahanObject buffer=new KahanObject(0, 0);
		
		if(aggOp.correctionLocation==CorrectionLocationType.LASTROW)
		{
			for(int r=0; r<rlen; r++)
				for(int c=0; c<clen; c++)
				{
					buffer._sum=this.quickGetValue(r, c);
					buffer._correction=cor.quickGetValue(0, c);
					buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newWithCor.quickGetValue(r, c), 
							newWithCor.quickGetValue(r+1, c));
					quickSetValue(r, c, buffer._sum);
					cor.quickSetValue(0, c, buffer._correction);
				}
			
		}else if(aggOp.correctionLocation==CorrectionLocationType.LASTCOLUMN)
		{
			if(aggOp.increOp.fn instanceof Builtin 
			   && ( ((Builtin)(aggOp.increOp.fn)).bFunc == Builtin.BuiltinCode.MAXINDEX
			         || ((Builtin)(aggOp.increOp.fn)).bFunc == Builtin.BuiltinCode.MININDEX )
			         ){
					// *** HACK ALERT *** HACK ALERT *** HACK ALERT ***
					// rowIndexMax() and its siblings don't fit very well into the standard
					// aggregate framework. We (ab)use the "correction factor" argument to
					// hold the maximum value in each row/column.
				
					// The execute() method for this aggregate takes as its argument
					// two candidates for the highest value. Bookkeeping about
					// indexes (return column/row index with highest value, breaking
					// ties in favor of higher indexes) is handled in this function.
					// Note that both versions of incrementalAggregate() contain
					// very similar blocks of special-case code. If one block is
					// modified, the other needs to be changed to match.
					for(int r=0; r<rlen; r++){
						double currMaxValue = cor.quickGetValue(r, 0);
						long newMaxIndex = (long)newWithCor.quickGetValue(r, 0);
						double newMaxValue = newWithCor.quickGetValue(r, 1);
						double update = aggOp.increOp.fn.execute(newMaxValue, currMaxValue);
						
						if (2.0 == update) {
							// Return value of 2 ==> both values the same, break ties
							// in favor of higher index.
							long curMaxIndex = (long) quickGetValue(r,0);
							quickSetValue(r, 0, Math.max(curMaxIndex, newMaxIndex));
						} else if(1.0 == update){
							// Return value of 1 ==> new value is better; use its index
							quickSetValue(r, 0, newMaxIndex);
							cor.quickSetValue(r, 0, newMaxValue);
						} else {
							// Other return value ==> current answer is best
						}
					}
					// *** END HACK ***
				}else{
					for(int r=0; r<rlen; r++)
						for(int c=0; c<clen; c++)
						{
							buffer._sum=this.quickGetValue(r, c);
							buffer._correction=cor.quickGetValue(r, 0);;
							buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newWithCor.quickGetValue(r, c), newWithCor.quickGetValue(r, c+1));
							quickSetValue(r, c, buffer._sum);
							cor.quickSetValue(r, 0, buffer._correction);
						}
				}
		}
		else if(aggOp.correctionLocation==CorrectionLocationType.NONE)
		{
			
			//e.g., ak+ kahan plus as used in sum, mapmult, mmcj and tsmm
			if(aggOp.increOp.fn instanceof KahanPlus)
			{
				LibMatrixAgg.aggregateBinaryMatrix(newWithCor, this, cor);
			}
			else
			{
				if( newWithCor.isInSparseFormat() && aggOp.sparseSafe ) //SPARSE
				{
					SparseBlock b = newWithCor.getSparseBlock();
					if( b==null ) //early abort on empty block
						return;
					for( int r=0; r<Math.min(rlen, b.numRows()); r++ )
					{
						if( !b.isEmpty(r) ) 
						{
							int bpos = b.pos(r);
							int blen = b.size(r);
							int[] bix = b.indexes(r);
							double[] bvals = b.values(r);
							for( int j=bpos; j<bpos+blen; j++)
							{
								int c = bix[j];
								buffer._sum = this.quickGetValue(r, c);
								buffer._correction = cor.quickGetValue(r, c);
								buffer = (KahanObject) aggOp.increOp.fn.execute(buffer, bvals[j]);
								quickSetValue(r, c, buffer._sum);
								cor.quickSetValue(r, c, buffer._correction);			
							}
						}
					}
					
				}
				else //DENSE or SPARSE (!sparsesafe)
				{
					for(int r=0; r<rlen; r++)
						for(int c=0; c<clen; c++)
						{
							buffer._sum=this.quickGetValue(r, c);
							buffer._correction=cor.quickGetValue(r, c);
							buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newWithCor.quickGetValue(r, c));
							quickSetValue(r, c, buffer._sum);
							cor.quickSetValue(r, c, buffer._correction);
						}
				}
			
				//change representation if required
				//(note since ak+ on blocks is currently only applied in MR, hence no need to account for this in mem estimates)
				examSparsity(); 
			}
		}
		else if(aggOp.correctionLocation==CorrectionLocationType.LASTTWOROWS)
		{
			double n, n2, mu2;
			for(int r=0; r<rlen; r++)
				for(int c=0; c<clen; c++)
				{
					buffer._sum=this.quickGetValue(r, c);
					n=cor.quickGetValue(0, c);
					buffer._correction=cor.quickGetValue(1, c);
					mu2=newWithCor.quickGetValue(r, c);
					n2=newWithCor.quickGetValue(r+1, c);
					n=n+n2;
					double toadd=(mu2-buffer._sum)*n2/n;
					buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, toadd);
					quickSetValue(r, c, buffer._sum);
					cor.quickSetValue(0, c, n);
					cor.quickSetValue(1, c, buffer._correction);
				}
			
		}else if(aggOp.correctionLocation==CorrectionLocationType.LASTTWOCOLUMNS)
		{
			double n, n2, mu2;
			for(int r=0; r<rlen; r++)
				for(int c=0; c<clen; c++)
				{
					buffer._sum=this.quickGetValue(r, c);
					n=cor.quickGetValue(r, 0);
					buffer._correction=cor.quickGetValue(r, 1);
					mu2=newWithCor.quickGetValue(r, c);
					n2=newWithCor.quickGetValue(r, c+1);
					n=n+n2;
					double toadd=(mu2-buffer._sum)*n2/n;
					buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, toadd);
					quickSetValue(r, c, buffer._sum);
					cor.quickSetValue(r, 0, n);
					cor.quickSetValue(r, 1, buffer._correction);
				}
		}
		else if (aggOp.correctionLocation == CorrectionLocationType.LASTFOURROWS
				&& aggOp.increOp.fn instanceof CM
				&& ((CM) aggOp.increOp.fn).getAggOpType() == AggregateOperationTypes.VARIANCE) {
			// create buffers to store results
			CM_COV_Object cbuff_curr = new CM_COV_Object();
			CM_COV_Object cbuff_part = new CM_COV_Object();

			// perform incremental aggregation
			for (int r=0; r<rlen; r++)
				for (int c=0; c<clen; c++) {
					// extract current values: { var | mean, count, m2 correction, mean correction }
					// note: m2 = var * (n - 1)
					cbuff_curr.w = cor.quickGetValue(1, c); // count
					cbuff_curr.m2._sum = quickGetValue(r, c) * (cbuff_curr.w - 1); // m2
					cbuff_curr.mean._sum = cor.quickGetValue(0, c); // mean
					cbuff_curr.m2._correction = cor.quickGetValue(2, c);
					cbuff_curr.mean._correction = cor.quickGetValue(3, c);

					// extract partial values: { var | mean, count, m2 correction, mean correction }
					// note: m2 = var * (n - 1)
					cbuff_part.w = newWithCor.quickGetValue(r+2, c); // count
					cbuff_part.m2._sum = newWithCor.quickGetValue(r, c) * (cbuff_part.w - 1); // m2
					cbuff_part.mean._sum = newWithCor.quickGetValue(r+1, c); // mean
					cbuff_part.m2._correction = newWithCor.quickGetValue(r+3, c);
					cbuff_part.mean._correction = newWithCor.quickGetValue(r+4, c);

					// calculate incremental aggregated variance
					cbuff_curr = (CM_COV_Object) aggOp.increOp.fn.execute(cbuff_curr, cbuff_part);

					// store updated values: { var | mean, count, m2 correction, mean correction }
					double var = cbuff_curr.getRequiredResult(AggregateOperationTypes.VARIANCE);
					quickSetValue(r, c, var);
					cor.quickSetValue(0, c, cbuff_curr.mean._sum); // mean
					cor.quickSetValue(1, c, cbuff_curr.w); // count
					cor.quickSetValue(2, c, cbuff_curr.m2._correction);
					cor.quickSetValue(3, c, cbuff_curr.mean._correction);
				}
		}
		else if (aggOp.correctionLocation == CorrectionLocationType.LASTFOURCOLUMNS
				&& aggOp.increOp.fn instanceof CM
				&& ((CM) aggOp.increOp.fn).getAggOpType() == AggregateOperationTypes.VARIANCE) {
			// create buffers to store results
			CM_COV_Object cbuff_curr = new CM_COV_Object();
			CM_COV_Object cbuff_part = new CM_COV_Object();

			// perform incremental aggregation
			for (int r=0; r<rlen; r++)
				for (int c=0; c<clen; c++) {
					// extract current values: { var | mean, count, m2 correction, mean correction }
					// note: m2 = var * (n - 1)
					cbuff_curr.w = cor.quickGetValue(r, 1); // count
					cbuff_curr.m2._sum = quickGetValue(r, c) * (cbuff_curr.w - 1); // m2
					cbuff_curr.mean._sum = cor.quickGetValue(r, 0); // mean
					cbuff_curr.m2._correction = cor.quickGetValue(r, 2);
					cbuff_curr.mean._correction = cor.quickGetValue(r, 3);

					// extract partial values: { var | mean, count, m2 correction, mean correction }
					// note: m2 = var * (n - 1)
					cbuff_part.w = newWithCor.quickGetValue(r, c+2); // count
					cbuff_part.m2._sum = newWithCor.quickGetValue(r, c) * (cbuff_part.w - 1); // m2
					cbuff_part.mean._sum = newWithCor.quickGetValue(r, c+1); // mean
					cbuff_part.m2._correction = newWithCor.quickGetValue(r, c+3);
					cbuff_part.mean._correction = newWithCor.quickGetValue(r, c+4);

					// calculate incremental aggregated variance
					cbuff_curr = (CM_COV_Object) aggOp.increOp.fn.execute(cbuff_curr, cbuff_part);

					// store updated values: { var | mean, count, m2 correction, mean correction }
					double var = cbuff_curr.getRequiredResult(AggregateOperationTypes.VARIANCE);
					quickSetValue(r, c, var);
					cor.quickSetValue(r, 0, cbuff_curr.mean._sum); // mean
					cor.quickSetValue(r, 1, cbuff_curr.w); // count
					cor.quickSetValue(r, 2, cbuff_curr.m2._correction);
					cor.quickSetValue(r, 3, cbuff_curr.mean._correction);
				}
		}
		else
			throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correctionLocation);
	}
	
	public void incrementalAggregate(AggregateOperator aggOp, MatrixValue newWithCorrection)
		throws DMLRuntimeException
	{
		//assert(aggOp.correctionExists);
		MatrixBlock newWithCor=checkType(newWithCorrection);
		KahanObject buffer=new KahanObject(0, 0);
		
		if(aggOp.correctionLocation==CorrectionLocationType.LASTROW)
		{			
			if( aggOp.increOp.fn instanceof KahanPlus )
			{
				LibMatrixAgg.aggregateBinaryMatrix(newWithCor, this, aggOp);
			}
			else
			{
				for(int r=0; r<rlen-1; r++)
					for(int c=0; c<clen; c++)
					{
						buffer._sum=this.quickGetValue(r, c);
						buffer._correction=this.quickGetValue(r+1, c);
						buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newWithCor.quickGetValue(r, c), 
								newWithCor.quickGetValue(r+1, c));
						quickSetValue(r, c, buffer._sum);
						quickSetValue(r+1, c, buffer._correction);
					}
			}	
		}
		else if(aggOp.correctionLocation==CorrectionLocationType.LASTCOLUMN)
		{
			if(aggOp.increOp.fn instanceof Builtin 
			   && ( ((Builtin)(aggOp.increOp.fn)).bFunc == Builtin.BuiltinCode.MAXINDEX 
			        || ((Builtin)(aggOp.increOp.fn)).bFunc == Builtin.BuiltinCode.MININDEX) 
			        ){
				// *** HACK ALERT *** HACK ALERT *** HACK ALERT ***
				// rowIndexMax() and its siblings don't fit very well into the standard
				// aggregate framework. We (ab)use the "correction factor" argument to
				// hold the maximum value in each row/column.
			
				// The execute() method for this aggregate takes as its argument
				// two candidates for the highest value. Bookkeeping about
				// indexes (return column/row index with highest value, breaking
				// ties in favor of higher indexes) is handled in this function.
				// Note that both versions of incrementalAggregate() contain
				// very similar blocks of special-case code. If one block is
				// modified, the other needs to be changed to match.
				for(int r = 0; r < rlen; r++){
					double currMaxValue = quickGetValue(r, 1);
					long newMaxIndex = (long)newWithCor.quickGetValue(r, 0);
					double newMaxValue = newWithCor.quickGetValue(r, 1);
					double update = aggOp.increOp.fn.execute(newMaxValue, currMaxValue);
	
					if (2.0 == update) {
						// Return value of 2 ==> both values the same, break ties
						// in favor of higher index.
						long curMaxIndex = (long) quickGetValue(r,0);
						quickSetValue(r, 0, Math.max(curMaxIndex, newMaxIndex));
					} else if(1.0 == update){
						// Return value of 1 ==> new value is better; use its index
						quickSetValue(r, 0, newMaxIndex);
						quickSetValue(r, 1, newMaxValue);
					} else {
						// Other return value ==> current answer is best
					}
				}
				// *** END HACK ***
			}
			else
			{
				if(aggOp.increOp.fn instanceof KahanPlus)
				{
					LibMatrixAgg.aggregateBinaryMatrix(newWithCor, this, aggOp);
				}
				else
				{
					for(int r=0; r<rlen; r++)
						for(int c=0; c<clen-1; c++)
						{
							buffer._sum=this.quickGetValue(r, c);
							buffer._correction=this.quickGetValue(r, c+1);
							buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newWithCor.quickGetValue(r, c), newWithCor.quickGetValue(r, c+1));
							quickSetValue(r, c, buffer._sum);
							quickSetValue(r, c+1, buffer._correction);
						}
				}
			}
		}
		else if(aggOp.correctionLocation==CorrectionLocationType.LASTTWOROWS)
		{
			double n, n2, mu2;
			for(int r=0; r<rlen-2; r++)
				for(int c=0; c<clen; c++)
				{
					buffer._sum=this.quickGetValue(r, c);
					n=this.quickGetValue(r+1, c);
					buffer._correction=this.quickGetValue(r+2, c);
					mu2=newWithCor.quickGetValue(r, c);
					n2=newWithCor.quickGetValue(r+1, c);
					n=n+n2;
					double toadd=(mu2-buffer._sum)*n2/n;
					buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, toadd);
					quickSetValue(r, c, buffer._sum);
					quickSetValue(r+1, c, n);
					quickSetValue(r+2, c, buffer._correction);
				}
			
		}else if(aggOp.correctionLocation==CorrectionLocationType.LASTTWOCOLUMNS)
		{
			double n, n2, mu2;
			for(int r=0; r<rlen; r++)
				for(int c=0; c<clen-2; c++)
				{
					buffer._sum=this.quickGetValue(r, c);
					n=this.quickGetValue(r, c+1);
					buffer._correction=this.quickGetValue(r, c+2);
					mu2=newWithCor.quickGetValue(r, c);
					n2=newWithCor.quickGetValue(r, c+1);
					n=n+n2;
					double toadd=(mu2-buffer._sum)*n2/n;
					buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, toadd);
					quickSetValue(r, c, buffer._sum);
					quickSetValue(r, c+1, n);
					quickSetValue(r, c+2, buffer._correction);
				}
		}
		else if (aggOp.correctionLocation == CorrectionLocationType.LASTFOURROWS
				&& aggOp.increOp.fn instanceof CM
				&& ((CM) aggOp.increOp.fn).getAggOpType() == AggregateOperationTypes.VARIANCE) {
			// create buffers to store results
			CM_COV_Object cbuff_curr = new CM_COV_Object();
			CM_COV_Object cbuff_part = new CM_COV_Object();

			// perform incremental aggregation
			for (int r=0; r<rlen-4; r++)
				for (int c=0; c<clen; c++) {
					// extract current values: { var | mean, count, m2 correction, mean correction }
					// note: m2 = var * (n - 1)
					cbuff_curr.w = quickGetValue(r+2, c); // count
					cbuff_curr.m2._sum = quickGetValue(r, c) * (cbuff_curr.w - 1); // m2
					cbuff_curr.mean._sum = quickGetValue(r+1, c); // mean
					cbuff_curr.m2._correction = quickGetValue(r+3, c);
					cbuff_curr.mean._correction = quickGetValue(r+4, c);

					// extract partial values: { var | mean, count, m2 correction, mean correction }
					// note: m2 = var * (n - 1)
					cbuff_part.w = newWithCor.quickGetValue(r+2, c); // count
					cbuff_part.m2._sum = newWithCor.quickGetValue(r, c) * (cbuff_part.w - 1); // m2
					cbuff_part.mean._sum = newWithCor.quickGetValue(r+1, c); // mean
					cbuff_part.m2._correction = newWithCor.quickGetValue(r+3, c);
					cbuff_part.mean._correction = newWithCor.quickGetValue(r+4, c);

					// calculate incremental aggregated variance
					cbuff_curr = (CM_COV_Object) aggOp.increOp.fn.execute(cbuff_curr, cbuff_part);

					// store updated values: { var | mean, count, m2 correction, mean correction }
					double var = cbuff_curr.getRequiredResult(AggregateOperationTypes.VARIANCE);
					quickSetValue(r, c, var);
					quickSetValue(r+1, c, cbuff_curr.mean._sum); // mean
					quickSetValue(r+2, c, cbuff_curr.w); // count
					quickSetValue(r+3, c, cbuff_curr.m2._correction);
					quickSetValue(r+4, c, cbuff_curr.mean._correction);
				}
		}
		else if (aggOp.correctionLocation == CorrectionLocationType.LASTFOURCOLUMNS
				&& aggOp.increOp.fn instanceof CM
				&& ((CM) aggOp.increOp.fn).getAggOpType() == AggregateOperationTypes.VARIANCE) {
			// create buffers to store results
			CM_COV_Object cbuff_curr = new CM_COV_Object();
			CM_COV_Object cbuff_part = new CM_COV_Object();

			// perform incremental aggregation
			for (int r=0; r<rlen; r++)
				for (int c=0; c<clen-4; c++) {
					// extract current values: { var | mean, count, m2 correction, mean correction }
					// note: m2 = var * (n - 1)
					cbuff_curr.w = quickGetValue(r, c+2); // count
					cbuff_curr.m2._sum = quickGetValue(r, c) * (cbuff_curr.w - 1); // m2
					cbuff_curr.mean._sum = quickGetValue(r, c+1); // mean
					cbuff_curr.m2._correction = quickGetValue(r, c+3);
					cbuff_curr.mean._correction = quickGetValue(r, c+4);

					// extract partial values: { var | mean, count, m2 correction, mean correction }
					// note: m2 = var * (n - 1)
					cbuff_part.w = newWithCor.quickGetValue(r, c+2); // count
					cbuff_part.m2._sum = newWithCor.quickGetValue(r, c) * (cbuff_part.w - 1); // m2
					cbuff_part.mean._sum = newWithCor.quickGetValue(r, c+1); // mean
					cbuff_part.m2._correction = newWithCor.quickGetValue(r, c+3);
					cbuff_part.mean._correction = newWithCor.quickGetValue(r, c+4);

					// calculate incremental aggregated variance
					cbuff_curr = (CM_COV_Object) aggOp.increOp.fn.execute(cbuff_curr, cbuff_part);

					// store updated values: { var | mean, count, m2 correction, mean correction }
					double var = cbuff_curr.getRequiredResult(AggregateOperationTypes.VARIANCE);
					quickSetValue(r, c, var);
					quickSetValue(r, c+1, cbuff_curr.mean._sum); // mean
					quickSetValue(r, c+2, cbuff_curr.w); // count
					quickSetValue(r, c+3, cbuff_curr.m2._correction);
					quickSetValue(r, c+4, cbuff_curr.mean._correction);
				}
		}
		else
			throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correctionLocation);
	}

	@Override
	public MatrixValue reorgOperations(ReorgOperator op, MatrixValue ret, int startRow, int startColumn, int length)
		throws DMLRuntimeException 
	{
		if ( !( op.fn instanceof SwapIndex || op.fn instanceof DiagIndex 
			|| op.fn instanceof SortIndex || op.fn instanceof RevIndex ) )
			throw new DMLRuntimeException("the current reorgOperations cannot support: "+op.fn.getClass()+".");
		
		MatrixBlock result = checkType(ret);

		//compute output dimensions and sparsity flag
		CellIndex tempCellIndex = new CellIndex(-1,-1);
		op.fn.computeDimension( rlen, clen, tempCellIndex );
		boolean sps = evalSparseFormatInMemory(tempCellIndex.row, tempCellIndex.column, nonZeros);

		//prepare output matrix block w/ right meta data
		if( result == null )
			result = new MatrixBlock(tempCellIndex.row, tempCellIndex.column, sps, nonZeros);
		else
			result.reset(tempCellIndex.row, tempCellIndex.column, sps, nonZeros);
		
		if( LibMatrixReorg.isSupportedReorgOperator(op) )
		{
			//SPECIAL case (operators with special performance requirements, 
			//or size-dependent special behavior)
			//currently supported opcodes: r', rdiag, rsort
			LibMatrixReorg.reorg(this, result, op);
		}
		else 
		{
			//GENERIC case (any reorg operator)
			CellIndex temp = new CellIndex(0, 0);
			if(sparse)
			{
				if(sparseBlock != null)
				{
					for(int r=0; r<Math.min(rlen, sparseBlock.numRows()); r++)
					{
						if(sparseBlock.isEmpty(r)) 
							continue;
						
						int apos = sparseBlock.pos(r);
						int alen = sparseBlock.size(r);
						int[] aix = sparseBlock.indexes(r);
						double[] avals = sparseBlock.values(r);
						
						for(int i=apos; i<apos+alen; i++) {
							tempCellIndex.set(r, aix[i]);
							op.fn.execute(tempCellIndex, temp);
							result.appendValue(temp.row, temp.column, avals[i]);
						}
					}
				}
			}
			else
			{
				if( denseBlock != null ) 
				{
					if( result.isInSparseFormat() ) //SPARSE<-DENSE
					{
						double[] a = denseBlock;
						for( int i=0, aix=0; i<rlen; i++ )
							for( int j=0; j<clen; j++, aix++ )
							{
								temp.set(i, j);
								op.fn.execute(temp, temp);
								result.appendValue(temp.row, temp.column, a[aix]);	
							}
					}
					else //DENSE<-DENSE
					{
						result.allocateDenseBlock();
						Arrays.fill(result.denseBlock, 0);
						double[] a = denseBlock;
						double[] c = result.denseBlock;
						int n = result.clen;
						
						for( int i=0, aix=0; i<rlen; i++ )
							for( int j=0; j<clen; j++, aix++ )
							{
								temp.set(i, j);
								op.fn.execute(temp, temp);
								c[temp.row*n+temp.column] = a[aix];	
							}
						result.nonZeros = nonZeros;
					}
				}
			}
		}
		
		return result;
	}

	public MatrixBlock appendOperations( MatrixBlock that, MatrixBlock ret ) 	
		throws DMLRuntimeException 
	{
		//default append-cbind
		return appendOperations(that, ret, true);
	}

	public MatrixBlock appendOperations( MatrixBlock that, MatrixBlock ret, boolean cbind ) 	
		throws DMLRuntimeException 
	{
		MatrixBlock result = checkType( ret );
		final int m = cbind ? rlen : rlen+that.rlen;
		final int n = cbind ? clen+that.clen : clen;
		final long nnz = nonZeros+that.nonZeros;		
		boolean sp = evalSparseFormatInMemory(m, n, nnz);
		
		//init result matrix 
		if( result == null ) 
			result = new MatrixBlock(m, n, sp, nnz);
		else
			result.reset(m, n, sp, nnz);
		
		//core append operation
		//copy left and right input into output
		if( !result.sparse ) //DENSE
		{	
			if( cbind ) {
				result.copy(0, m-1, 0, clen-1, this, false);
				result.copy(0, m-1, clen, n-1, that, false);
			}
			else { //rbind
				result.copy(0, rlen-1, 0, n-1, this, false);
				result.copy(rlen, m-1, 0, n-1, that, false);	
			}
		}
		else //SPARSE
		{
			//adjust sparse rows if required
			if( !this.isEmptyBlock(false) || !that.isEmptyBlock(false) ) {
				result.allocateSparseRowsBlock();
			
				//allocate sparse rows once for cbind
				if( cbind && result.getSparseBlock() instanceof SparseBlockMCSR ) {
					SparseBlock sblock = result.getSparseBlock();
					for( int i=0; i<result.rlen; i++ ) {
						int lnnz = (int)(this.recomputeNonZeros(i, i, 0, this.clen-1)
							+ that.recomputeNonZeros(i, i, 0, that.clen-1));
						sblock.allocate(i, lnnz);
					}
				}
			}
			
			//core append operation
			result.appendToSparse(this, 0, 0);			
			if( cbind )
				result.appendToSparse(that, 0, clen);
			else //rbind
				result.appendToSparse(that, rlen, 0);
		}		
		
		//update meta data
		result.nonZeros = nnz;
		
		return result;
	}

	public MatrixBlock transposeSelfMatrixMultOperations( MatrixBlock out, MMTSJType tstype ) 	
		throws DMLRuntimeException 
	{
		return transposeSelfMatrixMultOperations(out, tstype, 1);
	}

	public MatrixBlock transposeSelfMatrixMultOperations( MatrixBlock out, MMTSJType tstype, int k ) 	
		throws DMLRuntimeException 
	{
		//check for transpose type
		if( !(tstype == MMTSJType.LEFT || tstype == MMTSJType.RIGHT) )
			throw new DMLRuntimeException("Invalid MMTSJ type '"+tstype.toString()+"'.");
		
		//setup meta data
		boolean leftTranspose = ( tstype == MMTSJType.LEFT );
		int dim = leftTranspose ? clen : rlen;
		
		//create output matrix block
		if( out == null )
			out = new MatrixBlock(dim, dim, false);
		else
			out.reset(dim, dim, false);
		
		//compute matrix mult
		if( k > 1 )
			LibMatrixMult.matrixMultTransposeSelf(this, out, leftTranspose, k);
		else
			LibMatrixMult.matrixMultTransposeSelf(this, out, leftTranspose);
		
		return out;
	}

	public MatrixBlock chainMatrixMultOperations( MatrixBlock v, MatrixBlock w, MatrixBlock out, ChainType ctype ) 
		throws DMLRuntimeException 	
	{
		return chainMatrixMultOperations(v, w, out, ctype, 1);
	}

	public MatrixBlock chainMatrixMultOperations( MatrixBlock v, MatrixBlock w, MatrixBlock out, ChainType ctype, int k ) 	
		throws DMLRuntimeException 
	{
		//check for transpose type
		if( !(ctype == ChainType.XtXv || ctype == ChainType.XtwXv || ctype == ChainType.XtXvy) )
			throw new DMLRuntimeException("Invalid mmchain type '"+ctype.toString()+"'.");
		
		//check for matching dimensions
		if( this.getNumColumns() != v.getNumRows() )
			throw new DMLRuntimeException("Dimensions mismatch on mmchain operation ("+this.getNumColumns()+" != "+v.getNumRows()+")");
		if( v!=null && v.getNumColumns() != 1 )
			throw new DMLRuntimeException("Invalid input vector (column vector expected, but ncol="+v.getNumColumns()+")");
		if( w!=null && w.getNumColumns() != 1 )
			throw new DMLRuntimeException("Invalid weight vector (column vector expected, but ncol="+w.getNumColumns()+")");
		
		//prepare result
		if( out != null )
			out.reset(clen, 1, false);
		else 
			out = new MatrixBlock(clen, 1, false);
		
		//compute matrix mult
		if( k > 1 )
			LibMatrixMult.matrixMultChain(this, v, w, out, ctype, k);
		else
			LibMatrixMult.matrixMultChain(this, v, w, out, ctype);
		
		return out;
	}

	public void permutationMatrixMultOperations( MatrixValue m2Val, MatrixValue out1Val, MatrixValue out2Val ) 	
		throws DMLRuntimeException 
	{
		permutationMatrixMultOperations(m2Val, out1Val, out2Val, 1);
	}

	public void permutationMatrixMultOperations( MatrixValue m2Val, MatrixValue out1Val, MatrixValue out2Val, int k ) 	
		throws DMLRuntimeException 
	{
		//check input types and dimensions
		MatrixBlock m2 = checkType(m2Val);
		MatrixBlock ret1 = checkType(out1Val);
		MatrixBlock ret2 = checkType(out2Val);
		
		if(this.rlen!=m2.rlen)
			throw new RuntimeException("Dimensions do not match for permutation matrix multiplication ("+this.rlen+"!="+m2.rlen+").");

		//compute permutation matrix multiplication
		if (k > 1)
			LibMatrixMult.matrixMultPermute(this, m2, ret1, ret2, k);
		else
			LibMatrixMult.matrixMultPermute(this, m2, ret1, ret2);

	}

	public final MatrixBlock leftIndexingOperations(MatrixBlock rhsMatrix, IndexRange ixrange, MatrixBlock ret, UpdateType update) 
		throws DMLRuntimeException 
	{
		return leftIndexingOperations(
				rhsMatrix, (int)ixrange.rowStart, (int)ixrange.rowEnd, 
				(int)ixrange.colStart, (int)ixrange.colEnd, ret, update);
	}
	
	/**
	 * Method to perform leftIndexing operation for a given lower and upper bounds in row and column dimensions.
	 * Updated matrix is returned as the output.
	 * 
	 * Operations to be performed: 
	 *   1) result=this; 
	 *   2) result[rowLower:rowUpper, colLower:colUpper] = rhsMatrix;
	 * 
	 * @param rhsMatrix matrix
	 * @param rl row lower
	 * @param ru row upper
	 * @param cl column lower
	 * @param cu column upper
	 * @param ret ?
	 * @param update ?
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public MatrixBlock leftIndexingOperations(MatrixBlock rhsMatrix, int rl, int ru, 
			int cl, int cu, MatrixBlock ret, UpdateType update) 
		throws DMLRuntimeException 
	{	
		// Check the validity of bounds
		if ( rl < 0 || rl >= getNumRows() || ru < rl || ru >= getNumRows()
				|| cl < 0 || cu >= getNumColumns() || cu < cl || cu >= getNumColumns() ) {
			throw new DMLRuntimeException("Invalid values for matrix indexing: ["+(rl+1)+":"+(ru+1)+"," + (cl+1)+":"+(cu+1)+"] " +
							"must be within matrix dimensions ["+getNumRows()+","+getNumColumns()+"].");
		}
		
		if ( (ru-rl+1) < rhsMatrix.getNumRows() || (cu-cl+1) < rhsMatrix.getNumColumns()) {
			throw new DMLRuntimeException("Invalid values for matrix indexing: " +
					"dimensions of the source matrix ["+rhsMatrix.getNumRows()+"x" + rhsMatrix.getNumColumns() + "] " +
					"do not match the shape of the matrix specified by indices [" +
					(rl+1) +":" + (ru+1) + ", " + (cl+1) + ":" + (cu+1) + "].");
		}
		
		MatrixBlock result = ret;		
		boolean sp = estimateSparsityOnLeftIndexing(rlen, clen, nonZeros, 
				     rhsMatrix.getNumRows(), rhsMatrix.getNumColumns(), rhsMatrix.getNonZeros());
		
		if( !update.isInPlace() ) //general case
		{
			if(result==null)
				result=new MatrixBlock(rlen, clen, sp);
			else
				result.reset(rlen, clen, sp);
			result.copy(this, sp);
		}
		else //update in-place
		{
			//use current block as in-place result
			result = this;
			
			//ensure that the current block adheres to the sparsity estimate
			//and thus implicitly the memory budget used by the compiler
			if( result.sparse && !sp )
				result.sparseToDense();
			else if( !result.sparse && sp )
				result.denseToSparse();	
			
			//ensure right sparse block representation to prevent serialization
			if( result.sparse && update != UpdateType.INPLACE_PINNED ) {
				result.sparseBlock = SparseBlockFactory.copySparseBlock(
						DEFAULT_INPLACE_SPARSEBLOCK, result.sparseBlock, false);
			}
		}
		
		//NOTE conceptually we could directly use a zeroout and copy(..., false) but
		//     since this was factors slower, we still use a full copy and subsequently
		//     copy(..., true) - however, this can be changed in the future once we 
		//     improved the performance of zeroout.
		//result = (MatrixBlockDSM) zeroOutOperations(result, new IndexRange(rowLower,rowUpper, colLower, colUpper ), false);
		
		MatrixBlock src = (MatrixBlock)rhsMatrix;

		if(rl==ru && cl==cu) { //specific case: cell update			
			//copy single value and update nnz
			result.quickSetValue(rl, cl, src.quickGetValue(0, 0));
		}		
		else { //general case
			//handle csr sparse blocks separately to avoid repeated shifting on column-wise access
			if( !result.isEmptyBlock(false) && result.sparse && result.sparseBlock instanceof SparseBlockCSR ) {
				SparseBlockCSR sblock = (SparseBlockCSR) result.sparseBlock;
				if( src.sparse )
					sblock.setIndexRange(rl, ru+1, cl, cu+1, src.getSparseBlock());
				else //dense
					sblock.setIndexRange(rl, ru+1, cl, cu+1, src.getDenseBlock(), 0, src.getNumRows()*src.getNumColumns());
				result.nonZeros = sblock.size();
			}
			//copy submatrix into result
			else {
				result.copy(rl, ru, cl, cu, src, true);
			}
		}

		return result;
	}
	
	/**
	 * Explicitly allow left indexing for scalars. Note: This operation is now 0-based.
	 * 
	 * * Operations to be performed: 
	 *   1) result=this; 
	 *   2) result[row,column] = scalar.getDoubleValue();
	 * 
	 * @param scalar scalar object
	 * @param rl row lower
	 * @param cl column lower
	 * @param ret ?
	 * @param update ?
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public MatrixBlock leftIndexingOperations(ScalarObject scalar, int rl, int cl, MatrixBlock ret, UpdateType update) 
		throws DMLRuntimeException 
	{
		double inVal = scalar.getDoubleValue();
		boolean sp = estimateSparsityOnLeftIndexing(rlen, clen, nonZeros, 1, 1, (inVal!=0)?1:0);
		
		if( !update.isInPlace() ) //general case
		{
			if(ret==null)
				ret=new MatrixBlock(rlen, clen, sp);
			else
				ret.reset(rlen, clen, sp);
			ret.copy(this, sp);
			
		}
		else //update in-place
		{
			//use current block as in-place result
			ret = this;
			
			//ensure right sparse block representation to prevent serialization
			if( ret.sparse && update != UpdateType.INPLACE_PINNED ) {
				ret.sparseBlock = SparseBlockFactory.copySparseBlock(
						DEFAULT_INPLACE_SPARSEBLOCK, ret.sparseBlock, false);
			}
		}
		
		ret.quickSetValue(rl, cl, inVal);
		return ret;
	}

	public final MatrixBlock sliceOperations(IndexRange ixrange, MatrixBlock ret) throws DMLRuntimeException {
		return sliceOperations(
				(int)ixrange.rowStart, (int)ixrange.rowEnd, 
				(int)ixrange.colStart, (int)ixrange.colEnd, ret);
	}
	
	/**
	 * Method to perform rangeReIndex operation for a given lower and upper bounds in row and column dimensions.
	 * Extracted submatrix is returned as "result". Note: This operation is now 0-based.
	 * 
	 * @param rl row lower
	 * @param ru row upper
	 * @param cl column lower
	 * @param cu column upper
	 * @param ret ?
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public MatrixBlock sliceOperations(int rl, int ru, int cl, int cu, CacheBlock ret) 
		throws DMLRuntimeException 
	{	
		// check the validity of bounds
		if ( rl < 0 || rl >= getNumRows() || ru < rl || ru >= getNumRows()
				|| cl < 0 || cu >= getNumColumns() || cu < cl || cu >= getNumColumns() ) {
			throw new DMLRuntimeException("Invalid values for matrix indexing: ["+(rl+1)+":"+(ru+1)+"," + (cl+1)+":"+(cu+1)+"] " +
							"must be within matrix dimensions ["+getNumRows()+","+getNumColumns()+"]");
		}
		
		// Output matrix will have the same sparsity as that of the input matrix.
		// (assuming a uniform distribution of non-zeros in the input)
		MatrixBlock result=checkType((MatrixBlock)ret);
		long estnnz= (long) ((double)this.nonZeros/rlen/clen*(ru-rl+1)*(cu-cl+1));
		boolean result_sparsity = this.sparse && MatrixBlock.evalSparseFormatInMemory(ru-rl+1, cu-cl+1, estnnz);
		if(result==null)
			result=new MatrixBlock(ru-rl+1, cu-cl+1, result_sparsity, estnnz);
		else
			result.reset(ru-rl+1, cu-cl+1, result_sparsity, estnnz);
		
		// actual slice operation
		if( rl==0 && ru==rlen-1 && cl==0 && cu==clen-1 ) {
			// copy if entire matrix required
			result.copy( this );
		}
		else //general case
		{
			//core slicing operation (nnz maintained internally)
			if (sparse) 
				sliceSparse(rl, ru, cl, cu, result);
			else 
				sliceDense(rl, ru, cl, cu, result);
		}
		
		return result;
	}

	private void sliceSparse(int rl, int ru, int cl, int cu, MatrixBlock dest) 
		throws DMLRuntimeException
	{
		//check for early abort
		if( isEmptyBlock(false) ) 
			return;
		
		if( cl==cu ) //COLUMN VECTOR 
		{
			//note: always dense dest
			dest.allocateDenseBlock();
			for( int i=rl; i<=ru; i++ ) {
				if( !sparseBlock.isEmpty(i) ) {
					double val = sparseBlock.get(i, cl);
					if( val != 0 ) {
						dest.denseBlock[i-rl] = val;
						dest.nonZeros++;
					}
				}
			}
		}
		else if( rl==ru && cl==0 && cu==clen-1 ) //ROW VECTOR 
		{
			//note: always sparse dest, but also works for dense
			dest.appendRow(0, sparseBlock.get(rl));
		}
		else //general case (sparse/dense dest)
		{
			for(int i=rl; i <= ru; i++) 
				if( !sparseBlock.isEmpty(i)) 
				{
					int apos = sparseBlock.pos(i);
					int alen = sparseBlock.size(i);
					int[] aix = sparseBlock.indexes(i);
					double[] avals = sparseBlock.values(i);
					int astart = (cl>0)?sparseBlock.posFIndexGTE(i, cl) : apos;
					if( astart != -1 )
						for( int j=astart; j<apos+alen && aix[j] <= cu; j++ )
							dest.appendValue(i-rl, aix[j]-cl, avals[j]);	
				}
		}
	}

	private void sliceDense(int rl, int ru, int cl, int cu, MatrixBlock dest) 
		throws DMLRuntimeException
	{
		//ensure allocated input/output blocks
		if( denseBlock == null )
			return;
		dest.allocateDenseBlock();

		//indexing operation
		if( cl==cu ) //COLUMN INDEXING
		{
			if( clen==1 ) //vector -> vector
			{
				System.arraycopy(denseBlock, rl, dest.denseBlock, 0, ru-rl+1);
			}
			else //matrix -> vector
			{
				//IBM JVM bug (JDK7) causes crash for certain cl/cu values (e.g., divide by zero for 4) 
				//for( int i=rl*clen+cl, ix=0; i<=ru*clen+cu; i+=clen, ix++ )
				//	dest.denseBlock[ix] = denseBlock[i];
				int len = clen;
				for( int i=rl*len+cl, ix=0; i<=ru*len+cu; i+=len, ix++ )
					dest.denseBlock[ix] = denseBlock[i];
			}
		}
		else // GENERAL RANGE INDEXING
		{
			//IBM JVM bug (JDK7) causes crash for certain cl/cu values (e.g., divide by zero for 4) 
			//for(int i = rl, ix1 = rl*clen+cl, ix2=0; i <= ru; i++, ix1+=clen, ix2+=dest.clen) 
			//	System.arraycopy(denseBlock, ix1, dest.denseBlock, ix2, dest.clen);
			int len1 = clen;
			int len2 = dest.clen;
			for(int i = rl, ix1 = rl*len1+cl, ix2=0; i <= ru; i++, ix1+=len1, ix2+=len2) 
				System.arraycopy(denseBlock, ix1, dest.denseBlock, ix2, len2);
		}
		
		//compute nnz of output (not maintained due to native calls)
		dest.recomputeNonZeros();
	}
	
	public void sliceOperations(ArrayList<IndexedMatrixValue> outlist, IndexRange range, int rowCut, int colCut, 
			int normalBlockRowFactor, int normalBlockColFactor, int boundaryRlen, int boundaryClen)
	{
		MatrixBlock topleft=null, topright=null, bottomleft=null, bottomright=null;
		Iterator<IndexedMatrixValue> p=outlist.iterator();
		int blockRowFactor=normalBlockRowFactor, blockColFactor=normalBlockColFactor;
		if(rowCut>range.rowEnd)
			blockRowFactor=boundaryRlen;
		if(colCut>range.colEnd)
			blockColFactor=boundaryClen;
		
		int minrowcut=(int)Math.min(rowCut,range.rowEnd);
		int mincolcut=(int)Math.min(colCut, range.colEnd);
		int maxrowcut=(int)Math.max(rowCut, range.rowStart);
		int maxcolcut=(int)Math.max(colCut, range.colStart);
		
		if(range.rowStart<rowCut && range.colStart<colCut)
		{
			topleft=(MatrixBlock) p.next().getValue();
			//topleft.reset(blockRowFactor, blockColFactor, 
			//		checkSparcityOnSlide(rowCut-(int)range.rowStart, colCut-(int)range.colStart, blockRowFactor, blockColFactor));
			
			topleft.reset(blockRowFactor, blockColFactor, 
					estimateSparsityOnSlice(minrowcut-(int)range.rowStart, mincolcut-(int)range.colStart, blockRowFactor, blockColFactor));
		}
		if(range.rowStart<rowCut && range.colEnd>=colCut)
		{
			topright=(MatrixBlock) p.next().getValue();
			topright.reset(blockRowFactor, boundaryClen, 
					estimateSparsityOnSlice(minrowcut-(int)range.rowStart, (int)range.colEnd-maxcolcut+1, blockRowFactor, boundaryClen));
		}
		if(range.rowEnd>=rowCut && range.colStart<colCut)
		{
			bottomleft=(MatrixBlock) p.next().getValue();
			bottomleft.reset(boundaryRlen, blockColFactor, 
					estimateSparsityOnSlice((int)range.rowEnd-maxrowcut+1, mincolcut-(int)range.colStart, boundaryRlen, blockColFactor));
		}
		if(range.rowEnd>=rowCut && range.colEnd>=colCut)
		{
			bottomright=(MatrixBlock) p.next().getValue();
			bottomright.reset(boundaryRlen, boundaryClen, 
					estimateSparsityOnSlice((int)range.rowEnd-maxrowcut+1, (int)range.colEnd-maxcolcut+1, boundaryRlen, boundaryClen));
		}
		
		if(sparse)
		{
			if(sparseBlock!=null)
			{
				int r=(int)range.rowStart;
				for(; r<Math.min(Math.min(rowCut, sparseBlock.numRows()), range.rowEnd+1); r++)
					sliceHelp(r, range, colCut, topleft, topright, normalBlockRowFactor-rowCut, normalBlockRowFactor, normalBlockColFactor);
				
				for(; r<=Math.min(range.rowEnd, sparseBlock.numRows()-1); r++)
					sliceHelp(r, range, colCut, bottomleft, bottomright, -rowCut, normalBlockRowFactor, normalBlockColFactor);
				//System.out.println("in: \n"+this);
				//System.out.println("outlist: \n"+outlist);
			}
		}else
		{
			if(denseBlock!=null)
			{
				int i=((int)range.rowStart)*clen;
				int r=(int) range.rowStart;
				for(; r<Math.min(rowCut, range.rowEnd+1); r++)
				{
					int c=(int) range.colStart;
					for(; c<Math.min(colCut, range.colEnd+1); c++)
						topleft.appendValue(r+normalBlockRowFactor-rowCut, c+normalBlockColFactor-colCut, denseBlock[i+c]);
					for(; c<=range.colEnd; c++)
						topright.appendValue(r+normalBlockRowFactor-rowCut, c-colCut, denseBlock[i+c]);
					i+=clen;
				}
				
				for(; r<=range.rowEnd; r++)
				{
					int c=(int) range.colStart;
					for(; c<Math.min(colCut, range.colEnd+1); c++)
						bottomleft.appendValue(r-rowCut, c+normalBlockColFactor-colCut, denseBlock[i+c]);
					for(; c<=range.colEnd; c++)
						bottomright.appendValue(r-rowCut, c-colCut, denseBlock[i+c]);
					i+=clen;
				}
			}
		}
	}
	
	private void sliceHelp(int r, IndexRange range, int colCut, MatrixBlock left, MatrixBlock right, int rowOffset, int normalBlockRowFactor, int normalBlockColFactor)
	{
		if(sparseBlock.isEmpty(r)) 
			return;
		
		int[] cols=sparseBlock.indexes(r);
		double[] values=sparseBlock.values(r);
		int start=sparseBlock.posFIndexGTE(r, (int)range.colStart);
		if(start<0) 
			return;
		int end=sparseBlock.posFIndexLTE(r, (int)range.colEnd);
		if(end<0 || start>end) 
			return;
		
		//actual slice operation
		for(int i=start; i<=end; i++) {
			if(cols[i]<colCut)
				left.appendValue(r+rowOffset, cols[i]+normalBlockColFactor-colCut, values[i]);
			else
				right.appendValue(r+rowOffset, cols[i]-colCut, values[i]);
		}
	}
	
	@Override
	//This the append operations for MR side
	//nextNCol is the number columns for the block right of block v2
	public void appendOperations(MatrixValue v2,
			ArrayList<IndexedMatrixValue> outlist, int blockRowFactor,
			int blockColFactor, boolean cbind, boolean m2IsLast, int nextNCol)
			throws DMLRuntimeException 
	{	
		MatrixBlock m2 = (MatrixBlock)v2;
		
		//case 1: copy lhs and rhs to output
		if( cbind && clen==blockColFactor 
			|| !cbind && rlen==blockRowFactor )
		{
			((MatrixBlock) outlist.get(0).getValue()).copy(this);
			((MatrixBlock) outlist.get(1).getValue()).copy(m2);
		}
		//case 2: append part of rhs to lhs, append to 2nd output if necessary
		else
		{
			//single output block (via plain append operation)
			if( cbind && clen + m2.clen < blockColFactor
				|| !cbind && rlen + m2.rlen < blockRowFactor )
			{
				appendOperations(m2, (MatrixBlock) outlist.get(0).getValue(), cbind);
			}
			//two output blocks (via slice and append)
			else
			{
				//prepare output block 1
				MatrixBlock ret1 = (MatrixBlock) outlist.get(0).getValue();
				int lrlen1 = cbind ? rlen-1 : blockRowFactor-rlen-1;
				int lclen1 = cbind ? blockColFactor-clen-1 : clen-1;
				MatrixBlock tmp1 = m2.sliceOperations(0, lrlen1, 0, lclen1, new MatrixBlock());
				appendOperations(tmp1, ret1, cbind);
	
				//prepare output block 2
				MatrixBlock ret2 = (MatrixBlock) outlist.get(1).getValue();
				if( cbind )
					m2.sliceOperations(0, rlen-1, lclen1+1, m2.clen-1, ret2);
				else
					m2.sliceOperations(lrlen1+1, m2.rlen-1, 0, clen-1, ret2);
			}
		}
	}

	public MatrixValue zeroOutOperations(MatrixValue result, IndexRange range, boolean complementary)
			throws DMLRuntimeException 
	{
		checkType(result);
		double currentSparsity=(double)nonZeros/(double)rlen/(double)clen;
		double estimatedSps=currentSparsity*(double)(range.rowEnd-range.rowStart+1)
		                    *(double)(range.colEnd-range.colStart+1)/(double)rlen/(double)clen;
		if(!complementary)
			estimatedSps=currentSparsity-estimatedSps;
		
		boolean lsparse = evalSparseFormatInMemory(rlen, clen, (long)(estimatedSps*rlen*clen));
		
		if(result==null)
			result=new MatrixBlock(rlen, clen, lsparse, (int)(estimatedSps*rlen*clen));
		else
			result.reset(rlen, clen, lsparse, (int)(estimatedSps*rlen*clen));
		
		
		if(sparse)
		{
			if(sparseBlock!=null)
			{
				if(!complementary)//if zero out
				{
					for(int r=0; r<Math.min((int)range.rowStart, sparseBlock.numRows()); r++)
						((MatrixBlock) result).appendRow(r, sparseBlock.get(r));
					for(int r=Math.min((int)range.rowEnd+1, sparseBlock.numRows()); r<Math.min(rlen, sparseBlock.numRows()); r++)
						((MatrixBlock) result).appendRow(r, sparseBlock.get(r));
				}
				for(int r=(int)range.rowStart; r<=Math.min(range.rowEnd, sparseBlock.numRows()-1); r++)
				{
					if(sparseBlock.isEmpty(r)) 
						continue;
					int[] cols=sparseBlock.indexes(r);
					double[] values=sparseBlock.values(r);
					
					if(complementary)//if selection
					{
						int start=sparseBlock.posFIndexGTE(r,(int)range.colStart);
						if(start<0) continue;
						int end=sparseBlock.posFIndexGT(r,(int)range.colEnd);
						if(end<0 || start>end) 
							continue;
						
						for(int i=start; i<end; i++)
						{
							((MatrixBlock) result).appendValue(r, cols[i], values[i]);
						}
					}else
					{
						int pos = sparseBlock.pos(r);
						int len = sparseBlock.size(r);
						int start=sparseBlock.posFIndexGTE(r,(int)range.colStart);
						if(start<0) start=pos+len;
						int end=sparseBlock.posFIndexGT(r,(int)range.colEnd);
						if(end<0) end=pos+len;
						
						for(int i=pos; i<start; i++)
						{
							((MatrixBlock) result).appendValue(r, cols[i], values[i]);
						}
						for(int i=end; i<pos+len; i++)
						{
							((MatrixBlock) result).appendValue(r, cols[i], values[i]);
						}
					}
				}
			}
		}else
		{
			if(denseBlock!=null)
			{
				if(complementary)//if selection
				{
					int offset=((int)range.rowStart)*clen;
					for(int r=(int) range.rowStart; r<=range.rowEnd; r++)
					{
						for(int c=(int) range.colStart; c<=range.colEnd; c++)
							((MatrixBlock) result).appendValue(r, c, denseBlock[offset+c]);
						offset+=clen;
					}
				}else
				{
					int offset=0;
					int r=0;
					for(; r<(int)range.rowStart; r++)
						for(int c=0; c<clen; c++, offset++)
							((MatrixBlock) result).appendValue(r, c, denseBlock[offset]);
					
					for(; r<=(int)range.rowEnd; r++)
					{
						for(int c=0; c<(int)range.colStart; c++)
							((MatrixBlock) result).appendValue(r, c, denseBlock[offset+c]);
						for(int c=(int)range.colEnd+1; c<clen; c++)
							((MatrixBlock) result).appendValue(r, c, denseBlock[offset+c]);
						offset+=clen;
					}
					
					for(; r<rlen; r++)
						for(int c=0; c<clen; c++, offset++)
							((MatrixBlock) result).appendValue(r, c, denseBlock[offset]);
				}
				
			}
		}
		return result;
	}
	
	public MatrixValue aggregateUnaryOperations(AggregateUnaryOperator op, MatrixValue result, 
			int blockingFactorRow, int blockingFactorCol, MatrixIndexes indexesIn)
		throws DMLRuntimeException
	{
		return aggregateUnaryOperations(op, result, 
				blockingFactorRow, blockingFactorCol, indexesIn, false);
	}
	
	
	public MatrixValue aggregateUnaryOperations(AggregateUnaryOperator op, MatrixValue result, 
			int blockingFactorRow, int blockingFactorCol, MatrixIndexes indexesIn, boolean inCP) 
		throws DMLRuntimeException
	{
		CellIndex tempCellIndex = new CellIndex(-1,-1);
		op.indexFn.computeDimension(rlen, clen, tempCellIndex);
		if(op.aggOp.correctionExists)
		{
			switch(op.aggOp.correctionLocation)
			{
				case LASTROW: 
					tempCellIndex.row++; 
					break;
				case LASTCOLUMN: 
					tempCellIndex.column++; 
					break;
				case LASTTWOROWS: 
					tempCellIndex.row+=2; 
					break;
				case LASTTWOCOLUMNS: 
					tempCellIndex.column+=2; 
					break;
				case LASTFOURROWS:
					tempCellIndex.row+=4;
					break;
				case LASTFOURCOLUMNS:
					tempCellIndex.column+=4;
					break;
				default:
					throw new DMLRuntimeException("unrecognized correctionLocation: "+op.aggOp.correctionLocation);	
			}
		}
		if(result==null)
			result=new MatrixBlock(tempCellIndex.row, tempCellIndex.column, false);
		else
			result.reset(tempCellIndex.row, tempCellIndex.column, false);
		
		MatrixBlock ret = (MatrixBlock) result;
		if( LibMatrixAgg.isSupportedUnaryAggregateOperator(op) ) {
			if( op.getNumThreads() > 1 )
				LibMatrixAgg.aggregateUnaryMatrix(this, ret, op, op.getNumThreads());
			else
				LibMatrixAgg.aggregateUnaryMatrix(this, ret, op);
			LibMatrixAgg.recomputeIndexes(ret, op, blockingFactorRow, blockingFactorCol, indexesIn);
		}
		else if(op.sparseSafe)
			sparseAggregateUnaryHelp(op, ret, blockingFactorRow, blockingFactorCol, indexesIn);
		else
			denseAggregateUnaryHelp(op, ret, blockingFactorRow, blockingFactorCol, indexesIn);
		
		if(op.aggOp.correctionExists && inCP)
			((MatrixBlock)result).dropLastRowsOrColums(op.aggOp.correctionLocation);
		
		return ret;
	}
	
	private void sparseAggregateUnaryHelp(AggregateUnaryOperator op, MatrixBlock result,
			int blockingFactorRow, int blockingFactorCol, MatrixIndexes indexesIn) throws DMLRuntimeException
	{
		//initialize result
		if(op.aggOp.initialValue!=0)
			result.reset(result.rlen, result.clen, op.aggOp.initialValue);
		
		CellIndex tempCellIndex = new CellIndex(-1,-1);
		KahanObject buffer=new KahanObject(0,0);
		int r = 0, c = 0;
		
		if(sparse)
		{
			if(sparseBlock!=null)
			{
				SparseBlock a = sparseBlock;
				
				for(r=0; r<Math.min(rlen, a.numRows()); r++)
				{
					if(a.isEmpty(r)) 
						continue;
					int apos = a.pos(r);
					int alen = a.size(r);
					int[] aix = a.indexes(r);
					double[] aval = a.values(r);
					
					for(int i=apos; i<apos+alen; i++) {
						tempCellIndex.set(r, aix[i]);
						op.indexFn.execute(tempCellIndex, tempCellIndex);
						incrementalAggregateUnaryHelp(op.aggOp, result, 
								tempCellIndex.row, tempCellIndex.column, aval[i], buffer);
					}
				}
			}
		}
		else
		{
			if(denseBlock!=null)
			{
				int limit=rlen*clen;
				for(int i=0; i<limit; i++)
				{
					r=i/clen;
					c=i%clen;
					tempCellIndex.set(r, c);
					op.indexFn.execute(tempCellIndex, tempCellIndex);
					incrementalAggregateUnaryHelp(op.aggOp, result, tempCellIndex.row, tempCellIndex.column, denseBlock[i], buffer);
				}
			}
		}
	}
	
	private void denseAggregateUnaryHelp(AggregateUnaryOperator op, MatrixBlock result,
			int blockingFactorRow, int blockingFactorCol, MatrixIndexes indexesIn) throws DMLRuntimeException
	{
		//initialize 
		if(op.aggOp.initialValue!=0)
			result.reset(result.rlen, result.clen, op.aggOp.initialValue);
		
		CellIndex tempCellIndex = new CellIndex(-1,-1);
		KahanObject buffer=new KahanObject(0,0);
		
		for(int i=0; i<rlen; i++)
			for(int j=0; j<clen; j++)
			{
				tempCellIndex.set(i, j);
				op.indexFn.execute(tempCellIndex, tempCellIndex);

				if(op.aggOp.correctionExists
				   && op.aggOp.correctionLocation == CorrectionLocationType.LASTCOLUMN
				   && op.aggOp.increOp.fn instanceof Builtin 
				   && ( ((Builtin)(op.aggOp.increOp.fn)).bFunc == Builtin.BuiltinCode.MAXINDEX
				        || ((Builtin)(op.aggOp.increOp.fn)).bFunc == Builtin.BuiltinCode.MININDEX) 
				        ){
					double currMaxValue = result.quickGetValue(i, 1);
					long newMaxIndex = UtilFunctions.computeCellIndex(indexesIn.getColumnIndex(), blockingFactorCol, j);
					double newMaxValue = quickGetValue(i, j);
					double update = op.aggOp.increOp.fn.execute(newMaxValue, currMaxValue);
						   
					//System.out.println("currV="+currMaxValue+",newV="+newMaxValue+",newIX="+newMaxIndex+",update="+update);
					if(update == 1){
						result.quickSetValue(i, 0, newMaxIndex);
						result.quickSetValue(i, 1, newMaxValue);
					}
				}else
					incrementalAggregateUnaryHelp(op.aggOp, result, tempCellIndex.row, tempCellIndex.column, quickGetValue(i,j), buffer);
			}
	}
	
	
	private void incrementalAggregateUnaryHelp(AggregateOperator aggOp, MatrixBlock result, int row, int column, 
			double newvalue, KahanObject buffer) throws DMLRuntimeException
	{
		if(aggOp.correctionExists)
		{
			if(aggOp.correctionLocation==CorrectionLocationType.LASTROW || aggOp.correctionLocation==CorrectionLocationType.LASTCOLUMN)
			{
				int corRow=row, corCol=column;
				if(aggOp.correctionLocation==CorrectionLocationType.LASTROW)//extra row
					corRow++;
				else if(aggOp.correctionLocation==CorrectionLocationType.LASTCOLUMN)
					corCol++;
				else
					throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correctionLocation);
				
				buffer._sum=result.quickGetValue(row, column);
				buffer._correction=result.quickGetValue(corRow, corCol);
				buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newvalue);
				result.quickSetValue(row, column, buffer._sum);
				result.quickSetValue(corRow, corCol, buffer._correction);
			}else if(aggOp.correctionLocation==CorrectionLocationType.NONE)
			{
				throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correctionLocation);
			}else// for mean
			{
				int corRow=row, corCol=column;
				int countRow=row, countCol=column;
				if(aggOp.correctionLocation==CorrectionLocationType.LASTTWOROWS)
				{
					countRow++;
					corRow+=2;
				}
				else if(aggOp.correctionLocation==CorrectionLocationType.LASTTWOCOLUMNS)
				{
					countCol++;
					corCol+=2;
				}
				else
					throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correctionLocation);
				buffer._sum=result.quickGetValue(row, column);
				buffer._correction=result.quickGetValue(corRow, corCol);
				double count=result.quickGetValue(countRow, countCol)+1.0;
				buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newvalue, count);
				result.quickSetValue(row, column, buffer._sum);
				result.quickSetValue(corRow, corCol, buffer._correction);
				result.quickSetValue(countRow, countCol, count);
			}
			
		}else
		{
			newvalue=aggOp.increOp.fn.execute(result.quickGetValue(row, column), newvalue);
			result.quickSetValue(row, column, newvalue);
		}
	}

	public void dropLastRowsOrColums(CorrectionLocationType correctionLocation) 
	{
		//do nothing 
		if(   correctionLocation==CorrectionLocationType.NONE 
	       || correctionLocation==CorrectionLocationType.INVALID )
		{
			return;
		}
		
		//determine number of rows/cols to be removed
		int step;
		switch (correctionLocation) {
			case LASTROW:
			case LASTCOLUMN:
				step = 1;
				break;
			case LASTTWOROWS:
			case LASTTWOCOLUMNS:
				step = 2;
				break;
			case LASTFOURROWS:
			case LASTFOURCOLUMNS:
				step = 4;
				break;
			default:
				step = 0;
		}

		
		//e.g., colSums, colMeans, colMaxs, colMeans, colVars
		if(   correctionLocation==CorrectionLocationType.LASTROW
		   || correctionLocation==CorrectionLocationType.LASTTWOROWS
		   || correctionLocation==CorrectionLocationType.LASTFOURROWS )
		{
			if( sparse ) //SPARSE
			{
				if(sparseBlock!=null)
					for(int i=1; i<=step; i++)
						if(!sparseBlock.isEmpty(rlen-i))
							this.nonZeros-=sparseBlock.size(rlen-i);
			}
			else //DENSE
			{
				if(denseBlock!=null)
					for(int i=(rlen-step)*clen; i<rlen*clen; i++)
						if(denseBlock[i]!=0)
							this.nonZeros--;
			}
			
			//just need to shrink the dimension, the deleted rows won't be accessed
			rlen -= step;
		}
		
		//e.g., rowSums, rowsMeans, rowsMaxs, rowsMeans, rowVars
		else if(   correctionLocation==CorrectionLocationType.LASTCOLUMN
		        || correctionLocation==CorrectionLocationType.LASTTWOCOLUMNS
		        || correctionLocation==CorrectionLocationType.LASTFOURCOLUMNS )
		{
			if(sparse) //SPARSE
			{
				if(sparseBlock!=null)
				{
					for(int r=0; r<Math.min(rlen, sparseBlock.numRows()); r++)
						if(!sparseBlock.isEmpty(r))
						{
							int newSize=sparseBlock.posFIndexGTE(r, clen-step);
							if(newSize >= 0)
							{
								this.nonZeros-=sparseBlock.size(r)-newSize;
								int pos = sparseBlock.pos(r);
								int cl = sparseBlock.indexes(r)[pos+newSize-1];
								sparseBlock.deleteIndexRange(r, cl+1, clen);
								//TODO perf sparse block: truncate replaced by deleteIndexRange
							}
						}
				}
			}
			else //DENSE
			{
				if(this.denseBlock!=null)
				{
					//the first row doesn't need to be copied
					int targetIndex=clen-step;
					int sourceOffset=clen;
					this.nonZeros=0;
					for(int i=0; i<targetIndex; i++)
						if(denseBlock[i]!=0)
							this.nonZeros++;
					
					//start from the 2nd row
					for(int r=1; r<rlen; r++)
					{
						for(int c=0; c<clen-step; c++)
						{
							if((denseBlock[targetIndex]=denseBlock[sourceOffset+c])!=0)
								this.nonZeros++;
							targetIndex++;
						}
						sourceOffset+=clen;
					}
				}
			}
			
			clen -= step;
		}
	}

	public CM_COV_Object cmOperations(CMOperator op) 
		throws DMLRuntimeException 
	{
		// dimension check for input column vectors
		if ( this.getNumColumns() != 1) {
			throw new DMLRuntimeException("Central Moment can not be computed on [" 
					+ this.getNumRows() + "," + this.getNumColumns() + "] matrix.");
		}
		
		CM_COV_Object cmobj = new CM_COV_Object();
		
		// empty block handling (important for result corretness, otherwise
		// we get a NaN due to 0/0 on reading out the required result)
		if( isEmptyBlock(false) ) {
			op.fn.execute(cmobj, 0.0, getNumRows());
			return cmobj;
		}
		
		int nzcount = 0;
		if(sparse && sparseBlock!=null) //SPARSE
		{
			for(int r=0; r<Math.min(rlen, sparseBlock.numRows()); r++)
			{
				if(sparseBlock.isEmpty(r)) 
					continue;
				int apos = sparseBlock.pos(r);
				int alen = sparseBlock.size(r);
				double[] avals = sparseBlock.values(r);
				for(int i=apos; i<apos+alen; i++) {
					op.fn.execute(cmobj, avals[i]);
					nzcount++;
				}
			}
			// account for zeros in the vector
			op.fn.execute(cmobj, 0.0, this.getNumRows()-nzcount);
		}
		else if(denseBlock!=null)  //DENSE
		{
			//always vector (see check above)
			for(int i=0; i<rlen; i++)
				op.fn.execute(cmobj, denseBlock[i]);
		}

		return cmobj;
	}
		
	public CM_COV_Object cmOperations(CMOperator op, MatrixBlock weights) 
		throws DMLRuntimeException 
	{
		/* this._data must be a 1 dimensional vector */
		if ( this.getNumColumns() != 1 || weights.getNumColumns() != 1) {
			throw new DMLRuntimeException("Central Moment can be computed only on 1-dimensional column matrices.");
		}
		if ( this.getNumRows() != weights.getNumRows() || this.getNumColumns() != weights.getNumColumns()) {
			throw new DMLRuntimeException("Covariance: Mismatching dimensions between input and weight matrices - " +
					"["+this.getNumRows()+","+this.getNumColumns() +"] != [" 
					+ weights.getNumRows() + "," + weights.getNumColumns() +"]");
		}
		
		CM_COV_Object cmobj = new CM_COV_Object();
		if (sparse && sparseBlock!=null) //SPARSE
		{
			for(int i=0; i < rlen; i++) 
				op.fn.execute(cmobj, this.quickGetValue(i,0), weights.quickGetValue(i,0));
		}
		else if(denseBlock!=null) //DENSE
		{
			//always vectors (see check above)
			if( !weights.sparse )
			{
				//both dense vectors (default case)
				if(weights.denseBlock!=null)
					for( int i=0; i<rlen; i++ )
						op.fn.execute(cmobj, denseBlock[i], weights.denseBlock[i]);
			}
			else
			{
				for(int i=0; i<rlen; i++) 
					op.fn.execute(cmobj, denseBlock[i], weights.quickGetValue(i,0) );
			}
		}
		
		return cmobj;
	}
	
	public CM_COV_Object covOperations(COVOperator op, MatrixBlock that) 
		throws DMLRuntimeException 
	{
		/* this._data must be a 1 dimensional vector */
		if ( this.getNumColumns() != 1 || that.getNumColumns() != 1 ) {
			throw new DMLRuntimeException("Covariance can be computed only on 1-dimensional column matrices."); 
		}
		if ( this.getNumRows() != that.getNumRows() || this.getNumColumns() != that.getNumColumns()) {
			throw new DMLRuntimeException("Covariance: Mismatching input matrix dimensions - " +
					"["+this.getNumRows()+","+this.getNumColumns() +"] != [" 
					+ that.getNumRows() + "," + that.getNumColumns() +"]");
		}
		
		CM_COV_Object covobj = new CM_COV_Object();
		if(sparse && sparseBlock!=null) //SPARSE
		{
			for(int i=0; i < rlen; i++ ) 
				op.fn.execute(covobj, this.quickGetValue(i,0), that.quickGetValue(i,0));
		}
		else if(denseBlock!=null) //DENSE
		{
			//always vectors (see check above)
			if( !that.sparse )
			{
				//both dense vectors (default case)
				if(that.denseBlock!=null)
					for( int i=0; i<rlen; i++ )
						op.fn.execute(covobj, denseBlock[i], that.denseBlock[i]);
			}
			else
			{
				for(int i=0; i<rlen; i++)
					op.fn.execute(covobj, denseBlock[i], that.quickGetValue(i,0));
			}
		}
		
		return covobj;
	}
	
	public CM_COV_Object covOperations(COVOperator op, MatrixBlock that, MatrixBlock weights) 
		throws DMLRuntimeException 
	{
		/* this._data must be a 1 dimensional vector */
		if ( this.getNumColumns() != 1 || that.getNumColumns() != 1 || weights.getNumColumns() != 1) {
			throw new DMLRuntimeException("Covariance can be computed only on 1-dimensional column matrices."); 
		}
		if ( this.getNumRows() != that.getNumRows() || this.getNumColumns() != that.getNumColumns()) {
			throw new DMLRuntimeException("Covariance: Mismatching input matrix dimensions - " +
					"["+this.getNumRows()+","+this.getNumColumns() +"] != [" 
					+ that.getNumRows() + "," + that.getNumColumns() +"]");
		}
		if ( this.getNumRows() != weights.getNumRows() || this.getNumColumns() != weights.getNumColumns()) {
			throw new DMLRuntimeException("Covariance: Mismatching dimensions between input and weight matrices - " +
					"["+this.getNumRows()+","+this.getNumColumns() +"] != [" 
					+ weights.getNumRows() + "," + weights.getNumColumns() +"]");
		}
		
		CM_COV_Object covobj = new CM_COV_Object();
		if(sparse && sparseBlock!=null) //SPARSE
		{
			for(int i=0; i < rlen; i++ ) 
				op.fn.execute(covobj, this.quickGetValue(i,0), that.quickGetValue(i,0), weights.quickGetValue(i,0));
		}
		else if(denseBlock!=null) //DENSE
		{
			//always vectors (see check above)
			if( !that.sparse && !weights.sparse )
			{
				//all dense vectors (default case)
				if(that.denseBlock!=null)
					for( int i=0; i<rlen; i++ )
						op.fn.execute(covobj, denseBlock[i], that.denseBlock[i], weights.denseBlock[i]);
			}
			else
			{
				for(int i=0; i<rlen; i++)
					op.fn.execute(covobj, denseBlock[i], that.quickGetValue(i,0), weights.quickGetValue(i,0));
			}
		}
		
		return covobj;
	}

	public MatrixValue sortOperations(MatrixValue weights, MatrixValue result) throws DMLRuntimeException {
		boolean wtflag = (weights!=null);
		
		MatrixBlock wts= (weights == null ? null : checkType(weights));
		checkType(result);
		
		if ( getNumColumns() != 1 ) {
			throw new DMLRuntimeException("Invalid input dimensions (" + getNumRows() + "x" + getNumColumns() + ") to sort operation.");
		}
		if ( wts != null && wts.getNumColumns() != 1 ) {
			throw new DMLRuntimeException("Invalid weight dimensions (" + wts.getNumRows() + "x" + wts.getNumColumns() + ") to sort operation.");
		}
		
		// prepare result, currently always dense
		// #rows in temp matrix = 1 + #nnz in the input ( 1 is for the "zero" value)
		int dim1 = (int) (1+this.getNonZeros());
		if(result==null)
			result=new MatrixBlock(dim1, 2, false);
		else
			result.reset(dim1, 2, false);
		
		// Copy the input elements into a temporary array for sorting
		// First column is data and second column is weights
		// (since the inputs are vectors, they are likely dense - hence quickget is sufficient)
		MatrixBlock tdw = new MatrixBlock(dim1, 2, false);
		double d, w, zero_wt=0;
		int ind = 1;
		if( wtflag ) // w/ weights
		{
			for ( int i=0; i<rlen; i++ ) {
				d = quickGetValue(i,0);
				w = wts.quickGetValue(i,0);
				if ( d != 0 ) {
					tdw.quickSetValue(ind, 0, d);
					tdw.quickSetValue(ind, 1, w);
					ind++;
				}
				else
					zero_wt += w;
			}
		} 
		else //w/o weights
		{
			zero_wt = getNumRows() - getNonZeros();
			for( int i=0; i<rlen; i++ ) {
				d = quickGetValue(i,0);
				if( d != 0 ){
					tdw.quickSetValue(ind, 0, d);
					tdw.quickSetValue(ind, 1, 1);
					ind++;
				}
			}
		}
		tdw.quickSetValue(0, 0, 0.0);
		tdw.quickSetValue(0, 1, zero_wt); //num zeros in input
		
		// Sort td and tw based on values inside td (ascending sort), incl copy into result
		SortIndex sfn = SortIndex.getSortIndexFnObject(1, false, false);
		ReorgOperator rop = new ReorgOperator(sfn);
		LibMatrixReorg.reorg(tdw, (MatrixBlock)result, rop);
		
		return result;
	}
	
	public double interQuartileMean() throws DMLRuntimeException {
		
		double sum_wt = sumWeightForQuantile();
		
		double q25d = 0.25*sum_wt;
		double q75d = 0.75*sum_wt;
		
		int q25i = (int) Math.ceil(q25d);
		int q75i = (int) Math.ceil(q75d);
		
		// skip until (but excluding) q25
		int t = 0, i=-1;
		while(i<getNumRows() && t < q25i) {
			i++;
			//System.out.println("    " + i + ": " + quickGetValue(i,0) + "," + quickGetValue(i,1));
			t += quickGetValue(i,1);
		}
		// compute the portion of q25
		double runningSum = (t-q25d)*quickGetValue(i,0);
		
		// add until (including) q75
		while(i<getNumRows() && t < q75i) {
			i++;
			t += quickGetValue(i,1);
			runningSum += quickGetValue(i,0)*quickGetValue(i,1);
		}
		// subtract additional portion of q75
		runningSum -= (t-q75d)*quickGetValue(i,0);
		
		return runningSum/(sum_wt*0.5);
	}
	
	/**
	 * Computes the weighted interQuartileMean.
	 * The matrix block ("this" pointer) has two columns, in which the first column 
	 * refers to the data and second column denotes corresponding weights.
	 * 
	 * @return InterQuartileMean
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public double interQuartileMeanOLD() throws DMLRuntimeException {
		double sum_wt = sumWeightForQuantile();
		
		int fromPos = (int) Math.ceil(0.25*sum_wt);
		int toPos = (int) Math.ceil(0.75*sum_wt);
		int selectRange = toPos-fromPos; // range: (fromPos,toPos]
		
		if ( selectRange == 0 )
			return 0.0;
		
		int index, count=0;
		
		// The first row (0^th row) has value 0.
		// If it has a non-zero weight i.e., input data has zero values
		// then "index" must start from 0, otherwise we skip the first row 
		// and start with the next value in the data, which is in the 1st row.
		if ( quickGetValue(0,1) > 0 ) 
			index = 0;
		else
			index = 1;
		
		// keep scanning the weights, until we hit the required position <code>fromPos</code>
		while ( count < fromPos ) {
			count += quickGetValue(index,1);
			index++;
		}
		
		double runningSum; 
		double val;
		int wt, selectedCount;
		
		runningSum = (count-fromPos) * quickGetValue(index-1, 0);
		selectedCount = (count-fromPos);
		
		while(count <= toPos ) {
			val = quickGetValue(index,0);
			wt = (int) quickGetValue(index,1);
			
			runningSum += (val * Math.min(wt, selectRange-selectedCount));
			selectedCount += Math.min(wt, selectRange-selectedCount);
			count += wt;
			index++;
		}
		
		//System.out.println(fromPos + ", " + toPos + ": " + count + ", "+ runningSum + ", " + selectedCount);
		
		return runningSum/selectedCount;
	}
	
	public MatrixValue pickValues(MatrixValue quantiles, MatrixValue ret) 
		throws DMLRuntimeException {
	
		MatrixBlock qs=checkType(quantiles);
		
		if ( qs.clen != 1 ) {
			throw new DMLRuntimeException("Multiple quantiles can only be computed on a 1D matrix");
		}
		
		MatrixBlock output = checkType(ret);

		if(output==null)
			output=new MatrixBlock(qs.rlen, qs.clen, false); // resulting matrix is mostly likely be dense
		else
			output.reset(qs.rlen, qs.clen, false);
		
		for ( int i=0; i < qs.rlen; i++ ) {
			output.quickSetValue(i, 0, this.pickValue(qs.quickGetValue(i,0)) );
		}
		
		return output;
	}
	
	public double median() throws DMLRuntimeException {
		double sum_wt = sumWeightForQuantile();
		return pickValue(0.5, sum_wt%2==0);
	}
	
	public double pickValue(double quantile) throws DMLRuntimeException{
		return pickValue(quantile, false);
	}
	
	public double pickValue(double quantile, boolean average) 
		throws DMLRuntimeException 
	{
		double sum_wt = sumWeightForQuantile();
		
		// do averaging only if it is asked for; and sum_wt is even
		average = average && (sum_wt%2 == 0);
		
		int pos = (int) Math.ceil(quantile*sum_wt);
		
		int t = 0, i=-1;
		do {
			i++;
			t += quickGetValue(i,1);
		} while(t<pos && i < getNumRows());
		
		if ( quickGetValue(i,1) != 0 ) {
			// i^th value is present in the data set, simply return it
			if ( average ) {
				if(pos < t) {
					return quickGetValue(i,0);
				}
				if(quickGetValue(i+1,1) != 0)
					return (quickGetValue(i,0)+quickGetValue(i+1,0))/2;
				else
					// (i+1)^th value is 0. So, fetch (i+2)^th value
					return (quickGetValue(i,0)+quickGetValue(i+2,0))/2;
			}
			else 
				return quickGetValue(i, 0);
		}
		else {
			// i^th value is not present in the data set. 
			// It can only happen in the case where i^th value is 0.0; and 0.0 is not present in the data set (but introduced by sort).
			if ( i+1 < getNumRows() )
				// when 0.0 is not the last element in the sorted order
				return quickGetValue(i+1,0);
			else
				// when 0.0 is the last element in the sorted order (input data is all negative)
				return quickGetValue(i-1,0);
		}
	}
	
	/**
	 * In a given two column matrix, the second column denotes weights.
	 * This function computes the total weight
	 * 
	 * @return sum weight for quantile
	 * @throws DMLRuntimeException
	 */
	private double sumWeightForQuantile() 
		throws DMLRuntimeException 
	{
		double sum_wt = 0;
		for (int i=0; i < getNumRows(); i++ ) {
			double tmp = quickGetValue(i, 1);
			sum_wt += tmp;		
			
			// test all values not just final sum_wt to ensure that non-integer weights
			// don't cancel each other out; integer weights are required by all quantiles, etc
			if( Math.floor(tmp) < tmp ) {
				throw new DMLRuntimeException("Wrong input data, quantile weights "
						+ "are expected to be integers but found '"+tmp+"'.");
			}
		}
		
		return sum_wt;
	}

	public MatrixValue aggregateBinaryOperations(MatrixIndexes m1Index, MatrixValue m1Value, MatrixIndexes m2Index, MatrixValue m2Value, 
			MatrixValue result, AggregateBinaryOperator op, boolean useNativeBLAS ) throws DMLRuntimeException
	{
		return aggregateBinaryOperations(m1Value, m2Value, result, op, useNativeBLAS);
	}

	public MatrixValue aggregateBinaryOperations(MatrixValue m1Value, MatrixValue m2Value, MatrixValue result, AggregateBinaryOperator op, boolean useNativeBLAS) 
		throws DMLRuntimeException
	{
		//check input types, dimensions, configuration
		MatrixBlock m1 = checkType(m1Value);
		MatrixBlock m2 = checkType(m2Value);
		MatrixBlock ret = checkType(result);
		if( m1.clen != m2.rlen ) {
			throw new RuntimeException("Dimensions do not match for matrix multiplication ("+m1.clen+"!="+m2.rlen+").");
		}
		if( !(op.binaryFn instanceof Multiply && op.aggOp.increOp.fn instanceof Plus) ) {
			throw new DMLRuntimeException("Unsupported binary aggregate operation: ("+op.binaryFn+", "+op.aggOp+").");
		}
			
		//setup meta data (dimensions, sparsity)
		int rl = m1.rlen;
		int cl = m2.clen;
		SparsityEstimate sp = estimateSparsityOnAggBinary(m1, m2, op);
		
		//create output matrix block
		if( ret==null )
			ret = new MatrixBlock(rl, cl, sp.sparse, sp.estimatedNonZeros);
		else
			ret.reset(rl, cl, sp.sparse, sp.estimatedNonZeros);
		
		//compute matrix multiplication (only supported binary aggregate operation)
		if( useNativeBLAS )
			LibMatrixNative.matrixMult(m1, m2, ret, op.getNumThreads());
		else if( op.getNumThreads() > 1 )
			LibMatrixMult.matrixMult(m1, m2, ret, op.getNumThreads());
		else
			LibMatrixMult.matrixMult(m1, m2, ret);
		
		return ret;
	}

	public MatrixBlock aggregateTernaryOperations(MatrixBlock m1, MatrixBlock m2, MatrixBlock m3, MatrixBlock ret, AggregateTernaryOperator op, boolean inCP) 
		throws DMLRuntimeException
	{
		//check input dimensions and operators
		if( m1.rlen!=m2.rlen || m1.clen!=m2.clen || (m3!=null && (m2.rlen!=m3.rlen || m2.clen!=m3.clen)) )
			throw new DMLRuntimeException("Invalid dimensions for aggregate tertiary ("+m1.rlen+"x"+m1.clen+", "+m2.rlen+"x"+m2.clen+", "+m3.rlen+"x"+m3.clen+").");
		if( !( op.aggOp.increOp.fn instanceof KahanPlus && op.binaryFn instanceof Multiply) )
			throw new DMLRuntimeException("Unsupported operator for aggregate tertiary operations.");
		
		//create output matrix block w/ corrections
		int rl = (op.indexFn instanceof ReduceRow) ? 2 : 1;
		int cl = (op.indexFn instanceof ReduceRow) ? m1.clen : 2;
		if( ret == null )
			ret = new MatrixBlock(rl, cl, false);
		else
			ret.reset(rl, cl, false);
				
		//execute ternary aggregate function
		if( op.getNumThreads() > 1 )
			ret = LibMatrixAgg.aggregateTernary(m1, m2, m3, ret, op, op.getNumThreads());
		else
			ret = LibMatrixAgg.aggregateTernary(m1, m2, m3, ret, op);
		
		if(op.aggOp.correctionExists && inCP)
			ret.dropLastRowsOrColums(op.aggOp.correctionLocation);
		return ret;
	}

	public MatrixBlock  uaggouterchainOperations(MatrixBlock mbLeft, MatrixBlock mbRight, MatrixBlock mbOut, BinaryOperator bOp, AggregateUnaryOperator uaggOp) 
		throws DMLRuntimeException
	{
		double bv[] = DataConverter.convertToDoubleVector(mbRight);
		int bvi[] = null;
		
		//process instruction
		if (LibMatrixOuterAgg.isSupportedUaggOp(uaggOp, bOp))
		{
			if((LibMatrixOuterAgg.isRowIndexMax(uaggOp)) || (LibMatrixOuterAgg.isRowIndexMin(uaggOp))) 
			{
				bvi = LibMatrixOuterAgg.prepareRowIndices(bv.length, bv, bOp, uaggOp);
			} else {
				Arrays.sort(bv);
			}

			int iRows = (uaggOp.indexFn instanceof ReduceCol ? mbLeft.getNumRows(): 2); 
			int iCols = (uaggOp.indexFn instanceof ReduceRow ? mbLeft.getNumColumns(): 2); 
			if(mbOut==null)
				mbOut=new MatrixBlock(iRows, iCols, false);  // Output matrix will be dense matrix most of the time.
			else
				mbOut.reset(iRows, iCols, false);

			LibMatrixOuterAgg.aggregateMatrix(mbLeft, mbOut, bv, bvi, bOp, uaggOp);
		} else
			throw new DMLRuntimeException("Unsupported operator for unary aggregate operations.");
		
		return mbOut;
	}
	
		
	/**
	 * Invocation from CP instructions. The aggregate is computed on the groups object
	 * against target and weights. 
	 * 
	 * Notes:
	 * * The computed number of groups is reused for multiple invocations with different target.
	 * * This implementation supports that the target is passed as column or row vector,
	 *   in case of row vectors we also use sparse-safe implementations for sparse safe
	 *   aggregation operators.
	 * 
	 * @param tgt ?
	 * @param wghts ?
	 * @param ret ?
	 * @param ngroups ?
	 * @param op operator
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public MatrixBlock groupedAggOperations(MatrixValue tgt, MatrixValue wghts, MatrixValue ret, int ngroups, Operator op) 
		throws DMLRuntimeException 
	{
		//single-threaded grouped aggregate 
		return groupedAggOperations(tgt, wghts, ret, ngroups, op, 1);
	}

	public MatrixBlock groupedAggOperations(MatrixValue tgt, MatrixValue wghts, MatrixValue ret, int ngroups, Operator op, int k) 
		throws DMLRuntimeException 		
	{
		//setup input matrices
		MatrixBlock target = checkType(tgt);
		MatrixBlock weights = checkType(wghts);
		
		//check valid dimensions
		boolean validMatrixOp = (weights == null && ngroups>=1);
		if( this.getNumColumns() != 1 || (weights!=null && weights.getNumColumns()!=1) )
			throw new DMLRuntimeException("groupedAggregate can only operate on 1-dimensional column matrices for groups and weights.");
		if( target.getNumColumns() != 1 && op instanceof CMOperator && !validMatrixOp )
			throw new DMLRuntimeException("groupedAggregate can only operate on 1-dimensional column matrices for target (for this aggregation function).");
		if( target.getNumColumns() != 1 && target.getNumRows()!=1 && !validMatrixOp )
			throw new DMLRuntimeException("groupedAggregate can only operate on 1-dimensional column or row matrix for target.");
		if( this.getNumRows() != target.getNumRows() && this.getNumRows() !=Math.max(target.getNumRows(),target.getNumColumns()) || (weights != null && this.getNumRows() != weights.getNumRows()) ) 
			throw new DMLRuntimeException("groupedAggregate can only operate on matrices with equal dimensions.");
		
		// obtain numGroups from instruction, if provided
		if (ngroups > 0)
			numGroups = ngroups;
		
		// Determine the number of groups
		if( numGroups <= 0 ) //reuse if available
		{
			double min = this.min();
			double max = this.max();
			
			if ( min <= 0 )
				throw new DMLRuntimeException("Invalid value (" + min + ") encountered in 'groups' while computing groupedAggregate");
			if ( max <= 0 )
				throw new DMLRuntimeException("Invalid value (" + max + ") encountered in 'groups' while computing groupedAggregate.");
		
			numGroups = (int) max;
		}
	
		// Allocate result matrix
		boolean rowVector = (target.getNumRows()==1 && target.getNumColumns()>1);
		MatrixBlock result = checkType(ret);
		boolean result_sparsity = estimateSparsityOnGroupedAgg(rlen, numGroups);
		if(result==null)
			result=new MatrixBlock(numGroups, rowVector?1:target.getNumColumns(), result_sparsity);
		else
			result.reset(numGroups, rowVector?1:target.getNumColumns(), result_sparsity);

		//execute grouped aggregate operation
		if( k > 1 )
			LibMatrixAgg.groupedAggregate(this, target, weights, result, numGroups, op, k);
		else
			LibMatrixAgg.groupedAggregate(this, target, weights, result, numGroups, op);
		
		return result;
	}

	public MatrixBlock removeEmptyOperations( MatrixBlock ret, boolean rows, MatrixBlock select )
		throws DMLRuntimeException 
	{	
		MatrixBlock result = checkType(ret);
		return LibMatrixReorg.rmempty(this, result, rows, select);
	}

	public MatrixBlock removeEmptyOperations( MatrixBlock ret, boolean rows)
		throws DMLRuntimeException 
	{
		return removeEmptyOperations(ret, rows, null);
	}

	public MatrixBlock rexpandOperations( MatrixBlock ret, double max, boolean rows, boolean cast, boolean ignore, int k )
		throws DMLRuntimeException 
	{	
		MatrixBlock result = checkType(ret);
		return LibMatrixReorg.rexpand(this, result, max, rows, cast, ignore, k);
	}
	
	
	@Override
	public MatrixValue replaceOperations(MatrixValue result, double pattern, double replacement) 
		throws DMLRuntimeException
	{
		MatrixBlock ret = checkType(result);
		examSparsity(); //ensure its in the right format
		ret.reset(rlen, clen, sparse);
		if( nonZeros == 0 && pattern != 0  )
			return ret; //early abort
		boolean NaNpattern = Double.isNaN(pattern);
		
		if( sparse ) //SPARSE
		{
			if( pattern != 0d ) //SPARSE <- SPARSE (sparse-safe)
			{
				ret.allocateSparseRowsBlock();
				SparseBlock a = sparseBlock;
				SparseBlock c = ret.sparseBlock;
				
				for( int i=0; i<rlen; i++ ) {
					if( !a.isEmpty(i) )
					{
						c.allocate(i);
						int apos = a.pos(i);
						int alen = a.size(i);
						int[] aix = a.indexes(i);
						double[] avals = a.values(i);
						for( int j=apos; j<apos+alen; j++ )
						{
							double val = avals[j];
							if( val== pattern || (NaNpattern && Double.isNaN(val)) )
								c.append(i, aix[j], replacement);
							else
								c.append(i, aix[j], val);
						}
					}
				}
			}
			else //DENSE <- SPARSE
			{
				ret.sparse = false;
				ret.allocateDenseBlock();	
				SparseBlock a = sparseBlock;
				double[] c = ret.denseBlock;
				
				//initialize with replacement (since all 0 values, see SPARSITY_TURN_POINT)
				Arrays.fill(c, replacement); 
				
				//overwrite with existing values (via scatter)
				if( a != null  ) //check for empty matrix
					for( int i=0, cix=0; i<rlen; i++, cix+=clen )
					{
						if( !a.isEmpty(i) )
						{
							int apos = a.pos(i);
							int alen = a.size(i);
							int[] aix = a.indexes(i);
							double[] avals = a.values(i);
							for( int j=apos; j<apos+alen; j++ )
								if( avals[ j ] != 0 )
									c[ cix+aix[j] ] = avals[ j ];
						}
					}
			}			
		}
		else //DENSE <- DENSE
		{
			int mn = ret.rlen * ret.clen;
			ret.allocateDenseBlock();
			double[] a = denseBlock;
			double[] c = ret.denseBlock;
			
			for( int i=0; i<mn; i++ ) 
			{
				double val = a[i];
				if( val== pattern || (NaNpattern && Double.isNaN(val)) )
					c[i] = replacement;
				else
					c[i] = a[i];
			}
		}
		
		ret.recomputeNonZeros();
		ret.examSparsity();
		
		return ret;
	}
	
	
	/**
	 *  D = ctable(A,v2,W)
	 *  this &lt;- A; scalarThat &lt;- v2; that2 &lt;- W; result &lt;- D
	 *  
	 * (i1,j1,v1) from input1 (this)
	 * (v2) from sclar_input2 (scalarThat)
	 * (i3,j3,w)  from input3 (that2) 
	 */
	@Override
	public void ternaryOperations(Operator op, double scalarThat,
			MatrixValue that2Val, CTableMap resultMap, MatrixBlock resultBlock)
		throws DMLRuntimeException 
	{
		MatrixBlock that2 = checkType(that2Val);
		CTable ctable = CTable.getCTableFnObject();
		double v2 = scalarThat;
		
		//sparse-unsafe ctable execution
		//(because input values of 0 are invalid and have to result in errors) 
		if ( resultBlock == null ) {
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					double w = that2.quickGetValue(i, j);
					ctable.execute(v1, v2, w, false, resultMap);
				}
		}
		else {
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					double w = that2.quickGetValue(i, j);
					ctable.execute(v1, v2, w, false, resultBlock);
				}
			resultBlock.recomputeNonZeros();
		}
	}

	/**
	 *  D = ctable(A,v2,w)
	 *  this &lt;- A; scalar_that &lt;- v2; scalar_that2 &lt;- w; result &lt;- D
	 *  
	 * (i1,j1,v1) from input1 (this)
     * (v2) from sclar_input2 (scalarThat)
	 * (w)  from scalar_input3 (scalarThat2)
	 */
	@Override
	public void ternaryOperations(Operator op, double scalarThat,
			double scalarThat2, CTableMap resultMap, MatrixBlock resultBlock)
			throws DMLRuntimeException 
	{
		CTable ctable = CTable.getCTableFnObject();
		double v2 = scalarThat;
		double w = scalarThat2;
		
		//sparse-unsafe ctable execution
		//(because input values of 0 are invalid and have to result in errors) 
		if ( resultBlock == null ) { 
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					ctable.execute(v1, v2, w, false, resultMap);
				}
		}
		else {
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					ctable.execute(v1, v2, w, false, resultBlock);
				}	
			resultBlock.recomputeNonZeros();
		}		
	}
	
	/**
	 * Specific ctable case of ctable(seq(...),X), where X is the only
	 * matrix input. The 'left' input parameter specifies if the seq appeared
	 * on the left, otherwise it appeared on the right.
	 * 
	 */
	@Override
	public void ternaryOperations(Operator op, MatrixIndexes ix1, double scalarThat,
			boolean left, int brlen, CTableMap resultMap, MatrixBlock resultBlock)
		throws DMLRuntimeException 
	{	
		CTable ctable = CTable.getCTableFnObject();
		double w = scalarThat;
		int offset = (int) ((ix1.getRowIndex()-1)*brlen); 
		
		//sparse-unsafe ctable execution
		//(because input values of 0 are invalid and have to result in errors) 
		if( resultBlock == null) {
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					if( left )
						ctable.execute(offset+i+1, v1, w, false, resultMap);
					else
						ctable.execute(v1, offset+i+1, w, false, resultMap);
				}
		}
		else {
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					if( left )
						ctable.execute(offset+i+1, v1, w, false, resultBlock);
					else
						ctable.execute(v1, offset+i+1, w, false, resultBlock);
				}
			resultBlock.recomputeNonZeros();
		}
	}

	/**
	 *  D = ctable(A,B,w)
	 *  this &lt;- A; that &lt;- B; scalar_that2 &lt;- w; result &lt;- D
	 *  
	 * (i1,j1,v1) from input1 (this)
	 * (i1,j1,v2) from input2 (that)
	 * (w)  from scalar_input3 (scalarThat2)
	 * 
	 * NOTE: This method supports both vectors and matrices. In case of matrices and ignoreZeros=true
	 * we can also use a sparse-safe implementation
	 */
	@Override
	public void ternaryOperations(Operator op, MatrixValue thatVal, double scalarThat2, boolean ignoreZeros,
			     CTableMap resultMap, MatrixBlock resultBlock)
			throws DMLRuntimeException 
	{	
		//setup ctable computation
		MatrixBlock that = checkType(thatVal);
		CTable ctable = CTable.getCTableFnObject();
		double w = scalarThat2;
		
		if( ignoreZeros //SPARSE-SAFE & SPARSE INPUTS
			&& this.sparse && that.sparse )
		{
			//note: only used if both inputs have aligned zeros, which
			//allows us to infer that the nnz both inputs are equivalent
			
			//early abort on empty blocks possible
			if( this.isEmptyBlock(false) && that.isEmptyBlock(false) )
				return;
			
			SparseBlock a = this.sparseBlock;
			SparseBlock b = that.sparseBlock;
			for( int i=0; i<rlen; i++ )
			{
				if( !a.isEmpty(i) )
				{
					int alen = a.size(i);
					int apos = a.pos(i);
					double[] avals = a.values(i);
					int bpos = b.pos(i);
					double[] bvals = b.values(i); 
					
					if( resultBlock == null ) {
						for( int j=0; j<alen; j++ )
							ctable.execute(avals[apos+j], bvals[bpos+j], w, ignoreZeros, resultMap);		
					}
					else {
						for( int j=0; j<alen; j++ )
							ctable.execute(avals[apos+j], bvals[bpos+j], w, ignoreZeros, resultBlock);			
					}
				}
			}	
		}
		else //SPARSE-UNSAFE | GENERIC INPUTS
		{
			//sparse-unsafe ctable execution
			//(because input values of 0 are invalid and have to result in errors) 
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					double v2 = that.quickGetValue(i, j);
					if( resultBlock == null )
						ctable.execute(v1, v2, w, ignoreZeros, resultMap);
					else
						ctable.execute(v1, v2, w, ignoreZeros, resultBlock);
				}
		}
		
		//maintain nnz (if necessary)
		if( resultBlock!=null )
			resultBlock.recomputeNonZeros();
	}
	
	/**
	 *  D = ctable(seq,A,w)
	 *  this &lt;- seq; thatMatrix &lt;- A; thatScalar &lt;- w; result &lt;- D
	 *  
	 * (i1,j1,v1) from input1 (this)
	 * (i1,j1,v2) from input2 (that)
	 * (w)  from scalar_input3 (scalarThat2)
	 * 
	 * @param op operator
	 * @param thatMatrix matrix value
	 * @param thatScalar scalar double
	 * @param resultBlock result matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public void ternaryOperations(Operator op, MatrixValue thatMatrix, double thatScalar, MatrixBlock resultBlock)
			throws DMLRuntimeException 
	{	
		MatrixBlock that = checkType(thatMatrix);
		CTable ctable = CTable.getCTableFnObject();
		double w = thatScalar;
		
		//sparse-unsafe ctable execution
		//(because input values of 0 are invalid and have to result in errors) 		
		//resultBlock guaranteed to be allocated for ctableexpand
		//each row in resultBlock will be allocated and will contain exactly one value
		int maxCol = 0;
		for( int i=0; i<rlen; i++ ) {
			double v2 = that.quickGetValue(i, 0);
			maxCol = ctable.execute(i+1, v2, w, maxCol, resultBlock);
		}
		
		//update meta data (initially unknown number of columns)
		//note: nnz maintained in ctable (via quickset)
		resultBlock.clen = maxCol;
	}
	
	/**
	 *  D = ctable(A,B,W)
	 *  this &lt;- A; that &lt;- B; that2 &lt;- W; result &lt;- D
	 *  
	 * (i1,j1,v1) from input1 (this)
	 * (i1,j1,v2) from input2 (that)
	 * (i1,j1,w)  from input3 (that2)
	 * 
	 * @param op operator
	 * @param thatVal matrix value 1
	 * @param that2Val matrix value 2
	 * @param resultMap table map
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public void ternaryOperations(Operator op, MatrixValue thatVal, MatrixValue that2Val, CTableMap resultMap)
		throws DMLRuntimeException 
	{
		ternaryOperations(op, thatVal, that2Val, resultMap, null);
	}
		
	@Override
	public void ternaryOperations(Operator op, MatrixValue thatVal, MatrixValue that2Val, CTableMap resultMap, MatrixBlock resultBlock)
		throws DMLRuntimeException
	{	
		MatrixBlock that = checkType(thatVal);
		MatrixBlock that2 = checkType(that2Val);
		CTable ctable = CTable.getCTableFnObject();
		
		//sparse-unsafe ctable execution
		//(because input values of 0 are invalid and have to result in errors) 
		if(resultBlock == null) 
		{
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					double v2 = that.quickGetValue(i, j);
					double w = that2.quickGetValue(i, j);
					ctable.execute(v1, v2, w, false, resultMap);
				}		
		}
		else 
		{
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ )
				{
					double v1 = this.quickGetValue(i, j);
					double v2 = that.quickGetValue(i, j);
					double w = that2.quickGetValue(i, j);
					ctable.execute(v1, v2, w, false, resultBlock);
				}
			resultBlock.recomputeNonZeros();
		}
	}
	
	@Override
	public MatrixValue quaternaryOperations(QuaternaryOperator qop, MatrixValue um, MatrixValue vm, MatrixValue wm, MatrixValue out)
		throws DMLRuntimeException
	{
		return quaternaryOperations(qop, um, vm, wm, out, 1);
	}

	public MatrixValue quaternaryOperations(QuaternaryOperator qop, MatrixValue um, MatrixValue vm, MatrixValue wm, MatrixValue out, int k)
		throws DMLRuntimeException
	{
		//check input dimensions
		if( getNumRows() != um.getNumRows() )
			throw new DMLRuntimeException("Dimension mismatch rows on quaternary operation: "+getNumRows()+"!="+um.getNumRows());
		if( getNumColumns() != vm.getNumRows() )
			throw new DMLRuntimeException("Dimension mismatch columns quaternary operation: "+getNumColumns()+"!="+vm.getNumRows());
		
		//check input data types
		MatrixBlock X = this;
		MatrixBlock U = checkType(um);
		MatrixBlock V = checkType(vm);
		MatrixBlock R = checkType(out);
		
		//prepare intermediates and output
		if( qop.wtype1 != null || qop.wtype4 != null )
			R.reset(1, 1, false);
		else if( qop.wtype2 != null || qop.wtype5 != null )
			R.reset(rlen, clen, sparse);
		else if( qop.wtype3 != null ) {
			MatrixCharacteristics mc = qop.wtype3.computeOutputCharacteristics(X.rlen, X.clen, U.clen);
			R.reset( (int)mc.getRows(), (int)mc.getCols(), qop.wtype3.isBasic()?X.isInSparseFormat():false);
		}
		
		//core block operation
		if( qop.wtype1 != null ){ //wsloss
			MatrixBlock W = qop.wtype1.hasFourInputs() ? checkType(wm) : null;
			if( k > 1 )
				LibMatrixMult.matrixMultWSLoss(X, U, V, W, R, qop.wtype1, k);
			else
				LibMatrixMult.matrixMultWSLoss(X, U, V, W, R, qop.wtype1);
		}
		else if( qop.wtype2 != null ){ //wsigmoid
			if( k > 1 )
				LibMatrixMult.matrixMultWSigmoid(X, U, V, R, qop.wtype2, k);
			else
				LibMatrixMult.matrixMultWSigmoid(X, U, V, R, qop.wtype2);
		}	
		else if( qop.wtype3 != null ){ //wdivmm
			//note: for wdivmm-minus X and W interchanged because W always present 
			MatrixBlock W = qop.wtype3.hasFourInputs() ? checkType(wm) : null;
			if( qop.getScalar() != 0 ) {
				W = new MatrixBlock(1, 1, false);
				W.quickSetValue(0, 0, qop.getScalar());
			}
			if( k > 1 )
				LibMatrixMult.matrixMultWDivMM(X, U, V, W, R, qop.wtype3, k);
			else
				LibMatrixMult.matrixMultWDivMM(X, U, V, W, R, qop.wtype3);	
		}
		else if( qop.wtype4 != null ){ //wcemm
			MatrixBlock W = qop.wtype4.hasFourInputs() ? checkType(wm) : null;
			double eps = (W != null && W.getNumRows() == 1 && W.getNumColumns() == 1) ? W.quickGetValue(0, 0) : qop.getScalar();
			
			if( k > 1 )
				LibMatrixMult.matrixMultWCeMM(X, U, V, eps, R, qop.wtype4, k);
			else
				LibMatrixMult.matrixMultWCeMM(X, U, V, eps, R, qop.wtype4);
		}
		else if( qop.wtype5 != null ){ //wumm
			if( k > 1 )
				LibMatrixMult.matrixMultWuMM(X, U, V, R, qop.wtype5, qop.fn, k);
			else
				LibMatrixMult.matrixMultWuMM(X, U, V, R, qop.wtype5, qop.fn);	
		}
		
		return R;
	}
	
	////////
	// Data Generation Methods
	// (rand, sequence)
	
	/**
	 * Function to generate the random matrix with specified dimensions (block sizes are not specified).
	 *  
	 * 
	 * @param rows number of rows
	 * @param cols number of columns
	 * @param sparsity sparsity as a percentage
	 * @param min minimum value
	 * @param max maximum value
	 * @param pdf pdf
	 * @param seed random seed
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public static MatrixBlock randOperations(int rows, int cols, double sparsity, double min, double max, String pdf, long seed) 
		throws DMLRuntimeException
	{
		return randOperations(rows, cols, sparsity, min, max, pdf, seed, 1);
	}
	
	/**
	 * Function to generate the random matrix with specified dimensions (block sizes are not specified).
	 *  
	 * @param rows number of rows
	 * @param cols number of columns
	 * @param sparsity sparsity as a percentage
	 * @param min minimum value
	 * @param max maximum value
	 * @param pdf pdf
	 * @param seed random seed
	 * @param k ?
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public static MatrixBlock randOperations(int rows, int cols, double sparsity, double min, double max, String pdf, long seed, int k) 
		throws DMLRuntimeException
	{
		RandomMatrixGenerator rgen = new RandomMatrixGenerator(pdf, rows, cols, 
				ConfigurationManager.getBlocksize(), ConfigurationManager.getBlocksize(), sparsity, min, max);
		
		if (k > 1)
			return randOperations(rgen, seed, k);
		else
			return randOperations(rgen, seed);
	}
	
	/**
	 * Function to generate the random matrix with specified dimensions and block dimensions.
	 * 
	 * @param rgen random matrix generator
	 * @param seed seed value
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public static MatrixBlock randOperations(RandomMatrixGenerator rgen, long seed) 
		throws DMLRuntimeException 
	{
		return randOperations(rgen, seed, 1);
	}
	
	/**
	 * Function to generate the random matrix with specified dimensions and block dimensions.
	 * 
	 * @param rgen random matrix generator
	 * @param seed seed value
	 * @param k ?
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public static MatrixBlock randOperations(RandomMatrixGenerator rgen, long seed, int k)
		throws DMLRuntimeException 
	{
		MatrixBlock out = new MatrixBlock();
		Well1024a bigrand = null;
		LongStream nnzInBlock = null;

		//setup seeds and nnz per block
		if( !LibMatrixDatagen.isShortcutRandOperation(rgen._min, rgen._max, rgen._sparsity, rgen._pdf) ){
			bigrand = LibMatrixDatagen.setupSeedsForRand(seed);
			nnzInBlock = LibMatrixDatagen.computeNNZperBlock(rgen._rows, rgen._cols, rgen._rowsPerBlock, rgen._colsPerBlock, rgen._sparsity);
		}
		
		//generate rand data
		if (k > 1)
			out.randOperationsInPlace(rgen, nnzInBlock, bigrand, -1, k);
		else
			out.randOperationsInPlace(rgen, nnzInBlock, bigrand, -1);
		
		return out;
	}
	
	/**
	 * Function to generate a matrix of random numbers. This is invoked both
	 * from CP as well as from MR. In case of CP, it generates an entire matrix
	 * block-by-block. A <code>bigrand</code> is passed so that block-level
	 * seeds are generated internally. In case of MR, it generates a single
	 * block for given block-level seed <code>bSeed</code>.
	 * 
	 * When pdf="uniform", cell values are drawn from uniform distribution in
	 * range <code>[min,max]</code>.
	 * 
	 * When pdf="normal", cell values are drawn from standard normal
	 * distribution N(0,1). The range of generated values will always be
	 * (-Inf,+Inf).
	 * 
	 * @param rgen random matrix generator
	 * @param nnzInBlock number of nonzeros in block
	 * @param bigrand ?
	 * @param bSeed seed value
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public MatrixBlock randOperationsInPlace(
								RandomMatrixGenerator rgen, LongStream nnzInBlock, 
								Well1024a bigrand, long bSeed ) 
		throws DMLRuntimeException
	{
		LibMatrixDatagen.generateRandomMatrix( this, rgen, nnzInBlock, bigrand, bSeed );
		return this;
	}
	
	/**
	 * Function to generate a matrix of random numbers. This is invoked both
	 * from CP as well as from MR. In case of CP, it generates an entire matrix
	 * block-by-block. A <code>bigrand</code> is passed so that block-level
	 * seeds are generated internally. In case of MR, it generates a single
	 * block for given block-level seed <code>bSeed</code>.
	 * 
	 * When pdf="uniform", cell values are drawn from uniform distribution in
	 * range <code>[min,max]</code>.
	 * 
	 * When pdf="normal", cell values are drawn from standard normal
	 * distribution N(0,1). The range of generated values will always be
	 * (-Inf,+Inf).
	 * 
	 * @param rgen random matrix generator
	 * @param nnzInBlock number of nonzeros in block
	 * @param bigrand ?
	 * @param bSeed seed value
	 * @param k ?
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public MatrixBlock randOperationsInPlace(RandomMatrixGenerator rgen, 
			LongStream nnzInBlock, Well1024a bigrand, long bSeed, int k) 
		throws DMLRuntimeException
	{
		LibMatrixDatagen.generateRandomMatrix( this, rgen, nnzInBlock, 
				bigrand, bSeed, k );
		return this;
	}
	
	/**
	 * Method to generate a sequence according to the given parameters. The
	 * generated sequence is always in dense format.
	 * 
	 * Both end points specified <code>from</code> and <code>to</code> must be
	 * included in the generated sequence i.e., [from,to] both inclusive. Note
	 * that, <code>to</code> is included only if (to-from) is perfectly
	 * divisible by <code>incr</code>.
	 * 
	 * For example, seq(0,1,0.5) generates (0.0 0.5 1.0) 
	 *      whereas seq(0,1,0.6) generates (0.0 0.6) but not (0.0 0.6 1.0)
	 * 
	 * @param from ?
	 * @param to ?
	 * @param incr ?
	 * @return matrix block
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	public static MatrixBlock seqOperations(double from, double to, double incr) 
		throws DMLRuntimeException 
	{
		MatrixBlock out = new MatrixBlock();
		LibMatrixDatagen.generateSequence( out, from, to, incr );
		
		return out;
	}

	public MatrixBlock seqOperationsInPlace(double from, double to, double incr) 
		throws DMLRuntimeException 
	{
		LibMatrixDatagen.generateSequence( this, from, to, incr );
		
		return this;
	}

	public static MatrixBlock sampleOperations(long range, int size, boolean replace, long seed) 
		throws DMLRuntimeException 
	{
		MatrixBlock out = new MatrixBlock();
		LibMatrixDatagen.generateSample( out, range, size, replace, seed );
		
		return out;
	}
	
	////////
	// Misc methods
	
	private static MatrixBlock checkType(MatrixValue block) 
		throws RuntimeException
	{
		if( block!=null && !(block instanceof MatrixBlock))
			throw new RuntimeException("Unsupported matrix value: "+block.getClass().getSimpleName());
		return (MatrixBlock) block;
	}
	
	/**
	 * Indicates if concurrent modifications of disjoint rows are thread-safe.
	 * 
	 * @return true if thread-safe
	 */
	public boolean isThreadSafe() {
		return !sparse || ((sparseBlock != null) ? sparseBlock.isThreadSafe() : 
			DEFAULT_SPARSEBLOCK == SparseBlock.Type.MCSR); //only MCSR thread-safe
	}
	
	/**
	 * Indicates if concurrent modifications of disjoint rows are thread-safe.
	 * 
	 * @param sparse true if sparse
	 * @return true if ?
	 */
	public static boolean isThreadSafe(boolean sparse) {
		return !sparse || DEFAULT_SPARSEBLOCK == SparseBlock.Type.MCSR; //only MCSR thread-safe	
	} 
	
	public void print()
	{
		System.out.println("sparse = "+sparse);
		if(!sparse)
			System.out.println("nonzeros = "+nonZeros);
		for(int i=0; i<rlen; i++)
		{
			for(int j=0; j<clen; j++)
			{
				System.out.print(quickGetValue(i, j)+"\t");
			}
			System.out.println();
		}
	}
	
	@Override
	public int compareTo(Object arg0) {
		throw new RuntimeException("CompareTo should never be called for matrix blocks.");
	}

	@Override
	public boolean equals(Object arg0) {
		throw new RuntimeException("equals should never be called for matrix blocks.");
	}
	
	@Override
	public int hashCode() {
		throw new RuntimeException("HashCode should never be called for matrix blocks.");
	}
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append("sparse? = ");
		sb.append(sparse);
		sb.append("\n");
		
		sb.append("nonzeros = ");
		sb.append(nonZeros);
		sb.append("\n");
		
		sb.append("size: ");
		sb.append(rlen);
		sb.append(" X ");
		sb.append(clen);
		sb.append("\n");
		
		if(sparse)
		{
			if( sparseBlock != null ) {
				//overloaded implementation in sparse blocks
				sb.append(sparseBlock.toString());
			}
		}
		else
		{
			if(denseBlock!=null) {
				for(int i=0, ix=0; i<rlen; i++, ix+=clen) {
					for(int j=0; j<clen; j++) {
						sb.append(this.denseBlock[ix+j]);
						sb.append("\t");
					}
					sb.append("\n");
				}
			}
		}
		
		return sb.toString();
	}


	///////////////////////////
	// Helper classes

	public static class SparsityEstimate
	{
		public long estimatedNonZeros=0;
		public boolean sparse=false;
		public SparsityEstimate(boolean sps, long nnzs)
		{
			sparse=sps;
			estimatedNonZeros=nnzs;
		}
		public SparsityEstimate(){}
	}
}
