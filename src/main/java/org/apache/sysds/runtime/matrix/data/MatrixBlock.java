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

package org.apache.sysds.runtime.matrix.data;

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
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.apache.commons.math3.random.Well1024a;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.sysds.common.Types.BlockType;
import org.apache.sysds.common.Types.CorrectionLocationType;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.lops.MMTSJ.MMTSJType;
import org.apache.sysds.lops.MapMultChain.ChainType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.compress.lib.CLALibBinaryCellOp;
import org.apache.sysds.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysds.runtime.controlprogram.caching.LazyWriteBuffer;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.data.DenseBlockFactory;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.data.SparseBlockCOO;
import org.apache.sysds.runtime.data.SparseBlockCSR;
import org.apache.sysds.runtime.data.SparseBlockFactory;
import org.apache.sysds.runtime.data.SparseBlockMCSR;
import org.apache.sysds.runtime.data.SparseRow;
import org.apache.sysds.runtime.functionobjects.Builtin;
import org.apache.sysds.runtime.functionobjects.Builtin.BuiltinCode;
import org.apache.sysds.runtime.functionobjects.CM;
import org.apache.sysds.runtime.functionobjects.CTable;
import org.apache.sysds.runtime.functionobjects.DiagIndex;
import org.apache.sysds.runtime.functionobjects.Divide;
import org.apache.sysds.runtime.functionobjects.FunctionObject;
import org.apache.sysds.runtime.functionobjects.IfElse;
import org.apache.sysds.runtime.functionobjects.KahanFunction;
import org.apache.sysds.runtime.functionobjects.KahanPlus;
import org.apache.sysds.runtime.functionobjects.KahanPlusSq;
import org.apache.sysds.runtime.functionobjects.MinusMultiply;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.functionobjects.PlusMultiply;
import org.apache.sysds.runtime.functionobjects.ReduceAll;
import org.apache.sysds.runtime.functionobjects.ReduceCol;
import org.apache.sysds.runtime.functionobjects.ReduceRow;
import org.apache.sysds.runtime.functionobjects.RevIndex;
import org.apache.sysds.runtime.functionobjects.SortIndex;
import org.apache.sysds.runtime.functionobjects.SwapIndex;
import org.apache.sysds.runtime.functionobjects.TernaryValueFunction.ValueFunctionWithConstant;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CM_COV_Object;
import org.apache.sysds.runtime.instructions.cp.KahanObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.matrix.data.LibMatrixBincell.BinaryAccessType;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateTernaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.CMOperator;
import org.apache.sysds.runtime.matrix.operators.CMOperator.AggregateOperationTypes;
import org.apache.sysds.runtime.matrix.operators.COVOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.QuaternaryOperator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.matrix.operators.ScalarOperator;
import org.apache.sysds.runtime.matrix.operators.SimpleOperator;
import org.apache.sysds.runtime.matrix.operators.TernaryOperator;
import org.apache.sysds.runtime.matrix.operators.UnaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.runtime.util.FastBufferedDataInputStream;
import org.apache.sysds.runtime.util.FastBufferedDataOutputStream;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.runtime.util.IndexRange;
import org.apache.sysds.runtime.util.UtilFunctions;
import org.apache.sysds.utils.NativeHelper;


public class MatrixBlock extends MatrixValue implements CacheBlock, Externalizable {
	// private static final Log LOG = LogFactory.getLog(MatrixBlock.class.getName());
	
	private static final long serialVersionUID = 7319972089143154056L;
	
	//sparsity nnz threshold, based on practical experiments on space consumption and performance
	public static final double SPARSITY_TURN_POINT = 0.4;
	//sparsity threshold for ultra-sparse matrix operations (40nnz in a 1kx1k block)
	public static final double ULTRA_SPARSITY_TURN_POINT  = 0.00004;
	public static final double ULTRA_SPARSITY_TURN_POINT2 = 0.0004;
	public static final int ULTRA_SPARSE_BLOCK_NNZ = 40;
	//default sparse block type: modified compressed sparse rows, for efficient incremental construction
	public static final SparseBlock.Type DEFAULT_SPARSEBLOCK = SparseBlock.Type.MCSR;
	//default sparse block type for update in place: compressed sparse rows, to prevent serialization
	public static final SparseBlock.Type DEFAULT_INPLACE_SPARSEBLOCK = SparseBlock.Type.CSR;
	//allowed overhead for shallow serialize in terms of in-memory-size/x <= serialized-size 
	public static final double MAX_SHALLOW_SERIALIZE_OVERHEAD = 2; //2x size of serialized
	//flag if MCSR blocks that do not qualify for shallow serialize should be converted to CSR
	public static final boolean CONVERT_MCSR_TO_CSR_ON_DEEP_SERIALIZE = true;
	//basic header (int rlen, int clen, byte type)
	public static final int HEADER_SIZE = 9;
	
	//matrix meta data
	protected int rlen       = -1;
	protected int clen       = -1;
	protected boolean sparse = true;
	protected long nonZeros   = 0;
	
	//matrix data (sparse or dense)
	protected DenseBlock denseBlock   = null;
	protected SparseBlock sparseBlock = null;
	
	//sparse-block-specific attributes (allocation only)
	protected int estimatedNNzsPerRow = -1;
	
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
	
	public MatrixBlock(double val) {
		reset(1, 1, false, 1, val);
	}
	
	public MatrixBlock(int rl, int cl, double val) {
		reset(rl, cl, false, (long)rl*cl, val);
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
		if( !that.isEmptyBlock(false) ) {
			nonZeros = that.nonZeros;
			estimatedNNzsPerRow = that.estimatedNNzsPerRow;
			sparseBlock = SparseBlockFactory
				.copySparseBlock(stype, that.sparseBlock, deep);
		}
	}
	
	public MatrixBlock(int rl, int cl, DenseBlock dBlock){
		rlen = rl;
		clen = cl;
		sparse = false;
		denseBlock = dBlock;
	}


	protected MatrixBlock(boolean empty){
		// do nothing
	}

	////////
	// Initialization methods
	// (reset, init, allocate, etc)
	
	@Override
	public final void reset() {
		reset(rlen, clen, sparse, -1, 0);
	}
	
	@Override
	public final void reset(int rl, int cl) {
		reset(rl, cl, sparse, -1, 0);
	}
	
	public final void reset(int rl, int cl, long estnnz) {
		reset(rl, cl, evalSparseFormatInMemory(rl, cl, estnnz), estnnz, 0);
	}
	
	@Override
	public final void reset(int rl, int cl, boolean sp) {
		reset(rl, cl, sp, -1, 0);
	}
	
	@Override
	public final void reset(int rl, int cl, boolean sp, long estnnz) {
		reset(rl, cl, sp, estnnz, 0);
	}
	
	@Override
	public final void reset(int rl, int cl, double val) {
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
	public void reset(int rl, int cl, boolean sp, long estnnz, double val) {
		//check for valid dimensions
		if( rl < 0 || cl < 0 )
			throw new RuntimeException("Invalid block dimensions: "+rl+" "+cl);
		
		//reset basic meta data
		rlen = rl;
		clen = cl;
		sparse = (val == 0) ? sp : false;
		nonZeros = (val == 0) ? 0 : (long)rl*cl;
		estimatedNNzsPerRow = (estnnz < 0 || !sparse) ? -1 :
			(int)Math.ceil((double)estnnz/(double)rlen);
		
		//reset sparse/dense blocks
		if( sparse )
			resetSparse();
		else
			resetDense(val);
	}
	
	private void resetSparse() {
		if(sparseBlock == null)
			return;
		sparseBlock.reset(estimatedNNzsPerRow, clen);
	}
	
	private void resetDense(double val) {
		//handle to dense block allocation and
		//reset dense block to given value
		if( denseBlock != null )
			denseBlock.reset(rlen, clen, val);
		else if( val != 0 ) {
			allocateDenseBlock(false);
			denseBlock.set(val);
		}
	}
	
	/**
	 * NOTE: This method is designed only for dense representation.
	 * 
	 * @param arr 2d double array matrix
	 * @param r number of rows
	 * @param c number of columns
	 */
	public void init(double[][] arr, int r, int c) {
		//input checks 
		if ( sparse )
			throw new DMLRuntimeException("MatrixBlockDSM.init() can be invoked only on matrices with dense representation.");
		if( r*c > rlen*clen )
			throw new DMLRuntimeException("MatrixBlockDSM.init() invoked with too large dimensions ("+r+","+c+") vs ("+rlen+","+clen+")");
		
		//allocate or resize dense block
		allocateDenseBlock();
		
		//copy and compute nnz
		DenseBlock db = getDenseBlock();
		for(int i=0; i < r; i++)
			System.arraycopy(arr[i], 0, db.values(i), db.pos(i), arr[i].length);
		recomputeNonZeros();
	}
	
	/**
	 * NOTE: This method is designed only for dense representation.
	 * 
	 * @param arr double array matrix
	 * @param r number of rows
	 * @param c number of columns
	 */
	public void init(double[] arr, int r, int c) {
		//input checks 
		if ( sparse )
			throw new DMLRuntimeException("MatrixBlockDSM.init() can be invoked only on matrices with dense representation.");
		if( r*c > rlen*clen )
			throw new DMLRuntimeException("MatrixBlockDSM.init() invoked with too large dimensions ("+r+","+c+") vs ("+rlen+","+clen+")");
		
		//allocate or resize dense block
		allocateDenseBlock();
		
		//copy and compute nnz (guaranteed single block)
		System.arraycopy(arr, 0, getDenseBlockValues(), 0, arr.length);
		recomputeNonZeros();
	}

	public boolean isAllocated() {
		return sparse ? (sparseBlock!=null) : (denseBlock!=null);
	}

	public final MatrixBlock allocateDenseBlock() {
		allocateDenseBlock( true );
		return this;
	}
	
	public Future<MatrixBlock> allocateBlockAsync() {
		ExecutorService pool = LazyWriteBuffer.getUtilThreadPool();
		return (pool != null) ? pool.submit(() -> allocateBlock()) : //async
			ConcurrentUtils.constantFuture(allocateBlock()); //fallback sync
	}

	public final MatrixBlock allocateBlock() {
		if( sparse )
			allocateSparseRowsBlock();
		else
			allocateDenseBlock();
		return this;
	}
	
	public boolean allocateDenseBlock(boolean clearNNZ) {
		//allocate block if non-existing or too small (guaranteed to be 0-initialized),
		long limit = (long)rlen * clen;
		boolean reset = (denseBlock == null || denseBlock.capacity() < limit);
		if( denseBlock == null )
			denseBlock = DenseBlockFactory.createDenseBlock(rlen, clen);
		else if( denseBlock.capacity() < limit )
			denseBlock.reset(rlen, clen);
		
		//clear nnz if necessary
		if( clearNNZ )
			nonZeros = 0;
		sparse = false;
		
		return reset;
	}

	public final boolean allocateSparseRowsBlock() {
		return allocateSparseRowsBlock(true);
	}

	public boolean allocateSparseRowsBlock(boolean clearNNZ) {
		//allocate block if non-existing or too small (guaranteed to be 0-initialized)
		//but do not replace existing block even if not in default type
		boolean reset = sparseBlock == null || sparseBlock.numRows()<rlen;
		if( reset ) {
			sparseBlock = SparseBlockFactory
				.createSparseBlock(DEFAULT_SPARSEBLOCK, rlen);
		}
		//clear nnz if necessary
		if( clearNNZ ) {
			nonZeros = 0;
		}
		return reset;
	}
	
	public void allocateAndResetSparseBlock(boolean clearNNZ, SparseBlock.Type stype)
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
	 */
	public final void allocateDenseBlockUnsafe(int rl, int cl) {
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
	public final void cleanupBlock( boolean dense, boolean sparse ) {
		if(dense)
			denseBlock = null;
		if(sparse)
			sparseBlock = null;
	}
	
	////////
	// Metadata information 
	
	@Override
	public final int getNumRows() {
		return rlen;
	}
	
	/**
	 * NOTE: setNumRows() and setNumColumns() are used only in ternaryInstruction (for contingency tables)
	 * and pmm for meta corrections.
	 * 
	 * @param r number of rows
	 */
	public final void setNumRows(int r) {
		rlen = r;
	}
	
	@Override
	public final int getNumColumns() {
		return clen;
	}

	public final void setNumColumns(int c) {
		clen = c;
	}
	
	@Override
	public final long getNonZeros() {
		return nonZeros;
	}
	
	public final long setNonZeros(long nnz) {
		return (nonZeros = nnz);
	}
	
	public final double getSparsity() {
		return OptimizerUtils.getSparsity(rlen, clen, nonZeros);
	}
	
	@Override
	public final DataCharacteristics getDataCharacteristics() {
		return new MatrixCharacteristics(rlen, clen, -1, nonZeros);
	}
	
	public final boolean isVector() {
		return (rlen == 1 || clen == 1);
	}
	
	public final long getLength() {
		return (long)rlen * clen;
	}
	
	@Override
	public final boolean isEmpty() {
		return isEmptyBlock(false);
	}
	
	public final boolean isEmptyBlock() {
		return isEmptyBlock(true);
	}
	
	
	public boolean isEmptyBlock(boolean safe)
	{
		boolean ret = ( sparse && sparseBlock==null ) || ( !sparse && denseBlock==null );
		if( nonZeros==0 )
		{
			//prevent under-estimation
			if(safe)
				recomputeNonZeros();
			ret = (nonZeros==0);
		}
		return ret;
	}

	////////
	// Data handling
	
	public DenseBlock getDenseBlock() {
		return denseBlock;
	}

	public void setDenseBlock(DenseBlock dblock){
		denseBlock = dblock;
	}
	
	public double[] getDenseBlockValues() {
		//this method is used as a short-hand for all operations that
		//guaranteed only deal with dense blocks of a single block.
		if( denseBlock != null && denseBlock.numBlocks() > 1 ) {
			throw new RuntimeException("Large dense in-memory block (with numblocks="+denseBlock.numBlocks()+") "
				+ "allocated but operation access to first block only, which might cause incorrect results.");
		}
		return (denseBlock != null) ? denseBlock.valuesAt(0) : null;
	}
	
	public SparseBlock getSparseBlock() {
		return sparseBlock;
	}
	
	public void setSparseBlock(SparseBlock sblock) {
		sparseBlock = sblock;
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
			return Collections.emptyListIterator();
		
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

	public double quickGetValue(int r, int c) {
		if( sparse && sparseBlock!=null )
			return sparseBlock.get(r, c);
		else if( !sparse && denseBlock!=null )
			return denseBlock.get(r, c);
		return 0;
	}

	public void quickSetValue(int r, int c, double v) 
	{
		if(sparse) {
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
		else {
			//early abort
			if( denseBlock==null && v==0 )
				return;
			
			//allocate and init dense block (w/o overwriting nnz)
			allocateDenseBlock(false);
			
			//set value and maintain nnz
			if( denseBlock.get(r, c)==0 )
				nonZeros++;
			denseBlock.set(r, c, v);
			if( v==0 )
				nonZeros--;
		}
	}

	/**
	 * Thread save set.
	 * Blocks need to be allocated, and in case of MCSR sparse, all rows 
	 * that are going to be accessed need to be allocated as well.
	 * 
	 * @param r row 
	 * @param c column 
	 * @param v value
	 */
	public void quickSetValueThreadSafe(int r, int c, double v) {
		if(sparse) {
			if(!(sparseBlock instanceof SparseBlockMCSR))
				throw new RuntimeException("Only MCSR Blocks are supported for Multithreaded sparse set.");
			synchronized (sparseBlock.get(r)) {
				sparseBlock.set(r,c,v);
			}
		}
		else
			denseBlock.set(r,c,v);
	}

	public double quickGetValueThreadSafe(int r, int c) {
		if(sparse) {
			if(!(sparseBlock instanceof SparseBlockMCSR))
				throw new RuntimeException("Only MCSR Blocks are supported for Multithreaded sparse get.");
			synchronized (sparseBlock.get(r)) {
				return sparseBlock.get(r,c);
			}
		}
		else
			return denseBlock.get(r,c);
	}

	public double getValueDenseUnsafe(int r, int c) {
		if(denseBlock==null)
			return 0;
		return denseBlock.get(r, c);
	}
	
	public boolean containsValue(double pattern) {
		//fast paths: infer from meta data only
		if(isEmptyBlock(true))
			return pattern==0;
		if( nonZeros < getLength() && pattern == 0 )
			return true;
		
		//make a pass over the data to determine if it includes the
		//pattern, with early abort as soon as the pattern is found
		boolean NaNpattern = Double.isNaN(pattern);
		if( isInSparseFormat() ) {
			SparseBlock sb = getSparseBlock();
			for(int i=0; i<rlen; i++) {
				if( sb.isEmpty(i) ) continue;
				int apos = sb.pos(i);
				int alen = sb.size(i);
				double[] avals = sb.values(i);
				for( int j=apos; j<apos+alen; j++ )
					if(avals[j]==pattern || (NaNpattern && Double.isNaN(avals[j])))
						return true;
			}
		}
		else {
			DenseBlock db = getDenseBlock();
			for(int i=0; i<rlen; i++) {
				double[] vals = db.values(i);
				int pos = db.pos(i);
				for(int j=pos; j<pos+clen; j++)
					if(vals[j]==pattern || (NaNpattern && Double.isNaN(vals[j])))
						return true;
			}
		}
		return false;
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

		if( !sparse ) { //DENSE 
			//allocate on demand (w/o overwriting nnz)
			allocateDenseBlock(false);
			
			//set value and maintain nnz
			denseBlock.set(r, c, v);
			nonZeros++;
		}
		else { //SPARSE
			//allocation on demand (w/o overwriting nnz)
			allocateSparseRowsBlock(false);
			sparseBlock.allocate(r, estimatedNNzsPerRow, clen);
			
			//set value and maintain nnz
			sparseBlock.append(r, c, v);
			nonZeros++;
		}
	}

	public void appendValuePlain(int r, int c, double v) {
		//early abort (append guarantees no overwrite)
		if( v == 0 ) 
			return;

		if( !sparse ) { //DENSE 
			//allocate on demand (w/o overwriting nnz)
			allocateDenseBlock(false);
			
			//set value and maintain nnz
			denseBlock.set(r, c, v);
		}
		else { //SPARSE
			//allocation on demand (w/o overwriting nnz)
			allocateSparseRowsBlock(false);
			sparseBlock.allocate(r, estimatedNNzsPerRow, clen);
			
			//set value and maintain nnz
			sparseBlock.append(r, c, v);
		}
	}
	
	public void appendRow(int r, SparseRow row) {
		appendRow(r, row, true);
	}
	
	public void appendRow(int r, SparseRow row, boolean deep)
	{
		if(row == null)
			return;
		
		if(sparse) {
			//allocation on demand
			allocateSparseRowsBlock(false);
			sparseBlock.set(r, row, deep);
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
		
		//append individual rows
		int m2 = that.rlen;
		for(int i=0; i<m2; i++)
			appendRowToSparse(sparseBlock, that, i, rowoffset, coloffset, deep);
	}
	
	public void appendRowToSparse( SparseBlock dest, MatrixBlock src, int i, int rowoffset, int coloffset, boolean deep ) {
		if( src.sparse ) //SPARSE <- SPARSE
		{
			SparseBlock a = src.sparseBlock;
			if( a.isEmpty(i) ) return;
			int aix = rowoffset+i;
			
			//single block append (avoid re-allocations)
			if( !dest.isAllocated(aix) && coloffset==0 ) { 
				//note: the deep copy flag is only relevant for MCSR due to
				//shallow references of b.get(i); other block formats do not
				//require a redundant copy because b.get(i) created a new row.
				boolean ldeep = (deep && a instanceof SparseBlockMCSR);
				dest.set(aix, a.get(i), ldeep);
			}
			else { //general case
				int pos = a.pos(i);
				int len = a.size(i);
				int[] ix = a.indexes(i);
				double[] val = a.values(i);
				if( estimatedNNzsPerRow > 0 )
					dest.allocate(aix, Math.max(estimatedNNzsPerRow, dest.size(aix)+len), clen);
				else
					dest.allocate(aix, dest.size(aix)+len);
				for( int j=pos; j<pos+len; j++ )
					dest.append(aix, coloffset+ix[j], val[j]);
			}
		}
		else //SPARSE <- DENSE
		{
			DenseBlock a = src.getDenseBlock();
			final int n2 = src.clen;
			double[] avals = a.values(i);
			int aix = a.pos(i);
			int cix = rowoffset + i;
			for( int j=0; j<n2; j++ ) {
				double bval = avals[aix+j];
				if( bval != 0 ) {
					dest.allocate(cix, estimatedNNzsPerRow, clen);
					dest.append(cix, coloffset+j, bval);
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
			if( !sparseBlock.isEmpty(i) )
				sparseBlock.sort(i);
	}
	
	/**
	 * Utility function for computing the min non-zero value. 
	 * 
	 * @return minimum non-zero value
	 */
	public double minNonZero() {
		//check for empty block and return immediately
		if( isEmptyBlock() )
			return -1;
		
		//NOTE: usually this method is only applied on dense vectors and hence not really tuned yet.
		double min = Double.POSITIVE_INFINITY;
		for( int i=0; i<rlen; i++ )
			for( int j=0; j<clen; j++ ){
				double val = quickGetValue(i, j);
				if( val != 0 )
					min = Math.min(min, val);
			}
		
		return min;
	}
	
	/**
	 * Wrapper method for reduceall-product of a matrix.
	 * 
	 * @return the product sum of the matrix content
	 */
	public double prod() {
		MatrixBlock out = new MatrixBlock(1, 1, false);
		LibMatrixAgg.aggregateUnaryMatrix(this, out,
			InstructionUtils.parseBasicAggregateUnaryOperator("ua*", 1));
		return out.quickGetValue(0, 0);
	}
	
	/**
	 * Wrapper method for reduceall-mean of a matrix.
	 * 
	 * @return the mean value of all values in the matrix
	 */
	public double mean() {
		MatrixBlock out = new MatrixBlock(1, 3, false);
		LibMatrixAgg.aggregateUnaryMatrix(this, out,
			InstructionUtils.parseBasicAggregateUnaryOperator("uamean", 1));
		return out.quickGetValue(0, 0);
	}
	
	/**
	 * Wrapper method for reduceall-min of a matrix.
	 * 
	 * @return the minimum value of all values in the matrix
	 */
	public double min() {
		MatrixBlock out = new MatrixBlock(1, 1, false);
		LibMatrixAgg.aggregateUnaryMatrix(this, out,
			InstructionUtils.parseBasicAggregateUnaryOperator("uamin", 1));
		return out.quickGetValue(0, 0);
	}
	
	/**
	 * Wrapper method for reduceall-max of a matrix.
	 * 
	 * @return the maximum value of all values in the matrix
	 */
	public double max() {
		MatrixBlock out = new MatrixBlock(1, 1, false);
		LibMatrixAgg.aggregateUnaryMatrix(this, out,
			InstructionUtils.parseBasicAggregateUnaryOperator("uamax", 1));
		return out.quickGetValue(0, 0);
	}
	
	/**
	 * Wrapper method for reduceall-sum of a matrix.
	 * 
	 * @return Sum of the values in the matrix.
	 */
	public double sum() {
		KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
		return sumWithFn(kplus);
	}

	/**
	 * Wrapper method for reduceall-sumSq of a matrix.
	 *
	 * @return Sum of the squared values in the matrix.
	 */
	public double sumSq() {
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
	 */
	private double sumWithFn(KahanFunction kfunc) {
		//construct operator
		CorrectionLocationType corrLoc = CorrectionLocationType.LASTCOLUMN;
		ReduceAll reduceAllObj = ReduceAll.getReduceAllFnObject();
		AggregateOperator aop = new AggregateOperator(0, kfunc, corrLoc);
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
	@Override
	public boolean isInSparseFormat() {
		return sparse;
	}
	
	public boolean isUltraSparse() {
		return isUltraSparse(true);
	}

	public boolean isUltraSparse(boolean checkNnz) {
		double sp = ((double)nonZeros/rlen)/clen;
		//check for sparse representation in order to account for vectors in dense
		return sparse && sp<ULTRA_SPARSITY_TURN_POINT 
			&& (!checkNnz || nonZeros<ULTRA_SPARSE_BLOCK_NNZ);
	}
	
	public boolean isSparsePermutationMatrix() {
		if( !isInSparseFormat() || nonZeros > rlen )
			return false;
		SparseBlock sblock = getSparseBlock();
		boolean isPM = (sblock != null);
		for( int i=0; i<rlen & isPM; i++ )
			isPM &= sblock.isEmpty(i) || sblock.size(i) == 1;
		return isPM;
	}
	
	private boolean isUltraSparseSerialize(boolean sparseDst) {
		return nonZeros<rlen && sparseDst;
	}

	/**
	 * Evaluates if this matrix block should be in sparse format in
	 * memory. Note that this call does not change the representation - 
	 * for this please call examSparsity.
	 * 
	 * @return true if matrix block should be in sparse format in memory
	 */
	public boolean evalSparseFormatInMemory() {
		//ensure exact size estimates for write
		if( nonZeros<=0 )
			recomputeNonZeros();
		
		//decide on in-memory representation
		return evalSparseFormatInMemory(rlen, clen, nonZeros);
	}
	
	@SuppressWarnings("unused")
	private boolean evalSparseFormatInMemory(boolean transpose)
	{
		int lrlen = (transpose) ? clen : rlen;
		int lclen = (transpose) ? rlen : clen;
		long lnonZeros = nonZeros;
		
		//ensure exact size estimates for write
		if( lnonZeros<=0 ) {
			recomputeNonZeros();
			lnonZeros = nonZeros;
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
	public boolean evalSparseFormatOnDisk() {
		//ensure exact size estimates for write
		if( nonZeros <= 0 )
			recomputeNonZeros();
		
		//decide on in-memory representation
		return evalSparseFormatOnDisk(rlen, clen, nonZeros);
	}
	
	/**
	 * Evaluates if this matrix block should be in sparse format in
	 * memory. Depending on the current representation, the state of the
	 * matrix block is changed to the right representation if necessary. 
	 * Note that this consumes for the time of execution memory for both 
	 * representations.
	 * 
	 * Allowing CSR format is default for this operation.
	 * 
	 */
	public void examSparsity() {
		examSparsity(true);
	}
	
	/**
	 * Evaluates if this matrix block should be in sparse format in
	 * memory. Depending on the current representation, the state of the
	 * matrix block is changed to the right representation if necessary. 
	 * Note that this consumes for the time of execution memory for both 
	 * representations.
	 * 
	 * @param allowCSR allow CSR format on dense to sparse conversion
	 */
	public void examSparsity(boolean allowCSR) {
		//determine target representation
		boolean sparseDst = evalSparseFormatInMemory(); 
		
		//check for empty blocks (e.g., sparse-sparse)
		if( isEmptyBlock(false) ) {
			cleanupBlock(true, true);
		}
		
		//change representation if required (also done for 
		//empty blocks in order to set representation flags)
		if( sparse && !sparseDst)
			sparseToDense();
		else if( !sparse && sparseDst )
			denseToSparse(allowCSR);
	}
	
	public static boolean evalSparseFormatInMemory(DataCharacteristics dc) {
		return evalSparseFormatInMemory(dc.getRows(), dc.getCols(), dc.getNonZeros());
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
	
	private void denseToSparse() {
		denseToSparse(true);
	}
	
	private void denseToSparse(boolean allowCSR)
	{
		DenseBlock a = getDenseBlock();
		
		//set target representation, early abort on empty blocks
		sparse = true;
		if( a == null )
			return;
		
		final int m = rlen;
		final int n = clen;
		
		if( allowCSR && nonZeros <= Integer.MAX_VALUE ) {
			//allocate target in memory-efficient CSR format
			int lnnz = (int) nonZeros;
			int[] rptr = new int[m+1];
			int[] indexes = new int[lnnz];
			double[] values = new double[lnnz];
			for( int i=0, pos=0; i<m; i++ ) {
				double[] avals = a.values(i);
				int aix = a.pos(i);
				for(int j=0; j<n; j++) {
					double aval = avals[aix+j];
					if( aval != 0 ) {
						indexes[pos] = j;
						values[pos] = aval;
						pos++;
					}
				}
				rptr[i+1]=pos;
			}
			sparseBlock = new SparseBlockCSR(
				rptr, indexes, values, lnnz);
		}
		else {
			//fallback to less-memory efficient MCSR format,
			//which however allows much larger sparse matrices
			if( !allocateSparseRowsBlock() )
				reset(); //reset if not allocated
			SparseBlock sblock = sparseBlock;
			for( int i=0; i<m; i++ ) {
				double[] avals = a.values(i);
				int aix = a.pos(i);
				//compute nnz per row (not via recomputeNonZeros as sparse allocated)
				int lnnz = UtilFunctions.computeNnz(avals, aix, clen);
				if( lnnz <= 0 ) continue;
				//allocate sparse row and append non-zero values
				sblock.allocate(i, lnnz);
				for( int j=0; j<n; j++ )
					sblock.append(i, j, avals[aix+j]);
			}
		}
		
		//update nnz and cleanup dense block
		denseBlock = null;
	}
	
	public void sparseToDense() {
		//set target representation, early abort on empty blocks
		sparse = false;
		if(sparseBlock==null)
			return;
		
		int limit=rlen*clen;
		if ( limit < 0 ) {
			throw new DMLRuntimeException("Unexpected error in sparseToDense().. limit < 0: " + rlen + ", " + clen + ", " + limit);
		}
		
		//allocate dense target block, but keep nnz (no need to maintain)
		if( !allocateDenseBlock(false) )
			denseBlock.reset();
		
		//copy sparse to dense
		SparseBlock a = sparseBlock;
		DenseBlock c = getDenseBlock();
		
		for( int i=0; i<rlen; i++ ) {
			if( a.isEmpty(i) ) continue;
			int apos = a.pos(i);
			int alen = a.size(i);
			int[] aix = a.indexes(i);
			double[] avals = a.values(i);
			double[] cvals = c.values(i);
			int cix = c.pos(i);
			for( int j=apos; j<apos+alen; j++ )
				if( avals[j] != 0 )
					cvals[cix+aix[j]] = avals[j];
		}
		
		//cleanup sparse rows
		sparseBlock = null;
	}

	/**
	 * Recomputes and materializes the number of non-zero values
	 * of the entire matrix block.
	 * 
	 * @return number of non-zeros
	 */
	public long recomputeNonZeros() {
		if( sparse && sparseBlock!=null ) { //SPARSE
			//note: rlen might be <= sparseBlock.numRows()
			nonZeros = sparseBlock.size(0, sparseBlock.numRows());
		}
		else if( !sparse && denseBlock!=null ) { //DENSE
			nonZeros = denseBlock.countNonZeros();
		}
		return nonZeros;
	}
	
	public long recomputeNonZeros(int rl, int ru) {
		return recomputeNonZeros(rl, ru, 0, clen-1);
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
		if( sparse && sparseBlock!=null ) //SPARSE
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
		else if( !sparse && denseBlock!=null ) { //DENSE
			return denseBlock.countNonZeros(rl, ru+1, cl, cu+1);
		}
		
		return 0; //empty block
	}

	@Override
	public void copy(MatrixValue thatValue) {
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
	
	public MatrixBlock copyShallow(MatrixBlock that) {
		rlen = that.rlen;
		clen = that.clen;
		nonZeros = that.nonZeros;
		sparse = that.sparse;
		if( !sparse )
			denseBlock = that.denseBlock;
		else
			sparseBlock = that.sparseBlock;
		return this;
	}
	
	private void copySparseToSparse(MatrixBlock that) {
		this.nonZeros=that.nonZeros;
		if( that.isEmptyBlock(false) ) {
			resetSparse();
			return;
		}
		
		allocateSparseRowsBlock(false);
		for(int i=0; i<Math.min(that.sparseBlock.numRows(), rlen); i++) {
			if(!that.sparseBlock.isEmpty(i)) {
				sparseBlock.set(i, that.sparseBlock.get(i), true);
			}
			else if(!this.sparseBlock.isEmpty(i)) {
				this.sparseBlock.reset(i,estimatedNNzsPerRow, clen);
			}
		}
	}
	
	private void copyDenseToDense(MatrixBlock that) {
		nonZeros = that.nonZeros;
		
		//plain reset to 0 for empty input
		if( that.isEmptyBlock(false) ) {
			if(denseBlock!=null)
				denseBlock.reset(rlen, clen);
			return;
		}
		
		//allocate and copy dense block
		allocateDenseBlock(false);
		denseBlock.set(that.denseBlock);
	}
	
	private void copySparseToDense(MatrixBlock that) {
		this.nonZeros=that.nonZeros;
		if( that.isEmptyBlock(false) ) {
			if(denseBlock!=null)
				denseBlock.reset(rlen, clen);
			return;
		}
		
		//allocate and init dense block (w/o overwriting nnz)
		allocateDenseBlock(false);
		SparseBlock a = that.getSparseBlock();
		DenseBlock c = getDenseBlock();
		for( int i=0; i<Math.min(a.numRows(), rlen); i++ ) {
			if( a.isEmpty(i) ) continue;
			int pos = a.pos(i);
			int len = a.size(i);
			int[] aix = a.indexes(i);
			double[] avals = a.values(i);
			double[] cvals = c.values(i);
			int cix = c.pos(i);
			for( int j=pos; j<pos+len; j++ )
				cvals[cix+aix[j]] = avals[j];
		}
	}
	
	private void copyDenseToSparse(MatrixBlock that) {
		nonZeros = that.nonZeros;
		if( that.isEmptyBlock(false) ) {
			resetSparse();
			return;
		}
		
		if( !allocateSparseRowsBlock(false) )
			resetSparse();
		DenseBlock a = that.getDenseBlock();
		SparseBlock c = getSparseBlock();
		for(int i=0; i<rlen; i++) {
			double[] avals = a.values(i);
			int aix = a.pos(i);
			for( int j=0; j<clen; j++ ) {
				double val = avals[aix+j];
				if( val == 0 ) continue;
				//create sparse row only if required
				c.allocate(i, estimatedNNzsPerRow, clen);
				c.append(i, j, val);
			}
		}
	}
	
	
	/**
	 * In-place copy of matrix src into the index range of the existing current matrix.
	 * Note that removal of existing nnz in the index range and nnz maintenance is 
	 * only done if 'awareDestNZ=true', 
	 * 
	 * @param rl row lower index, 0-based
	 * @param ru row upper index, 0-based, inclusive
	 * @param cl column lower index, 0-based
	 * @param cu column upper index, 0-based, inclusive
	 * @param src matrix block
	 * @param awareDestNZ
	 *           true, forces (1) to remove existing non-zeros in the index range of the 
	 *                 destination if not present in src and (2) to internally maintain nnz
	 *           false, assume empty index range in destination and do not maintain nnz
	 *                  (the invoker is responsible to recompute nnz after all copies are done) 
	 */
	public void copy(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ ) {
		if(sparse && src.sparse)
			copySparseToSparse(rl, ru, cl, cu, src, awareDestNZ);
		else if(sparse && !src.sparse)
			copyDenseToSparse(rl, ru, cl, cu, src, awareDestNZ);
		else if(!sparse && src.sparse)
			copySparseToDense(rl, ru, cl, cu, src, awareDestNZ);
		else
			copyDenseToDense(rl, ru, cl, cu, src, awareDestNZ);
	}

	private void copySparseToSparse(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ) {
		//handle empty src and dest
		if( src.isEmptyBlock(false) ) {
			if( awareDestNZ && sparseBlock != null )
				copyEmptyToSparse(rl, ru, cl, cu, true);
			return;
		}
		
		allocateSparseRowsBlock(false);
		
		//copy values
		SparseBlock a = src.sparseBlock;
		SparseBlock b = sparseBlock;
		for( int i=0; i<src.rlen; i++ ) {
			if( a.isEmpty(i) ) { 
				copyEmptyToSparse(rl+i, rl+i, cl, cu, true);
				continue;
			}
			int apos = a.pos(i); 
			int alen = a.size(i);
			int[] aix = a.indexes(i);
			double[] avals = a.values(i);
			//copy row into empty target row
			if( b.isEmpty(rl+i) ) {
				if( cl == 0 ) { //no index offset needed 
					appendRow(rl+i, a.get(i), false);
					nonZeros -= alen; //avoid nnz corruption
				}
				else {
					b.allocate(rl+i, alen);
					b.setIndexRange(rl+i, cl, cu+1, avals, aix, apos, alen);
				}
				nonZeros += awareDestNZ ? alen : 0;
			}
			//insert row into non-empty target row
			else { //general case
				int lnnz = b.size(rl+i);
				b.setIndexRange(rl+i, cl, cu+1, avals, aix, apos, alen);
				nonZeros += awareDestNZ ? (b.size(rl+i) - lnnz) : 0;
			}
		}
	}
	
	private void copySparseToDense(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ) {
		//handle empty src and dest
		if( src.isEmptyBlock(false) ) {
			if( awareDestNZ && denseBlock != null ) {
				nonZeros -= recomputeNonZeros(rl, ru, cl, cu);
				denseBlock.set(rl, ru+1, cl, cu+1, 0);
			}
			return;
		}
		if(denseBlock==null)
			allocateDenseBlock();
		else if( awareDestNZ ) {
			nonZeros -= recomputeNonZeros(rl, ru, cl, cu);
			denseBlock.set(rl, ru+1, cl, cu+1, 0);
		}
		
		//copy values
		SparseBlock a = src.sparseBlock;
		DenseBlock c = getDenseBlock();
		for( int i=0; i<src.rlen; i++ ) {
			if( a.isEmpty(i) ) continue;
			int apos = a.pos(i);
			int alen = a.size(i);
			int[] aix = a.indexes(i);
			double[] avals = a.values(i);
			double[] cvals = c.values(rl+i);
			int cix = c.pos(rl+i, cl);
			for( int j=apos; j<apos+alen; j++ )
				cvals[cix+aix[j]] = avals[j];
			nonZeros += awareDestNZ ? alen : 0;
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
		DenseBlock a = src.getDenseBlock();
		SparseBlock c = getSparseBlock();
		for( int i=0; i<src.rlen; i++ )
		{
			int rix = rl + i;
			double[] avals = a.values(i);
			int aix = a.pos(i);
			if( c instanceof SparseBlockMCSR 
				&& c.isEmpty(rix) ) //special case MCSR append
			{
				//count nnz per row (fits likely in L1 cache)
				int lnnz = UtilFunctions.computeNnz(avals, aix, src.clen);
				
				//allocate row once and copy values
				if( lnnz > 0 ) {
					c.allocate(rix, lnnz);
					for( int j=0; j<src.clen; j++ ) {
						double val = avals[aix+j];
						if( val != 0 )
							c.append(rix, cl+j, val); 
					}
					if( awareDestNZ )
						nonZeros += lnnz;
				}
			}
			else if( awareDestNZ ) { //general case (w/ awareness NNZ)
				int lnnz = c.size(rix);
				if( cl==cu ) {
					double val = avals[aix];
					c.set(rix, cl, val);
				}
				else {
					c.setIndexRange(rix, cl, cu+1, avals, aix, src.clen);
				}
				nonZeros += (c.size(rix) - lnnz);
			}
			else { //general case (w/o awareness NNZ)
				for( int j=0; j<src.clen; j++ ) {
					double val = avals[aix+j];
					if( val != 0 ) 
						c.set(rix, cl+j, val);
				}
			}
		}
	}
	
	private void copyDenseToDense(int rl, int ru, int cl, int cu, MatrixBlock src, boolean awareDestNZ) {
		//handle empty src and dest
		if( src.isEmptyBlock(false) ) {
			if( awareDestNZ && denseBlock != null ) {
				nonZeros -= recomputeNonZeros(rl, ru, cl, cu);
				denseBlock.set(rl, ru+1, cl, cu+1, 0);
			}
			return;
		}
		
		//allocate output block
		//no need to clear for awareDestNZ since overwritten 
		allocateDenseBlock(false);
		
		if( awareDestNZ && (nonZeros!=getLength() || src.nonZeros!=src.getLength()) )
			nonZeros = nonZeros - recomputeNonZeros(rl, ru, cl, cu) + src.nonZeros;
		
		//copy values
		DenseBlock a = src.getDenseBlock();
		DenseBlock c = getDenseBlock();
		c.set(rl, ru+1, cl, cu+1, a);
	}
	
	private void copyEmptyToSparse(int rl, int ru, int cl, int cu, boolean updateNNZ ) {
		SparseBlock a = sparseBlock;
		if( cl==cu ) { //specific case: column vector
			for( int i=rl; i<=ru; i++ )
				if( !a.isEmpty(i) ) {
					boolean update = a.set(i, cl, 0);
					if( updateNNZ )
						nonZeros -= update ? 1 : 0;
				}
		}
		else {
			for( int i=rl; i<=ru; i++ )
				if( !a.isEmpty(i) ) {
					int lnnz = a.size(i);
					a.deleteIndexRange(i, cl, cu+1);
					if( updateNNZ )
						nonZeros += (a.size(i)-lnnz);
				}
		}
	}

	@Override
	public void merge(CacheBlock that, boolean appendOnly) {
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
	 */
	public void merge(MatrixBlock that, boolean appendOnly) {
		merge(that, appendOnly, false, true);
	}
	
	public void merge(MatrixBlock that, boolean appendOnly, boolean par) {
		merge(that, appendOnly, par, true);
	}
	
	public void merge(MatrixBlock that, boolean appendOnly, boolean par, boolean deep) {
		//check for empty input source (nothing to merge)
		if( that == null || that.isEmptyBlock(false) )
			return;
		
		//check dimensions (before potentially copy to prevent implicit dimension change) 
		//this also does a best effort check for disjoint input blocks via the number of non-zeros
		if( rlen != that.rlen || clen != that.clen )
			throw new DMLRuntimeException("Dimension mismatch on merge disjoint (target="+rlen+"x"+clen+", source="+that.rlen+"x"+that.clen+")");
		if( nonZeros+that.nonZeros > (long)rlen*clen )
			throw new DMLRuntimeException("Number of non-zeros mismatch on merge disjoint (target="+rlen+"x"+clen+", nnz target="+nonZeros+", nnz source="+that.nonZeros+")");
		
		//check for empty target (copy in full)
		if( isEmptyBlock(false) && !(!sparse && isAllocated()) ) {
			copy(that);
			return;
		}
		
		//core matrix block merge (guaranteed non-empty source/target, nnz maintenance not required)
		long nnz = nonZeros + that.nonZeros;
		if( sparse )
			mergeIntoSparse(that, appendOnly, deep);
		else if( par )
			mergeIntoDensePar(that);
		else
			mergeIntoDense(that);
		
		//maintain number of nonzeros
		nonZeros = nnz;
	}

	private void mergeIntoDense(MatrixBlock that) {
		DenseBlock a = getDenseBlock();
		if( that.sparse ) { //DENSE <- SPARSE
			SparseBlock b = that.sparseBlock;
			int m = rlen;
			for( int i=0; i<m; i++ ) {
				if( b.isEmpty(i) ) continue;
				int bpos = b.pos(i);
				int blen = b.size(i);
				int[] bix = b.indexes(i);
				double[] avals = a.values(i);
				double[] bvals = b.values(i);
				int aix = a.pos(i);
				for( int j=bpos; j<bpos+blen; j++ ) {
					double bval = bvals[j];
					if( bval != 0 )
						avals[aix+bix[j]] = bval;
				}
			}
		}
		else { //DENSE <- DENSE
			DenseBlock b = that.getDenseBlock();
			for(int bi=0; bi<a.numBlocks(); bi++) {
				double[] avals = a.valuesAt(bi);
				double[] bvals = b.valuesAt(bi);
				int blen = a.size(bi);
				for( int j=0; j<blen; j++ )
					avals[j] = bvals[j]!=0 ? bvals[j] : avals[j];
			}
		}
	}
	
	private void mergeIntoDensePar(MatrixBlock that) {
		DenseBlock a = getDenseBlock();
		if( that.sparse ) { //DENSE <- SPARSE
			SparseBlock b = that.sparseBlock;
			int roff = 0; //row offset
			for( int bi=0; bi<a.numBlocks(); bi++ ) {
				double[] avals = a.valuesAt(bi);
				int alen = a.blockSize(bi);
				final int lroff = roff; //final for lambda
				IntStream.range(lroff, lroff+alen).parallel().forEach(i -> {
					if( b.isEmpty(i) ) return;
					int aix = (i-lroff)*clen;
					int bpos = b.pos(i);
					int blen = b.size(i);
					int[] bix = b.indexes(i);
					double[] bval = b.values(i);
					for( int j=bpos; j<bpos+blen; j++ )
						if( bval[j] != 0 )
							avals[aix+bix[j]] = bval[j];
				});
				roff += alen;
			}
		}
		else { //DENSE <- DENSE
			DenseBlock b = that.getDenseBlock();
			for(int bi=0; bi<a.numBlocks(); bi++) {
				double[] avals = a.valuesAt(bi);
				double[] bvals = b.valuesAt(bi);
				Arrays.parallelSetAll(avals,
					i -> (bvals[i]!=0) ? bvals[i] : avals[i]);
			}
		}
	}

	private void mergeIntoSparse(MatrixBlock that, boolean appendOnly, boolean deep) {
		SparseBlock a = sparseBlock;
		final boolean COO = (a instanceof SparseBlockCOO);
		final int m = rlen;
		final int n = clen;
		if( that.sparse ) { //SPARSE <- SPARSE
			SparseBlock b = that.sparseBlock;
			for( int i=0; i<m; i++ ) {
				if( b.isEmpty(i) ) continue;
				if( !COO && a.isEmpty(i) ) {
					//copy entire sparse row (no sort required)
					a.set(i, b.get(i), deep);
				}
				else {
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
					if( !COO && !appendOnly && appended )
						a.sort(i);
				}
			}
		}
		else { //SPARSE <- DENSE
			DenseBlock b = that.getDenseBlock();
			for( int i=0; i<m; i++ ) {
				double[] bvals = b.values(i);
				int bix = b.pos(i);
				boolean appended = false;
				for( int j=0; j<n; j++ ) {
					double bval = bvals[bix+j];
					if( bval != 0 ) {
						appendValue(i, j, bval); //incl alloc
						appended = true;
					}
				}
				//only sort if value appended
				if( !COO && !appendOnly && appended )
					a.sort(i);
			}
		}
		//full sort of coordinate blocks
		if( COO && !appendOnly )
			a.sort();
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
		if( !allocateDenseBlock(false) ) //allocate block
			denseBlock.reset(rlen, clen);
		
		DenseBlock a = getDenseBlock();
		long nnz = 0;
		if( in instanceof MatrixBlockDataInput ) { //fast deserialize
			MatrixBlockDataInput mbin = (MatrixBlockDataInput)in;
			for( int i=0; i<a.numBlocks(); i++ )
				nnz += mbin.readDoubleArray(a.size(i), a.valuesAt(i));
		}
		else if( in instanceof DataInputBuffer && HDFSTool.USE_BINARYBLOCK_SERIALIZATION ) {
			//workaround because sequencefile.reader.next(key, value) does not yet support serialization framework
			DataInputBuffer din = (DataInputBuffer)in;
			try(FastBufferedDataInputStream mbin = new FastBufferedDataInputStream(din)) {
				for( int i=0; i<a.numBlocks(); i++ )
					nnz += mbin.readDoubleArray(a.size(i), a.valuesAt(i));
			}
		}
		else { //default deserialize
			for( int i=0; i<rlen; i++ ) {
				double[] avals = a.values(i);
				int aix = a.pos(i);
				for( int j=0; j<clen; j++ )
					nnz += ((avals[aix+j] = in.readDouble()) != 0) ? 1 : 0;
			}
		}
		nonZeros = nnz;
	}
	
	private void readSparseBlock(DataInput in) 
		throws IOException 
	{
		if( !allocateSparseRowsBlock(false) )
			resetSparse(); //reset if not allocated
		
		if( in instanceof MatrixBlockDataInput ) { //fast deserialize
			MatrixBlockDataInput mbin = (MatrixBlockDataInput)in;
			nonZeros = mbin.readSparseRows(rlen, nonZeros, sparseBlock);
		}
		else if( in instanceof DataInputBuffer && HDFSTool.USE_BINARYBLOCK_SERIALIZATION ) {
			//workaround because sequencefile.reader.next(key, value) does not yet support serialization framework
			DataInputBuffer din = (DataInputBuffer)in;
			FastBufferedDataInputStream mbin = null;
			try {
				mbin = new FastBufferedDataInputStream(din);
				nonZeros = mbin.readSparseRows(rlen, nonZeros, sparseBlock);
			}
			finally {
				IOUtilFunctions.closeSilently(mbin);
			}
		}
		else { //default deserialize
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
		if( !allocateDenseBlock(false) ) //allocate block
			denseBlock.reset(rlen, clen);
		
		DenseBlock a = getDenseBlock();
		for(int r=0; r<rlen; r++) {
			int nr = in.readInt();
			double[] avals = a.values(r);
			int cix = a.pos(r);
			for( int j=0; j<nr; j++ ) {
				int c = in.readInt();
				avals[cix+c] = in.readDouble();
			}
		}
	}

	private void readUltraSparseBlock(DataInput in)
		throws IOException 
	{
		//allocate ultra-sparse block in CSR to avoid unnecessary size overhead 
		//and to allow efficient reset without repeated sparse row allocation
		
		//adjust size and ensure reuse block is in CSR format
		allocateAndResetSparseBlock(false, SparseBlock.Type.CSR);
		
		if( clen > 1 ) { //ULTRA-SPARSE BLOCK
			//block: read ijv-triples (ordered by row and column) via custom 
			//init to avoid repeated updates of row pointers per append
			SparseBlockCSR sblockCSR = (SparseBlockCSR) sparseBlock;
			sblockCSR.initUltraSparse((int)nonZeros, in);
		}
		else { //ULTRA-SPARSE COL
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
		if( !allocateDenseBlock(false) ) //allocate block
			denseBlock.reset(rlen, clen);
		
		if( clen > 1 ) { //ULTRA-SPARSE BLOCK
			//block: read ijv-triples
			DenseBlock a = getDenseBlock();
			for(long i=0; i<nonZeros; i++) {
				int r = in.readInt();
				int c = in.readInt();
				a.set(r, c, in.readDouble());
			}
		}
		else { //ULTRA-SPARSE COL
			//col: read iv-pairs
			double[] a = getDenseBlockValues();
			for(long i=0; i<nonZeros; i++)
				a[in.readInt()] = in.readDouble();
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
			else if( isUltraSparseSerialize(sparseDst) ) 
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
			else if( isUltraSparseSerialize(sparseDst) )
				writeDenseToUltraSparse(out);
			else if( sparseDst )
				writeDenseToSparse(out);
			else
				writeDenseBlock(out);
		}
	}

	private static void writeEmptyBlock(DataOutput out) 
		throws IOException
	{
		//empty blocks do not need to materialize row information
		out.writeByte( BlockType.EMPTY_BLOCK.ordinal() );
	}

	private void writeDenseBlock(DataOutput out) 
		throws IOException 
	{
		out.writeByte( BlockType.DENSE_BLOCK.ordinal() );
		
		DenseBlock a = getDenseBlock();
		if( out instanceof MatrixBlockDataOutput ) { //fast serialize
			MatrixBlockDataOutput mout = (MatrixBlockDataOutput)out;
			for(int i=0; i<a.numBlocks(); i++)
				mout.writeDoubleArray(a.size(i), a.valuesAt(i));
		}
		else { //general case (if fast serialize not supported)
			for(int i=0; i<a.numBlocks(); i++) {
				double[] avals = a.values(i);
				int limit = a.size(i);
				for(int j=0; j<limit; j++)
					out.writeDouble(avals[j]);
			}
		}
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
			if( sparseBlock instanceof SparseBlockCOO ) {
				SparseBlockCOO sblock = (SparseBlockCOO)sparseBlock;
				int[] rix = sblock.rowIndexes();
				int[] cix = sblock.indexes();
				double[] vals = sblock.values();
				for(int i=0; i<sblock.size(); i++) {
					//ultra-sparse block: write ijv-triples
					out.writeInt(rix[i]);
					out.writeInt(cix[i]);
					out.writeDouble(vals[i]);
					wnnz++;
				}
			}
			else {
				for(int r=0;r<Math.min(rlen, sparseBlock.numRows()); r++) {
					if( sparseBlock.isEmpty(r) ) continue;
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
		if( clen > 1 ) { //ULTRA-SPARSE BLOCK
			//block: write ijv-triples
			DenseBlock a = getDenseBlock();
			for( int r=0; r<rlen; r++ ) {
				double[] avals = a.values(r);
				int aix = a.pos(r);
				for( int c=0; c<clen; c++ ) {
					double aval = avals[aix+c];
					if( aval != 0 ) {
						out.writeInt(r);
						out.writeInt(c);
						out.writeDouble(aval);
						wnnz++;
					}
				}
			}
		}
		else { //ULTRA-SPARSE COL
			//col: write iv-pairs
			double[] a = getDenseBlockValues();
			for(int r=0; r<rlen; r++) {
				double aval = a[r];
				if( aval != 0 ) {
					out.writeInt(r);
					out.writeDouble(aval);
					wnnz++;
				}
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
		
		DenseBlock a = getDenseBlock();
		for( int r=0; r<rlen; r++ ) {
			double[] avals = a.values(r);
			int aix = a.pos(r);
			out.writeInt(a.countNonZeros(r));
			for( int c=0; c<clen; c++ ) {
				double aval = avals[aix+c];
				if( aval != 0 ) {
					out.writeInt(c);
					out.writeDouble(aval);
				}
			}
		}
	}

	private long readNnzInfo( DataInput in, boolean ultrasparse ) throws IOException {
		//note: if ultrasparse, int always sufficient because nnz<rlen
		// where rlen is limited to integer
		long lrlen = rlen;
		long lclen = clen;
		
		//read long if required, otherwise int (see writeNnzInfo, consistency required)
		if( lrlen*lclen > Integer.MAX_VALUE  && !ultrasparse)
			nonZeros = in.readLong();
		else
			nonZeros = in.readInt();
		return nonZeros;
	}

	private void writeNnzInfo( DataOutput out, boolean ultrasparse ) throws IOException {
		//note: if ultrasparse, int always sufficient because nnz<rlen
		// where rlen is limited to integer
		long lrlen = rlen;
		long lclen = clen;
		
		//write long if required, otherwise int
		if( lrlen*lclen > Integer.MAX_VALUE && !ultrasparse)
			out.writeLong( nonZeros ); 
		else
			out.writeInt( (int)nonZeros );
	}
	
	/**
	 * Redirects the default java serialization via externalizable to our default 
	 * hadoop writable serialization for efficient broadcast/rdd deserialization. 
	 * 
	 * @param is object input
	 * @throws IOException if IOException occurs
	 */
	@Override
	public void readExternal(ObjectInput is) 
		throws IOException
	{
		if( is instanceof ObjectInputStream
			&& !(is instanceof MatrixBlockDataInput) )
		{
			//fast deserialize of dense/sparse blocks
			ObjectInputStream ois = (ObjectInputStream)is;
			FastBufferedDataInputStream fis = new FastBufferedDataInputStream(ois);
			readFields(fis); //note: cannot close fos as this would close oos
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
	@Override
	public void writeExternal(ObjectOutput os) 
		throws IOException
	{
		//note: in case of a CorrMatrixBlock being wrapped around a matrix
		//block, the object output is already a FastBufferedDataOutputStream;
		//so in general we try to avoid unnecessary buffer allocations here.
		
		if( os instanceof ObjectOutputStream && !isEmptyBlock(false)
			&& !(os instanceof MatrixBlockDataOutput) ) {
			//fast serialize of dense/sparse blocks
			ObjectOutputStream oos = (ObjectOutputStream)os;
			FastBufferedDataOutputStream fos = new FastBufferedDataOutputStream(oos);
			write(fos); //note: cannot close fos as this would close oos
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
		
		long lrlen = rlen;
		long lclen = clen;
		
		//ensure exact size estimates for write
		if( nonZeros <= 0 ) {
			recomputeNonZeros();
		}
		
		//get exact size estimate (see write for the corresponding meaning)
		if( sparseSrc )
		{
			//write sparse to *
			if(sparseBlock==null || nonZeros==0)
				return HEADER_SIZE; //empty block
			else if( nonZeros<lrlen && sparseDst )
				return estimateSizeUltraSparseOnDisk(lrlen, lclen, nonZeros); //ultra sparse block
			else if( sparseDst )
				return estimateSizeSparseOnDisk(lrlen, lclen, nonZeros); //sparse block
			else 
				return estimateSizeDenseOnDisk(lrlen, lclen); //dense block
		}
		else
		{
			//write dense to *
			if(denseBlock==null || nonZeros==0)
				return HEADER_SIZE; //empty block
			else if( nonZeros<lrlen && sparseDst )
				return estimateSizeUltraSparseOnDisk(lrlen, lclen, nonZeros); //ultra sparse block
			else if( sparseDst )
				return estimateSizeSparseOnDisk(lrlen, lclen, nonZeros); //sparse block
			else
				return estimateSizeDenseOnDisk(lrlen, lclen); //dense block
		}
	}
	
	////////
	// Estimates size and sparsity

	public static long getHeaderSize() {
		// basic variables and references sizes
		long size = 16; // header
		size += 12; // 3 x ints (rlen, clen, ennz/row)
		size += 1; // boolean (sparse)
		size += 3; // padding
		size += 8; // nonZeros
		size += 2 * 8; // object references
		return size;
	}
	
	public long estimateSizeInMemory() {
		return estimateSizeInMemory(rlen, clen, getSparsity());
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

	public static long estimateSizeDenseInMemory(long nrows, long ncols) {
		double size = getHeaderSize()
			+ DenseBlockFactory.estimateSizeDenseInMemory(nrows, ncols);
		// robustness for long overflows
		return (long) Math.min(size, Long.MAX_VALUE);
	}

	public static long estimateSizeSparseInMemory(long nrows, long ncols, double sparsity) {
		return estimateSizeSparseInMemory(nrows, ncols, sparsity, DEFAULT_SPARSEBLOCK);
	}
	
	public static long estimateSizeSparseInMemory(long nrows, long ncols, double sparsity, SparseBlock.Type stype) {
		double size = getHeaderSize() + ((sparsity == 0) ? 0 : //allocated on demand
			SparseBlockFactory.estimateSizeSparseInMemory(stype, nrows, ncols, sparsity));
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
		if( !op.sparseSafe && !(op.fn instanceof Divide && m2.getSparsity()==1.0) ) {
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
			estnnz = OptimizerUtils.getOuterNonZeros(
				m, n, nz1, nz2, op.getBinaryOperatorOpOp2());
		}
		else //DEFAULT CASE
		{
			if( atype == BinaryAccessType.MATRIX_COL_VECTOR )
				nz2 = nz2 * n;
			else if( atype == BinaryAccessType.MATRIX_ROW_VECTOR )
				nz2 = nz2 * m;
			
			//compute output sparsity consistent w/ the hop compiler
			double sp1 = OptimizerUtils.getSparsity(m, n, nz1);
			double sp2 = OptimizerUtils.getSparsity(m, n, nz2);
			double spout = OptimizerUtils.getBinaryOpSparsity(
				sp1, sp2, op.getBinaryOperatorOpOp2(), true);
			estnnz = UtilFunctions.toLong(spout * m * n);
		}
		
		est.sparse = evalSparseFormatInMemory(m, n, estnnz);
		est.estimatedNonZeros = estnnz;
		
		return est;
	}
	
	private boolean estimateSparsityOnSlice(int selectRlen, int selectClen, int finalRlen, int finalClen) {
		long ennz = (long)((double)nonZeros/rlen/clen*selectRlen*selectClen);
		return evalSparseFormatInMemory(finalRlen, finalClen, ennz); 
	}
	
	private static boolean estimateSparsityOnLeftIndexing(
		long rlenm1, long clenm1, long nnzm1, long rlenm2, long clenm2, long nnzm2) {
		//min bound: nnzm1 - rlenm2*clenm2 + nnzm2
		//max bound: min(rlenm1*rlenm2, nnzm1+nnzm2)
		long ennz = Math.min(rlenm1*clenm1, nnzm1+nnzm2);
		return evalSparseFormatInMemory(rlenm1, clenm1, ennz);
	}
	
	private boolean requiresInplaceSparseBlockOnLeftIndexing(boolean sparse, UpdateType update, long nnz) {
		return sparse && update != UpdateType.INPLACE_PINNED
			&& !isShallowSerialize() && (nnz <= Integer.MAX_VALUE
				|| DEFAULT_INPLACE_SPARSEBLOCK==SparseBlock.Type.MCSR);
	}
	
	private static boolean estimateSparsityOnGroupedAgg( long rlen, long groups ) {
		long ennz = Math.min(groups, rlen);
		return evalSparseFormatInMemory(groups, 1, ennz);
	}
	
	////////
	// CacheBlock implementation
	
	@Override
	public long getInMemorySize() {
		//in-memory size given by header if not allocated
		if( !isAllocated() ) 
			return getHeaderSize();
		//in-memory size of dense/sparse representation
		return !sparse ? estimateSizeDenseInMemory(rlen, clen) :
			estimateSizeSparseInMemory(rlen, clen, getSparsity(),
			SparseBlockFactory.getSparseBlockType(sparseBlock));
	}
	
	@Override
	public long getExactSerializedSize() {
		return getExactSizeOnDisk();
	}
	
	@Override
	public boolean isShallowSerialize() {
		return isShallowSerialize(false);
	}
	
	@Override
	public boolean isShallowSerialize(boolean inclConvert) {
		//shallow serialize if dense, dense in serialized form or already in CSR
		boolean sparseDst = evalSparseFormatOnDisk();
		return !sparse || !sparseDst
			|| (sparse && sparseBlock instanceof SparseBlockCSR)
			|| (sparse && sparseBlock instanceof SparseBlockMCSR
				&& getInMemorySize() / MAX_SHALLOW_SERIALIZE_OVERHEAD 
				<= getExactSerializedSize())
			|| (sparse && sparseBlock instanceof SparseBlockMCSR
				&& nonZeros < Integer.MAX_VALUE //CSR constraint
				&& inclConvert && CONVERT_MCSR_TO_CSR_ON_DEEP_SERIALIZE
				&& !isUltraSparseSerialize(sparseDst));
	}
	
	@Override 
	public void toShallowSerializeBlock() {
		if( isShallowSerialize() || !isShallowSerialize(true) )
			return;
		sparseBlock = SparseBlockFactory.copySparseBlock(
			SparseBlock.Type.CSR, sparseBlock, false);
	}
	
	@Override
	public void compactEmptyBlock() {
		if( isEmptyBlock(false) && isAllocated() )
			cleanupBlock(true, true);
	}
	
	////////
	// Core block operations (called from instructions)

	@Override
	public MatrixBlock scalarOperations(ScalarOperator op, MatrixValue result) {
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
		if( op.getNumThreads() > 1 )
			LibMatrixBincell.bincellOp(this, ret, op, op.getNumThreads());
		else
			LibMatrixBincell.bincellOp(this, ret, op);
		
		return ret;
	}

	@Override
	public MatrixBlock unaryOperations(UnaryOperator op, MatrixValue result) {
		MatrixBlock ret = checkType(result);
		
		// estimate the sparsity structure of result matrix
		// by default, we guess result.sparsity=input.sparsity, unless not sparse safe
		boolean sp = this.sparse && op.sparseSafe;
		
		//allocate output
		int n = Builtin.isBuiltinCode(op.fn, BuiltinCode.CUMSUMPROD) ? 1 : clen;
		if( ret == null )
			ret = new MatrixBlock(rlen, n, sp, sp ? nonZeros : rlen*n);
		else
			ret.reset(rlen, n, sp);
		
		//early abort for comparisons w/ special values
		if( Builtin.isBuiltinCode(op.fn, BuiltinCode.ISNAN, BuiltinCode.ISNA))
			if( !containsValue(op.getPattern()) )
				return ret; //avoid unnecessary allocation
		
		//core execute
		if( LibMatrixAgg.isSupportedUnaryOperator(op) ) {
			//e.g., cumsum/cumprod/cummin/cumax/cumsumprod
			if( op.getNumThreads() > 1 )
				ret = LibMatrixAgg.cumaggregateUnaryMatrix(this, ret, op, op.getNumThreads());
			else
				ret = LibMatrixAgg.cumaggregateUnaryMatrix(this, ret, op);
		}
		else if(!sparse && !isEmptyBlock(false)
			&& OptimizerUtils.isMaxLocalParallelism(op.getNumThreads())) {
			//note: we apply multi-threading in a best-effort manner here
			//only for expensive operators such as exp, log, sigmoid, because
			//otherwise allocation, read and write anyway dominates
			ret.allocateDenseBlock(false);
			DenseBlock a = getDenseBlock();
			DenseBlock c = ret.getDenseBlock();
			for(int bi=0; bi<a.numBlocks(); bi++) {
				double[] avals = a.valuesAt(bi), cvals = c.valuesAt(bi);
				Arrays.parallelSetAll(cvals, i -> op.fn.execute(avals[i]));
			}
			ret.recomputeNonZeros();
		}
		else {
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

	private void sparseUnaryOperations(UnaryOperator op, MatrixBlock ret) {
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
			ret.allocateDenseBlock(false);
			SparseBlock a = sparseBlock;
			DenseBlock c = ret.denseBlock;
			long nnz = (ret.nonZeros > 0) ?
				(long) m*n-a.size() : 0;
			for(int i=0; i<m; i++) {
				if( a.isEmpty(i) ) continue;
				int apos = a.pos(i);
				int alen = a.size(i);
				int[] aix = a.indexes(i);
				double[] avals = a.values(i);
				double[] cvals = c.values(i);
				int cix = c.pos(i);
				for( int j=apos; j<apos+alen; j++ ) {
					double val = op.fn.execute(avals[j]);
					cvals[cix + aix[j]] = val; 
					nnz += (val != 0) ? 1 : 0;
				}
			}
			ret.nonZeros = nnz;
		}
		else //DENSE <- DENSE
		{
			//allocate dense output block
			ret.allocateDenseBlock(false);
			DenseBlock da = getDenseBlock();
			DenseBlock dc = ret.getDenseBlock();
			
			//unary op, incl nnz maintenance
			long nnz = 0;
			for( int bi=0; bi<da.numBlocks(); bi++ ) {
				double[] a = da.valuesAt(bi);
				double[] c = dc.valuesAt(bi);
				int len = da.size(bi);
				for( int i=0; i<len; i++ ) {
					c[i] = op.fn.execute(a[i]);
					nnz += (c[i] != 0) ? 1 : 0;
				}
			}
			ret.nonZeros = nnz;
		}
	}

	private void denseUnaryOperations(UnaryOperator op, MatrixBlock ret) {
		//prepare 0-value init (determine if unnecessarily sparse-unsafe)
		double val0 = op.fn.execute(0d);
		
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
		if( sparse && val0 != 0 ) {
			ret.reset(m, n, val0);
			ret.nonZeros = (long)m * n;
		}
		sparseUnaryOperations(op, ret);
	}

	@Override
	public MatrixBlock binaryOperations(BinaryOperator op, MatrixValue thatValue, MatrixValue result) {
		MatrixBlock that = checkType(thatValue);
		MatrixBlock ret = checkType(result);
		LibMatrixBincell.isValidDimensionsBinary(this, that);
		
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
		if( op.getNumThreads() > 1 )
			LibMatrixBincell.bincellOp( this, that, ret, op, op.getNumThreads() );
		else
			LibMatrixBincell.bincellOp( this, that, ret, op );
		
		return ret;
	}

	@Override
	public MatrixBlock binaryOperationsInPlace(BinaryOperator op, MatrixValue thatValue) {
		MatrixBlock that=checkType(thatValue);
		LibMatrixBincell.isValidDimensionsBinary(this, that);
	
		//estimate output sparsity
		SparsityEstimate resultSparse = estimateSparsityOnBinary(this, that, op);
		if(resultSparse.sparse && !this.sparse)
			denseToSparse();
		else if(!resultSparse.sparse && this.sparse)
			sparseToDense();
		
		//core binary cell operation
		LibMatrixBincell.bincellOpInPlace(this, that, op);
		return this;
	}
	
	public MatrixBlock ternaryOperations(TernaryOperator op, MatrixBlock m2, MatrixBlock m3, MatrixBlock ret) {
		if(m2 instanceof CompressedMatrixBlock)
			m2 = ((CompressedMatrixBlock) m2).getUncompressed("Ternay Operator arg2 " + op.fn.getClass().getSimpleName());
		if(m3 instanceof CompressedMatrixBlock)
			m3 = ((CompressedMatrixBlock) m3).getUncompressed("Ternay Operator arg3 " + op.fn.getClass().getSimpleName());
		
		//prepare inputs
		final boolean s1 = (rlen==1 && clen==1);
		final boolean s2 = (m2.rlen==1 && m2.clen==1);
		final boolean s3 = (m3.rlen==1 && m3.clen==1);
		final double d1 = s1 ? quickGetValue(0, 0) : Double.NaN;
		final double d2 = s2 ? m2.quickGetValue(0, 0) : Double.NaN;
		final double d3 = s3 ? m3.quickGetValue(0, 0) : Double.NaN;
		final int m = Math.max(Math.max(rlen, m2.rlen), m3.rlen);
		final int n = Math.max(Math.max(clen, m2.clen), m3.clen);
		final long nnz = nonZeros;
		
		//error handling 
		if( (!s1 && (rlen != m || clen != n))
			|| (!s2 && (m2.rlen != m || m2.clen != n))
			|| (!s3 && (m3.rlen != m || m3.clen != n)) ) {
			throw new DMLRuntimeException("Block sizes are not matched for ternary cell operations: "
				+ rlen + "x" + clen + " vs " + m2.rlen + "x" + m2.clen + " vs " + m3.rlen + "x" + m3.clen);
		}
		
		//prepare result
		boolean sparseOutput = (op.fn instanceof PlusMultiply || op.fn instanceof MinusMultiply)?
			evalSparseFormatInMemory(m, n, (s1?m*n*(d1!=0?1:0):getNonZeros())
				+ Math.min(s2?m*n:m2.getNonZeros(), s3?m*n:m3.getNonZeros())) : false;
		ret.reset(m, n, sparseOutput);
		
		if( op.fn instanceof IfElse && (s1 || nnz==0 || nnz==(long)m*n) ) {
			//SPECIAL CASE for shallow-copy if-else
			boolean expr = s1 ? (d1 != 0) : (nnz==(long)m*n);
			MatrixBlock tmp = expr ? m2 : m3;
			if( tmp.rlen==m && tmp.clen==n ) {
				//shallow copy incl meta data
				ret.copyShallow(tmp);
			}
			else {
				//fill output with given scalar value
				double tmpVal = tmp.quickGetValue(0, 0);
				if( tmpVal != 0 ) {
					ret.allocateDenseBlock();
					ret.denseBlock.set(tmpVal);
					ret.nonZeros = (long)m * n;
				}
			}
		}
		else if (s2 != s3 && (op.fn instanceof PlusMultiply || op.fn instanceof MinusMultiply) ) {
			//SPECIAL CASE for sparse-dense combinations of common +* and -*
			BinaryOperator bop = ((ValueFunctionWithConstant)op.fn).setOp2Constant(s2 ? d2 : d3);
			if( op.getNumThreads() > 1 )
				LibMatrixBincell.bincellOp(this, s2 ? m3 : m2, ret, bop, op.getNumThreads());
			else
				LibMatrixBincell.bincellOp(this, s2 ? m3 : m2, ret, bop);
		}
		else {
			//DEFAULT CASE
			LibMatrixTercell.tercellOp(this, m2, m3, ret, op);
			
			//ensure correct output representation
			ret.examSparsity();
		}
		
		return ret;
	}
	
	@Override
	public void incrementalAggregate(AggregateOperator aggOp, MatrixValue correction, MatrixValue newWithCorrection, boolean deep) {
		//assert(aggOp.correctionExists); 
		MatrixBlock cor=checkType(correction);
		MatrixBlock newWithCor=checkType(newWithCorrection);
		KahanObject buffer=new KahanObject(0, 0);
		
		if(aggOp.correction==CorrectionLocationType.LASTROW) {
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
		}
		else if(aggOp.correction==CorrectionLocationType.LASTCOLUMN) {
			if(aggOp.increOp.fn instanceof Builtin 
				&& ( ((Builtin)(aggOp.increOp.fn)).bFunc == Builtin.BuiltinCode.MAXINDEX
					|| ((Builtin)(aggOp.increOp.fn)).bFunc == Builtin.BuiltinCode.MININDEX ) ) {
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
							buffer._correction=cor.quickGetValue(r, 0);
							buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newWithCor.quickGetValue(r, c), newWithCor.quickGetValue(r, c+1));
							quickSetValue(r, c, buffer._sum);
							cor.quickSetValue(r, 0, buffer._correction);
						}
				}
		}
		else if(aggOp.correction==CorrectionLocationType.NONE) {
			//e.g., ak+ kahan plus as used in sum, mapmult, mmcj and tsmm
			if(aggOp.increOp.fn instanceof KahanPlus) {
				LibMatrixAgg.aggregateBinaryMatrix(newWithCor, this, cor, deep);
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
						for(int c=0; c<clen; c++) {
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
		else if(aggOp.correction==CorrectionLocationType.LASTTWOROWS) {
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
		}
		else if(aggOp.correction==CorrectionLocationType.LASTTWOCOLUMNS) {
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
		else if (aggOp.correction == CorrectionLocationType.LASTFOURROWS
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
		else if (aggOp.correction == CorrectionLocationType.LASTFOURCOLUMNS
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
			throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correction);
	}
	
	@Override
	public void incrementalAggregate(AggregateOperator aggOp, MatrixValue newWithCorrection) {
		//assert(aggOp.correctionExists);
		MatrixBlock newWithCor=checkType(newWithCorrection);
		KahanObject buffer=new KahanObject(0, 0);
		
		if(aggOp.correction==CorrectionLocationType.LASTROW)
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
		else if(aggOp.correction==CorrectionLocationType.LASTCOLUMN)
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
				if(aggOp.increOp.fn instanceof KahanPlus) {
					LibMatrixAgg.aggregateBinaryMatrix(newWithCor, this, aggOp);
				}
				else {
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
		else if(aggOp.correction==CorrectionLocationType.LASTTWOROWS)
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
			
		}else if(aggOp.correction==CorrectionLocationType.LASTTWOCOLUMNS)
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
		else if (aggOp.correction == CorrectionLocationType.LASTFOURROWS
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
		else if (aggOp.correction == CorrectionLocationType.LASTFOURCOLUMNS
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
			throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correction);
	}

	@Override
	public MatrixBlock reorgOperations(ReorgOperator op, MatrixValue ret, int startRow, int startColumn, int length)
	{
		if ( !( op.fn instanceof SwapIndex || op.fn instanceof DiagIndex 
			|| op.fn instanceof SortIndex || op.fn instanceof RevIndex ) )
			throw new DMLRuntimeException("the current reorgOperations cannot support: "+op.fn.getClass()+".");
		
		MatrixBlock result = checkType(ret);

		//compute output dimensions and sparsity, note that for diagM2V,
		//the input nnz might be much larger than the nnz of the output
		CellIndex tempCellIndex = new CellIndex(-1,-1);
		op.fn.computeDimension( rlen, clen, tempCellIndex );
		long ennz = Math.min(nonZeros, (long)tempCellIndex.row*tempCellIndex.column);
		boolean sps = evalSparseFormatInMemory(tempCellIndex.row, tempCellIndex.column, ennz);

		//prepare output matrix block w/ right meta data
		if( result == null )
			result = new MatrixBlock(tempCellIndex.row, tempCellIndex.column, sps, nonZeros);
		else
			result.reset(tempCellIndex.row, tempCellIndex.column, sps, nonZeros);
		
		if( LibMatrixReorg.isSupportedReorgOperator(op) )
		{
			//SPECIAL case (operators with special performance requirements, 
			//or size-dependent special behavior)
			//currently supported opcodes: r', rdiag, rsort, rev
			LibMatrixReorg.reorg(this, result, op);
		}
		else 
		{
			//GENERIC case (any reorg operator)
			CellIndex temp = new CellIndex(0, 0);
			if(sparse && sparseBlock != null) {
				for(int r=0; r<Math.min(rlen, sparseBlock.numRows()); r++) {
					if(sparseBlock.isEmpty(r)) continue;
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
			else if( !sparse && denseBlock != null ) {
				if( result.isInSparseFormat() ) { //SPARSE<-DENSE
					DenseBlock a = getDenseBlock();
					for( int i=0; i<rlen; i++ ) {
						double[] avals = a.values(i);
						int aix = a.pos(i);
						for( int j=0; j<clen; j++ ) {
							temp.set(i, j);
							op.fn.execute(temp, temp);
							result.appendValue(temp.row, temp.column, avals[aix+j]);
						}
					}
				}
				else { //DENSE<-DENSE
					result.allocateDenseBlock();
					DenseBlock a = getDenseBlock();
					DenseBlock c = result.getDenseBlock();
					for( int i=0; i<rlen; i++ ) {
						double[] avals = a.values(i);
						int aix = a.pos(i);
						for( int j=0; j<clen; j++ ) {
							temp.set(i, j);
							op.fn.execute(temp, temp);
							c.set(temp.row, temp.column, avals[aix+j]);
						}
					}
					result.nonZeros = nonZeros;
				}
			}
		}
		
		return result;
	}

	public MatrixBlock append( MatrixBlock that, MatrixBlock ret ) {
		return append(that, ret, true); //default cbind
	}

	public MatrixBlock append( MatrixBlock that, MatrixBlock ret, boolean cbind ) {
		return append(new MatrixBlock[]{that}, ret, cbind);
	}
	
	public MatrixBlock append( MatrixBlock[] that, MatrixBlock ret, boolean cbind ) {
		MatrixBlock result = checkType( ret );
		final int m = cbind ? rlen : rlen+Arrays.stream(that).mapToInt(mb -> mb.rlen).sum();
		final int n = cbind ? clen+Arrays.stream(that).mapToInt(mb -> mb.clen).sum() : clen;
		final long nnz = nonZeros+Arrays.stream(that).mapToLong(mb -> mb.nonZeros).sum();
		boolean shallowCopy = (nonZeros == nnz);
		boolean sp = evalSparseFormatInMemory(m, n, nnz);
		
		//init result matrix 
		if( result == null )
			result = new MatrixBlock(m, n, sp, nnz);
		else
			result.reset(m, n, sp, nnz);
		
		//core append operation
		//copy left and right input into output
		if( !result.sparse && nnz!=0 ) //DENSE
		{
			if( cbind ) {
				DenseBlock resd = result.allocateBlock().getDenseBlock();
				MatrixBlock[] in = ArrayUtils.addAll(new MatrixBlock[]{this}, that);
				
				for( int i=0; i<m; i++ ) {
					for( int k=0, off=0; k<in.length; off+=in[k].clen, k++ ) {
						if( in[k].isEmptyBlock(false) )
							continue;
						if( in[k].sparse ) {
							SparseBlock src = in[k].sparseBlock;
							if( src.isEmpty(i) )
								continue;
							int srcpos = src.pos(i);
							int srclen = src.size(i);
							int[] srcix = src.indexes(i);
							double[] srcval = src.values(i);
							double[] resval = resd.values(i);
							int resix = resd.pos(i, off);
							for (int j=srcpos; j<srcpos+srclen; j++)
								resval[resix+srcix[j]] = srcval[j];
						}
						else {
							DenseBlock src = in[k].getDenseBlock();
							double[] srcval = src.values(i);
							double[] resval = resd.values(i);
							System.arraycopy(srcval, src.pos(i),
								resval, resd.pos(i, off), in[k].clen);
						}
					}
				}
			}
			else { //rbind
				result.copy(0, rlen-1, 0, n-1, this, false);
				for(int i=0, off=rlen; i<that.length; i++) {
					result.copy(off, off+that[i].rlen-1, 0, n-1, that[i], false);
					off += that[i].rlen;
				}
			}
		}
		else if(nnz != 0) //SPARSE
		{
			//adjust sparse rows if required
			result.allocateSparseRowsBlock();
			//allocate sparse rows once for cbind
			if( cbind && nnz > rlen && !shallowCopy && result.getSparseBlock() instanceof SparseBlockMCSR ) {
				SparseBlock sblock = result.getSparseBlock();
				for( int i=0; i<result.rlen; i++ ) {
					final int row = i; //workaround for lambda compile issue
					int lnnz = (int) (this.recomputeNonZeros(i, i, 0, this.clen-1) + Arrays.stream(that)
						.mapToLong(mb -> mb.recomputeNonZeros(row, row, 0, mb.clen-1)).sum());
					sblock.allocate(i, lnnz);
				}
			}
			
			//core append operation
			result.appendToSparse(this, 0, 0, !shallowCopy);
			if( cbind ) {
				for(int i=0, off=clen; i<that.length; i++) {
					result.appendToSparse(that[i], 0, off);
					off += that[i].clen;
				}
			}
			else { //rbind
				for(int i=0, off=rlen; i<that.length; i++) {
					result.appendToSparse(that[i], off, 0);
					off += that[i].rlen;
				}
			}
		}
		
		//update meta data
		result.nonZeros = nnz;
		return result;
	}
	
	public static MatrixBlock naryOperations(Operator op, MatrixBlock[] matrices, ScalarObject[] scalars, MatrixBlock ret) {
		//note: currently only min max, plus supported and hence specialized implementation
		//prepare operator
		FunctionObject fn = ((SimpleOperator)op).fn;
		boolean plus = fn instanceof Plus;
		Builtin bfn = !plus ? (Builtin)((SimpleOperator)op).fn : null;
		
		//process all scalars
		double init = plus ? 0 :(bfn.getBuiltinCode() == BuiltinCode.MIN) ?
			Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
		for( ScalarObject so : scalars )
			init = fn.execute(init, so.getDoubleValue());

		//compute output dimensions and estimate sparsity
		final int m = matrices.length > 0 ? matrices[0].rlen : 1;
		final int n = matrices.length > 0 ? matrices[0].clen : 1;
		final long mn = (long) m * n;
		final long nnz = (!plus && bfn.getBuiltinCode()==BuiltinCode.MIN && init < 0)
			|| (!plus && bfn.getBuiltinCode()==BuiltinCode.MAX && init > 0) ? mn :
			Math.min(Arrays.stream(matrices).mapToLong(mb -> mb.nonZeros).sum(), mn);
		boolean sp = evalSparseFormatInMemory(m, n, nnz);
		
		//init result matrix 
		if( ret == null )
			ret = new MatrixBlock(m, n, sp, nnz);
		else
			ret.reset(m, n, sp, nnz);
		
		//main processing
		if( matrices.length > 0 ) {
			ret.allocateBlock();
			int[] cnt = !plus && Arrays.stream(matrices).allMatch(
				mb -> mb.sparse || mb.isEmpty()) ? new int[n] : null;
			if( ret.isInSparseFormat() ) {
				double[] tmp = new double[n];
				for(int i = 0; i < m; i++) {
					//reset tmp and compute row output
					Arrays.fill(tmp, init);
					if( plus )
						processAddRow(matrices, tmp, 0, n, i);
					else
						processMinMaxRow(bfn, matrices, tmp, 0, n, i, cnt);
					//copy to sparse output
					for(int j = 0; j < n; j++)
						if( tmp[j] != 0 )
							ret.appendValue(i, j, tmp[j]);
				}
			}
			else {
				DenseBlock c = ret.getDenseBlock();
				long lnnz = 0;
				for(int i = 0; i < m; i++) {
					if( init != 0 )
						Arrays.fill(c.values(i), c.pos(i), c.pos(i)+n, init);
					if( plus )
						processAddRow(matrices, c.values(i), c.pos(i), n, i);
					else
						processMinMaxRow(bfn, matrices, c.values(i), c.pos(i), n, i, cnt);
					lnnz += UtilFunctions.countNonZeros(c.values(i), c.pos(i), n);
				}
				ret.setNonZeros(lnnz);
			}
		}
		else {
			ret.quickSetValue(0, 0, init);
		}
		
		return ret;
	}
	
	private static void processAddRow(MatrixBlock[] inputs, double[] c, int cix, int n, int i) {
		//sparse-safe update over all input matrices
		for( MatrixBlock in : inputs ) {
			if( in.isEmptyBlock(false) )
				continue;
			if(in instanceof CompressedMatrixBlock){
				in = CLALibBinaryCellOp.binaryMVRow((CompressedMatrixBlock) in,c, null, new BinaryOperator(Plus.getPlusFnObject()), false);
			}
			else if( in.isInSparseFormat() ) {
				SparseBlock a = in.getSparseBlock();
				if( a.isEmpty(i) ) continue;
				LibMatrixMult.vectAdd(a.values(i), c, a.indexes(i), a.pos(i), cix, a.size(i));
			}
			else {
				DenseBlock a = in.getDenseBlock();
				LibMatrixMult.vectAdd(in.getDenseBlock().values(i), c, a.pos(i), cix, n);
			}
		}
	}
	
	private static void processMinMaxRow(Builtin fn, MatrixBlock[] inputs, double[] c, int cix, int n, int i, int[] cnt) {
		if( cnt != null )
			Arrays.fill(cnt, 0);
		
		//sparse-safe update over all input matrices
		for( MatrixBlock in : inputs ) {
			if( in.isEmptyBlock(false) )
				continue;
			if( in.isInSparseFormat() ) {
				SparseBlock a = in.sparseBlock;
				if( a.isEmpty(i) ) continue;
				int alen = a.size(i);
				int apos = a.pos(i);
				int[] aix = a.indexes(i);
				double[] avals = a.values(i);
				for( int k=apos; k<apos+alen; k++ ) {
					c[aix[k]] = fn.execute(c[aix[k]], avals[k]);
					if( cnt != null ) //maintain seen values
						cnt[aix[k]]++;
				}
			}
			else {
				double[] avals = in.getDenseBlock().values(i);
				int aix = in.getDenseBlock().pos(i);
				for( int j=0; j<n; j++ )
					c[cix+j] = fn.execute(c[cix+j], avals[aix+j]);
			}
		}
		
		//corrections for all sparse inputs
		if( Arrays.stream(inputs).allMatch(m -> m.sparse || m.isEmpty()) ) {
			for( int j=0; j<n; j++ ) {
				if( cnt[j]!=inputs.length )
					c[cix+j] = fn.execute(c[cix+j], 0);
			}
		}
	}
	
	public MatrixBlock transposeSelfMatrixMultOperations( MatrixBlock out, MMTSJType tstype ) {
		return transposeSelfMatrixMultOperations(out, tstype, 1);
	}

	public MatrixBlock transposeSelfMatrixMultOperations( MatrixBlock out, MMTSJType tstype, int k ) {
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
		
		//pre=processing (outside LibMatrixMult for seamless integration
		//with native BLAS library, e.g., for sparse-dense conversion)
		MatrixBlock m1 = LibMatrixMult
			.prepMatrixMultTransposeSelfInput(this, leftTranspose, k > 1);
		
		//compute matrix mult
		if( NativeHelper.isNativeLibraryLoaded() )
			LibMatrixNative.tsmm(m1, out, leftTranspose, k);
		else if( k > 1 )
			LibMatrixMult.matrixMultTransposeSelf(m1, out, leftTranspose, k);
		else
			LibMatrixMult.matrixMultTransposeSelf(m1, out, leftTranspose);
		
		return out;
	}

	public MatrixBlock chainMatrixMultOperations( MatrixBlock v, MatrixBlock w, MatrixBlock out, ChainType ctype ) {
		return chainMatrixMultOperations(v, w, out, ctype, 1);
	}

	public MatrixBlock chainMatrixMultOperations( MatrixBlock v, MatrixBlock w, MatrixBlock out, ChainType ctype, int k ) {
		checkMMChain(ctype, v, w);

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

	protected void checkMMChain(ChainType ctype, MatrixBlock v, MatrixBlock w){
		//check for transpose type
		if( !(ctype == ChainType.XtXv || ctype == ChainType.XtwXv || ctype == ChainType.XtXvy) )
			throw new DMLRuntimeException("Invalid mmchain type '"+ctype.toString()+"'.");

		//check for matching dimensions
		if( this.getNumColumns() != v.getNumRows() )
			throw new DMLRuntimeException("Dimensions mismatch on mmchain operation ("+this.getNumColumns()+" != "+v.getNumRows()+")");
		if( v.getNumColumns() != 1 )
			throw new DMLRuntimeException("Invalid input vector (column vector expected, but ncol="+v.getNumColumns()+")");
		if( w!=null && w.getNumColumns() != 1 )
			throw new DMLRuntimeException("Invalid weight vector (column vector expected, but ncol="+w.getNumColumns()+")");
			
	}

	public void permutationMatrixMultOperations( MatrixValue m2Val, MatrixValue out1Val, MatrixValue out2Val ) {
		permutationMatrixMultOperations(m2Val, out1Val, out2Val, 1);
	}

	public void permutationMatrixMultOperations( MatrixValue m2Val, MatrixValue out1Val, MatrixValue out2Val, int k ) {
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
	
	public final MatrixBlock leftIndexingOperations(MatrixBlock rhsMatrix,
			IndexRange ixrange, MatrixBlock ret, UpdateType update) {
		return leftIndexingOperations(rhsMatrix, (int)ixrange.rowStart,
			(int)ixrange.rowEnd, (int)ixrange.colStart, (int)ixrange.colEnd, ret, update);
	}

	public MatrixBlock leftIndexingOperations(MatrixBlock rhsMatrix,
			int rl, int ru, int cl, int cu, MatrixBlock ret, UpdateType update) {
		// Check the validity of bounds
		if( rl < 0 || rl >= getNumRows() || ru < rl || ru >= getNumRows()
			|| cl < 0 || cl >= getNumColumns() || cu < cl || cu >= getNumColumns() ) {
			throw new DMLRuntimeException("Invalid values for matrix indexing: ["+(rl+1)+":"+(ru+1)+"," 
				+ (cl+1)+":"+(cu+1)+"] " + "must be within matrix dimensions ["+getNumRows()+","+getNumColumns()+"].");
		}
		if( (ru-rl+1) != rhsMatrix.getNumRows() || (cu-cl+1) != rhsMatrix.getNumColumns() ) {
			throw new DMLRuntimeException("Invalid values for matrix indexing: " +
				"dimensions of the source matrix ["+rhsMatrix.getNumRows()+"x" + rhsMatrix.getNumColumns() + "] " +
				"do not match the shape of the matrix specified by indices [" +
				(rl+1) +":" + (ru+1) + ", " + (cl+1) + ":" + (cu+1) + "] (i.e., ["+(ru-rl+1)+"x"+(cu-cl+1)+"]).");
		}
		
		MatrixBlock result = ret;
		boolean sp = estimateSparsityOnLeftIndexing(rlen, clen, nonZeros,
			rhsMatrix.getNumRows(), rhsMatrix.getNumColumns(), rhsMatrix.getNonZeros());
		
		if( !update.isInPlace() ) { //general case
			if(result==null)
				result=new MatrixBlock(rlen, clen, sp);
			else
				result.reset(rlen, clen, sp);
			result.copy(this, sp);
		}
		else { //update in-place
			//use current block as in-place result
			result = this;
			//ensure that the current block adheres to the sparsity estimate
			//and thus implicitly the memory budget used by the compiler
			if( result.sparse && !sp )
				result.sparseToDense();
			else if( !result.sparse && sp )
				result.denseToSparse();
			
			//ensure right sparse block representation to prevent serialization
			if( requiresInplaceSparseBlockOnLeftIndexing(result.sparse, update, result.nonZeros+rhsMatrix.nonZeros) )
				result.sparseBlock = SparseBlockFactory.copySparseBlock(
					DEFAULT_INPLACE_SPARSEBLOCK, result.sparseBlock, false);
		}
		
		//NOTE conceptually we could directly use a zeroout and copy(..., false) but
		//     since this was factors slower, we still use a full copy and subsequently
		//     copy(..., true) - however, this can be changed in the future once we 
		//     improved the performance of zeroout.
		
		MatrixBlock src = rhsMatrix;

		if(rl==ru && cl==cu) { //specific case: cell update
			//copy single value and update nnz
			result.quickSetValue(rl, cl, src.quickGetValue(0, 0));
		}
		else { //general case
			//handle csr sparse blocks separately to avoid repeated shifting on column-wise access
			//(note that for sparse inputs this only applies to aligned column indexes)
			if( !result.isEmptyBlock(false) && result.sparse && (!src.sparse || rl==0)
				&& result.sparseBlock instanceof SparseBlockCSR ) {
				SparseBlockCSR sblock = (SparseBlockCSR) result.sparseBlock;
				if( src.sparse || src.isEmptyBlock(false) ) {
					sblock.setIndexRange(rl, ru+1, cl, cu+1, src.getSparseBlock());
				}
				else { //dense
					for(int bi=0; bi<src.denseBlock.numBlocks(); bi++) {
						int rpos = bi * src.denseBlock.blockSize();
						int blen = src.denseBlock.blockSize(bi);
						sblock.setIndexRange(rl+rpos, rl+rpos+blen, cl, cu+1,
							src.denseBlock.valuesAt(bi), 0, src.rlen*src.clen);
					}
				}
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
	 */
	public MatrixBlock leftIndexingOperations(ScalarObject scalar, int rl, int cl, MatrixBlock ret, UpdateType update) {
		double inVal = scalar.getDoubleValue();
		boolean sp = estimateSparsityOnLeftIndexing(rlen, clen, nonZeros, 1, 1, (inVal!=0)?1:0);
		
		if( !update.isInPlace() ) { //general case
			if(ret==null)
				ret=new MatrixBlock(rlen, clen, sp);
			else
				ret.reset(rlen, clen, sp);
			ret.copy(this, sp);
		}
		else { //update in-place
			//use current block as in-place result
			ret = this;
			//ensure right sparse block representation to prevent serialization
			if( requiresInplaceSparseBlockOnLeftIndexing(ret.sparse, update, ret.nonZeros+1) )
				ret.sparseBlock = SparseBlockFactory.copySparseBlock(
					DEFAULT_INPLACE_SPARSEBLOCK, ret.sparseBlock, false);
		}
		
		ret.quickSetValue(rl, cl, inVal);
		return ret;
	}
	
	

	public MatrixBlock slice(IndexRange ixrange, MatrixBlock ret) {
		return slice(
			(int)ixrange.rowStart, (int)ixrange.rowEnd, 
			(int)ixrange.colStart, (int)ixrange.colEnd, true, ret);
	}
	
	/**
	 * Slice out a row block
	 * @param rl The row lower to start from 
	 * @param ru The row lower to end at
	 * @return The sliced out matrix block.
	 */
	public final MatrixBlock slice(int rl, int ru) {
		return slice(rl, ru, 0, clen-1, true, null);
	}

	public final MatrixBlock slice(int rl, int ru, boolean deep){
		return slice(rl,ru, 0, clen-1, deep, null);
	}
	
	public final MatrixBlock slice(int rl, int ru, int cl, int cu){
		return slice(rl, ru, cl, cu, true, null);
	}

	@Override
	public final MatrixBlock slice(int rl, int ru, int cl, int cu, CacheBlock ret) {
		return slice(rl, ru, cl, cu, true, ret);
	}

	public final MatrixBlock slice(int rl, int ru, int cl, int cu, boolean deep){
		return slice(rl, ru, cl, cu, deep, null);
	}
	
	/**
	 * Method to perform rightIndex operation for a given lower and upper bounds in row and column dimensions.
	 * Extracted submatrix is returned as "result". Note: This operation is now 0-based.
	 * 
	 * This means that if you call with rl == ru then you get 1 row output.
	 * 
	 * @param rl row lower if this value is bellow 0 or above the number of rows contained in the matrix an execption is thrown
	 * @param ru row upper if this value is bellow rl or above the number of rows contained in the matrix an exception is thrown
	 * @param cl column lower if this value us bellow 0 or above the number of columns contained in the matrix an exception is thrown
	 * @param cu column upper if this value us bellow cl or above the number of columns contained in the matrix an exception is thrown
	 * @param deep should perform deep copy, this is relelvant in cases where the matrix is in sparse format,
	 *            or the entire matrix is sliced out
	 * @param ret output sliced out matrix block
	 * @return matrix block output matrix block
	 */
	public MatrixBlock slice(int rl, int ru, int cl, int cu, boolean deep, CacheBlock ret) {
		validateSliceArgument(rl, ru, cl, cu);
		
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
			if( deep )
				result.copy( this );
			else
				result = this;
		}
		else //general case
		{
			//core slicing operation (nnz maintained internally)
			if (sparse) 
				sliceSparse(rl, ru, cl, cu, deep, result);
			else 
				sliceDense(rl, ru, cl, cu, result);
		}
		
		return result;
	}

	protected void validateSliceArgument(int rl, int ru, int cl, int cu){
		// check the validity of bounds
		if ( rl < 0 || rl >= getNumRows() || ru < rl || ru >= getNumRows()
				|| cl < 0 || cl >= getNumColumns() || cu < cl || cu >= getNumColumns() ) {
			throw new DMLRuntimeException("Invalid values for matrix indexing: ["+(rl+1)+":"+(ru+1)+"," + (cl+1)+":"+(cu+1)+"] " 
				+ "must be within matrix dimensions ["+getNumRows()+","+getNumColumns()+"]");
		}
	}

	private void sliceSparse(int rl, int ru, int cl, int cu, boolean deep, MatrixBlock dest) {
		//check for early abort
		if( isEmptyBlock(false) ) 
			return;
		
		if( cl==cu ) //COLUMN VECTOR 
		{
			//note: always dense single-block dest
			dest.allocateDenseBlock();
			double[] c = dest.getDenseBlockValues();
			for( int i=rl; i<=ru; i++ ) {
				if( !sparseBlock.isEmpty(i) ) {
					double val = sparseBlock.get(i, cl);
					if( val != 0 ) {
						c[i-rl] = val;
						dest.nonZeros++;
					}
				}
			}
		}
		else if( cl==0 && cu==clen-1 ) //ROW batch
		{
			//note: always sparse dest, but also works for dense
			boolean ldeep = (deep && sparseBlock instanceof SparseBlockMCSR);
			for(int i = rl; i <= ru; i++)
				dest.appendRow(i-rl, sparseBlock.get(i), ldeep);
		}
		else //general case (sparse/dense dest)
		{
			SparseBlock sblock = sparseBlock;
			for(int i=rl; i <= ru; i++) {
				if( sblock.isEmpty(i) ) continue;
				int apos = sblock.pos(i);
				int alen = sblock.size(i);
				int[] aix = sblock.indexes(i);
				double[] avals = sblock.values(i);
				int astart = (cl>0)?sblock.posFIndexGTE(i, cl) : 0;
				if( astart != -1 )
					for( int j=apos+astart; j<apos+alen && aix[j] <= cu; j++ )
						dest.appendValue(i-rl, aix[j]-cl, avals[j]);
			}
		}
	}

	private void sliceDense(int rl, int ru, int cl, int cu, MatrixBlock dest) {
		//ensure allocated input/output blocks
		if( denseBlock == null )
			return;
		dest.allocateDenseBlock();

		//indexing operation
		if( cl==cu ) { //COLUMN INDEXING
			//note: output always single block
			if( clen==1 ) { //vector -> vector
				System.arraycopy(getDenseBlockValues(), rl,
					dest.getDenseBlockValues(), 0, ru-rl+1);
			}
			else { //matrix -> vector
				DenseBlock a = getDenseBlock();
				double[] c = dest.getDenseBlockValues();
				for( int i=rl; i<=ru; i++ )
					c[i-rl] = a.get(i, cl);
			}
		}
		else { // GENERAL RANGE INDEXING
			DenseBlock a = getDenseBlock();
			DenseBlock c = dest.getDenseBlock();
			int len = dest.clen;
			for(int i = rl; i <= ru; i++)
				System.arraycopy(a.values(i), a.pos(i)+cl, c.values(i-rl), c.pos(i-rl), len);
		}
		
		//compute nnz of output (not maintained due to native calls)
		dest.setNonZeros((getNonZeros() == getLength()) ? 
			(ru-rl+1) * (cu-cl+1) : dest.recomputeNonZeros());
	}
	
	@Override
	public void slice(ArrayList<IndexedMatrixValue> outlist, IndexRange range,
		int rowCut, int colCut, int blen, int boundaryRlen, int boundaryClen)
	{
		MatrixBlock topleft=null, topright=null, bottomleft=null, bottomright=null;
		Iterator<IndexedMatrixValue> p = outlist.iterator();
		int blockRowFactor=blen, blockColFactor=blen;
		if(rowCut>range.rowEnd)
			blockRowFactor=boundaryRlen;
		if(colCut>range.colEnd)
			blockColFactor=boundaryClen;
		
		int minrowcut=(int)Math.min(rowCut, range.rowEnd);
		int mincolcut=(int)Math.min(colCut, range.colEnd);
		int maxrowcut=(int)Math.max(rowCut, range.rowStart);
		int maxcolcut=(int)Math.max(colCut, range.colStart);
		
		if(range.rowStart<rowCut && range.colStart<colCut)
		{
			topleft=(MatrixBlock) p.next().getValue();
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
		
		if(sparse && sparseBlock!=null){
			int r=(int)range.rowStart;
			for(; r<Math.min(Math.min(rowCut, sparseBlock.numRows()), range.rowEnd+1); r++)
				sliceHelp(r, range, colCut, topleft, topright, blen-rowCut, blen, blen);
			
			for(; r<=Math.min(range.rowEnd, sparseBlock.numRows()-1); r++)
				sliceHelp(r, range, colCut, bottomleft, bottomright, -rowCut, blen, blen);
		}
		else if(denseBlock!=null){
			double[] a = getDenseBlockValues();
			int i=((int)range.rowStart)*clen;
			int r=(int) range.rowStart;
			for(; r<Math.min(rowCut, range.rowEnd+1); r++)
			{
				int c=(int) range.colStart;
				for(; c<Math.min(colCut, range.colEnd+1); c++)
					topleft.appendValue(r+blen-rowCut, c+blen-colCut, a[i+c]);
				for(; c<=range.colEnd; c++)
					topright.appendValue(r+blen-rowCut, c-colCut, a[i+c]);
				i+=clen;
			}
			
			for(; r<=range.rowEnd; r++)
			{
				int c=(int) range.colStart;
				for(; c<Math.min(colCut, range.colEnd+1); c++)
					bottomleft.appendValue(r-rowCut, c+blen-colCut, a[i+c]);
				for(; c<=range.colEnd; c++)
					bottomright.appendValue(r-rowCut, c-colCut, a[i+c]);
				i+=clen;
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
		int pos = sparseBlock.pos(r);
		for(int i=start; i<=end; i++) {
			if(cols[pos+i]<colCut)
				left.appendValue(r+rowOffset, cols[pos+i]+normalBlockColFactor-colCut, values[pos+i]);
			else
				right.appendValue(r+rowOffset, cols[pos+i]-colCut, values[pos+i]);
		}
	}
	
	@Override
	//This the append operations for MR side
	//nextNCol is the number columns for the block right of block v2
	public void append(MatrixValue v2, ArrayList<IndexedMatrixValue> outlist, int blen, boolean cbind, boolean m2IsLast, int nextNCol)
	{
		MatrixBlock m2 = (MatrixBlock)v2;
		
		//case 1: copy lhs and rhs to output
		if( cbind && clen==blen || !cbind && rlen==blen ) {
			((MatrixBlock) outlist.get(0).getValue()).copy(this);
			((MatrixBlock) outlist.get(1).getValue()).copy(m2);
		}
		//case 2: append part of rhs to lhs, append to 2nd output if necessary
		else {
			//single output block (via plain append operation)
			if( cbind && clen + m2.clen < blen
				|| !cbind && rlen + m2.rlen < blen )
			{
				append(m2, (MatrixBlock) outlist.get(0).getValue(), cbind);
			}
			//two output blocks (via slice and append)
			else
			{
				//prepare output block 1
				MatrixBlock ret1 = (MatrixBlock) outlist.get(0).getValue();
				int lrlen1 = cbind ? rlen-1 : blen-rlen-1;
				int lclen1 = cbind ? blen-clen-1 : clen-1;
				MatrixBlock tmp1 = m2.slice(0, lrlen1, 0, lclen1, new MatrixBlock());
				append(tmp1, ret1, cbind);
	
				//prepare output block 2
				MatrixBlock ret2 = (MatrixBlock) outlist.get(1).getValue();
				if( cbind )
					m2.slice(0, rlen-1, lclen1+1, m2.clen-1, ret2);
				else
					m2.slice(lrlen1+1, m2.rlen-1, 0, clen-1, ret2);
			}
		}
	}

	@Override
	public MatrixBlock zeroOutOperations(MatrixValue result, IndexRange range, boolean complementary) {
		checkType(result);
		double currentSparsity=(double)nonZeros/rlen/clen;
		double estimatedSps=currentSparsity*(range.rowEnd-range.rowStart+1)
			*(range.colEnd-range.colStart+1)/rlen/clen;
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
				double[] a = getDenseBlockValues();
				if(complementary)//if selection
				{
					int offset=((int)range.rowStart)*clen;
					for(int r=(int) range.rowStart; r<=range.rowEnd; r++)
					{
						for(int c=(int) range.colStart; c<=range.colEnd; c++)
							((MatrixBlock) result).appendValue(r, c, a[offset+c]);
						offset+=clen;
					}
				}else
				{
					int offset=0;
					int r=0;
					for(; r<(int)range.rowStart; r++)
						for(int c=0; c<clen; c++, offset++)
							((MatrixBlock) result).appendValue(r, c, a[offset]);
					
					for(; r<=(int)range.rowEnd; r++)
					{
						for(int c=0; c<(int)range.colStart; c++)
							((MatrixBlock) result).appendValue(r, c, a[offset+c]);
						for(int c=(int)range.colEnd+1; c<clen; c++)
							((MatrixBlock) result).appendValue(r, c, a[offset+c]);
						offset+=clen;
					}
					
					for(; r<rlen; r++)
						for(int c=0; c<clen; c++, offset++)
							((MatrixBlock) result).appendValue(r, c, a[offset]);
				}
				
			}
		}
		return (MatrixBlock)result;
	}
	
	@Override
	public final MatrixBlock aggregateUnaryOperations(AggregateUnaryOperator op,
			MatrixValue result, int blen, MatrixIndexes indexesIn) {
		return aggregateUnaryOperations(op, result, blen, indexesIn, false);
	}
	
	@Override
	public MatrixBlock aggregateUnaryOperations(AggregateUnaryOperator op, MatrixValue result,
			int blen, MatrixIndexes indexesIn, boolean inCP)  {

		MatrixBlock ret = prepareAggregateUnaryOutput(op,result,blen);
		
		if( LibMatrixAgg.isSupportedUnaryAggregateOperator(op) ) {
			if( op.getNumThreads() > 1 )
				LibMatrixAgg.aggregateUnaryMatrix(this, ret, op, op.getNumThreads());
			else
				LibMatrixAgg.aggregateUnaryMatrix(this, ret, op);
			LibMatrixAgg.recomputeIndexes(ret, op, blen, indexesIn);
		}
		else if(op.sparseSafe)
			sparseAggregateUnaryHelp(op, ret, blen, indexesIn);
		else
			denseAggregateUnaryHelp(op, ret, blen, indexesIn);
		
		if(op.aggOp.existsCorrection() && inCP)
			((MatrixBlock)result).dropLastRowsOrColumns(op.aggOp.correction);
		
		return ret;
	}

	public MatrixBlock prepareAggregateUnaryOutput(AggregateUnaryOperator op, MatrixValue result, int blen){
		CellIndex tempCellIndex = new CellIndex(-1,-1);
		op.indexFn.computeDimension(rlen, clen, tempCellIndex);
		if(op.aggOp.existsCorrection())
		{
			switch(op.aggOp.correction)
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
					throw new DMLRuntimeException("unrecognized correctionLocation: "+op.aggOp.correction);
			}
		}
		
		//prepare result matrix block
		if(result==null)
			result=new MatrixBlock(tempCellIndex.row, tempCellIndex.column, false);
		else
			result.reset(tempCellIndex.row, tempCellIndex.column, false);
		return (MatrixBlock)result;
	}
	
	private void sparseAggregateUnaryHelp(AggregateUnaryOperator op, MatrixBlock result,
			int blen, MatrixIndexes indexesIn)
	{
		//initialize result
		if(op.aggOp.initialValue!=0)
			result.reset(result.rlen, result.clen, op.aggOp.initialValue);
		CellIndex tempCellIndex = new CellIndex(-1,-1);
		KahanObject buffer = new KahanObject(0,0);
		
		if( sparse && sparseBlock!=null ) {
			SparseBlock a = sparseBlock;
			for(int r=0; r<Math.min(rlen, a.numRows()); r++) {
				if(a.isEmpty(r)) continue;
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
		else if( !sparse && denseBlock!=null ) {
			DenseBlock a = getDenseBlock();
			for(int i=0; i<rlen; i++)
				for(int j=0; j<clen; j++) {
					tempCellIndex.set(i, j);
					op.indexFn.execute(tempCellIndex, tempCellIndex);
					incrementalAggregateUnaryHelp(op.aggOp, result,
						tempCellIndex.row, tempCellIndex.column, a.get(i, j), buffer);
				}
		}
	}
	
	private void denseAggregateUnaryHelp(AggregateUnaryOperator op, MatrixBlock result,
			int blen, MatrixIndexes indexesIn)
	{
		if(op.aggOp.initialValue!=0)
			result.reset(result.rlen, result.clen, op.aggOp.initialValue);
		CellIndex tempCellIndex = new CellIndex(-1,-1);
		KahanObject buffer=new KahanObject(0,0);
		for(int i=0; i<rlen; i++)
			for(int j=0; j<clen; j++) {
				tempCellIndex.set(i, j);
				op.indexFn.execute(tempCellIndex, tempCellIndex);
				incrementalAggregateUnaryHelp(op.aggOp, result, tempCellIndex.row, tempCellIndex.column, quickGetValue(i,j), buffer);
			}
	}
	
	private static void incrementalAggregateUnaryHelp(AggregateOperator aggOp, MatrixBlock result, int row, int column, 
			double newvalue, KahanObject buffer)
	{
		if(aggOp.existsCorrection())
		{
			if(aggOp.correction==CorrectionLocationType.LASTROW || aggOp.correction==CorrectionLocationType.LASTCOLUMN)
			{
				int corRow=row, corCol=column;
				if(aggOp.correction==CorrectionLocationType.LASTROW)//extra row
					corRow++;
				else if(aggOp.correction==CorrectionLocationType.LASTCOLUMN)
					corCol++;
				else
					throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correction);
				
				buffer._sum=result.quickGetValue(row, column);
				buffer._correction=result.quickGetValue(corRow, corCol);
				buffer=(KahanObject) aggOp.increOp.fn.execute(buffer, newvalue);
				result.quickSetValue(row, column, buffer._sum);
				result.quickSetValue(corRow, corCol, buffer._correction);
			}else if(aggOp.correction==CorrectionLocationType.NONE)
			{
				throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correction);
			}else// for mean
			{
				int corRow=row, corCol=column;
				int countRow=row, countCol=column;
				if(aggOp.correction==CorrectionLocationType.LASTTWOROWS)
				{
					countRow++;
					corRow+=2;
				}
				else if(aggOp.correction==CorrectionLocationType.LASTTWOCOLUMNS)
				{
					countCol++;
					corCol+=2;
				}
				else
					throw new DMLRuntimeException("unrecognized correctionLocation: "+aggOp.correction);
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
	
	public void dropLastRowsOrColumns(CorrectionLocationType correctionLocation) 
	{
		//determine number of rows/cols to be removed
		int step = correctionLocation.getNumRemovedRowsColumns();
		if( step <= 0)
			return; 
		
		//e.g., colSums, colMeans, colMaxs, colMeans, colVars
		if(   correctionLocation==CorrectionLocationType.LASTROW
		   || correctionLocation==CorrectionLocationType.LASTTWOROWS
		   || correctionLocation==CorrectionLocationType.LASTFOURROWS )
		{
			if( sparse && sparseBlock!=null ) { //SPARSE
				nonZeros -= recomputeNonZeros(1, rlen-1, 0, clen-1);
				sparseBlock = SparseBlockFactory
					.createSparseBlock(DEFAULT_SPARSEBLOCK, sparseBlock.get(0));
			}
			else if( !sparse && denseBlock!=null ) { //DENSE
				nonZeros -= recomputeNonZeros(1, rlen-1, 0, clen-1);
				DenseBlock tmp = DenseBlockFactory.createDenseBlock(1, clen);
				tmp.set(0, getDenseBlockValues());
				denseBlock = tmp;
			}
			rlen -= step;
		}
		
		//e.g., rowSums, rowsMeans, rowsMaxs, rowsMeans, rowVars
		else if(   correctionLocation==CorrectionLocationType.LASTCOLUMN
		        || correctionLocation==CorrectionLocationType.LASTTWOCOLUMNS
		        || correctionLocation==CorrectionLocationType.LASTFOURCOLUMNS )
		{
			if( sparse && sparseBlock!=null ) { //SPARSE
				//sparse blocks are converted to a dense representation
				//because column vectors are always smaller in dense
				double[] tmp = new double[rlen];
				int lnnz = 0;
				for( int i=0; i<rlen; i++ )
					lnnz += ((tmp[i] = sparseBlock.get(i, 0))!=0)? 1 : 0;
				cleanupBlock(true, true);
				sparse = false;
				denseBlock = DenseBlockFactory.createDenseBlock(tmp, rlen, 1);
				nonZeros = lnnz;
			}
			else if( !sparse && denseBlock!=null ) { //DENSE
				double[] tmp = new double[rlen];
				double[] a = getDenseBlockValues();
				int lnnz = 0;
				for( int i=0, aix=0; i<rlen; i++, aix+=clen )
					lnnz += ((tmp[i] = a[aix])!=0)? 1 : 0;
				denseBlock = DenseBlockFactory.createDenseBlock(tmp, rlen, 1);
				nonZeros = lnnz;
			}
			clen -= step;
		}
	}

	public CM_COV_Object cmOperations(CMOperator op) {
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
			double[] a = getDenseBlockValues();
			for(int i=0; i<rlen; i++)
				op.fn.execute(cmobj, a[i]);
		}

		return cmobj;
	}
		
	public CM_COV_Object cmOperations(CMOperator op, MatrixBlock weights) {
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
			double[] a = getDenseBlockValues();
			if( !weights.sparse )
			{
				double[] w = weights.getDenseBlockValues();
				if(weights.denseBlock!=null)
					for( int i=0; i<rlen; i++ )
						op.fn.execute(cmobj, a[i], w[i]);
			}
			else
			{
				for(int i=0; i<rlen; i++) 
					op.fn.execute(cmobj, a[i], weights.quickGetValue(i,0) );
			}
		}
		
		return cmobj;
	}
	
	public CM_COV_Object covOperations(COVOperator op, MatrixBlock that) {
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
			double[] a = getDenseBlockValues();
			if( !that.sparse ) {
				if(that.denseBlock!=null) {
					double[] b = that.getDenseBlockValues();
					for( int i=0; i<rlen; i++ )
						op.fn.execute(covobj, a[i], b[i]);
				}
			}
			else {
				for(int i=0; i<rlen; i++)
					op.fn.execute(covobj, a[i], that.quickGetValue(i,0));
			}
		}
		
		return covobj;
	}
	
	public CM_COV_Object covOperations(COVOperator op, MatrixBlock that, MatrixBlock weights) {
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
			double[] a = getDenseBlockValues();
			
			if( !that.sparse && !weights.sparse )
			{
				double[] w = weights.getDenseBlockValues();
				if(that.denseBlock!=null) {
					double[] b = that.getDenseBlockValues();
					for( int i=0; i<rlen; i++ )
						op.fn.execute(covobj, a[i], b[i], w[i]);
				}
			}
			else
			{
				for(int i=0; i<rlen; i++)
					op.fn.execute(covobj, a[i], that.quickGetValue(i,0), weights.quickGetValue(i,0));
			}
		}
		
		return covobj;
	}


	public final MatrixBlock sortOperations(){
		return sortOperations(null, null);
	}

	public final MatrixBlock sortOperations(MatrixValue weights){
		return sortOperations(weights, null);
	}

	public MatrixBlock sortOperations(MatrixValue weights, MatrixBlock result) {
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
		SortIndex sfn = new SortIndex(1, false, false);
		ReorgOperator rop = new ReorgOperator(sfn);
		LibMatrixReorg.reorg(tdw, result, rop);
		
		return result;
	}
	
	public double interQuartileMean() {
		//input state: rlen x 2, values and weights, sorted by weight
		//approach: determine q25 and q75 keys by cumsum of weights
		double sum_wt = sumWeightForQuantile();
		double q25d = 0.25*sum_wt;
		double q75d = 0.75*sum_wt;
		int q25i = (int) Math.ceil(q25d);
		int q75i = (int) Math.ceil(q75d);
		
		// find q25 as sum of weights (but excluding from mean)
		double psum = 0; int i = -1;
		while(psum < q25i && i < getNumRows())
			psum += quickGetValue(++i, 1);
		double q25Part = psum-q25d; 
		double q25Val = quickGetValue(i, 0);
		
		// compute mean and find q75 as sum of weights (including in mean)
		double sum = 0;
		while(psum < q75i && i < getNumRows()) {
			double v1 = quickGetValue(++i, 0);
			double v2 = quickGetValue(i, 1);
			psum += v2;
			sum += v1 * v2;
		}
		double q75Part = psum-q75d;
		double q75Val = quickGetValue(i, 0);
		
		//compute final IQM, incl. correction for q25 and q75 portions 
		return computeIQMCorrection(sum, sum_wt, q25Part, q25Val, q75Part, q75Val);
	}
	
	public static double computeIQMCorrection(double sum, double sum_wt, 
		double q25Part, double q25Val, double q75Part, double q75Val) {
		return (sum + q25Part*q25Val - q75Part*q75Val) / (sum_wt*0.5); 
	}
	
	public MatrixBlock pickValues(MatrixValue quantiles, MatrixValue ret) {
	
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
	
	public double median() {
		double sum_wt = sumWeightForQuantile();
		return pickValue(0.5, sum_wt%2==0);
	}
	
	public final double pickValue(double quantile){
		return pickValue(quantile, false);
	}
	
	public double pickValue(double quantile, boolean average) {
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
	 */
	public double sumWeightForQuantile() {
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

	public final MatrixBlock aggregateBinaryOperations(MatrixBlock m1, MatrixBlock m2, AggregateBinaryOperator op){
		return aggregateBinaryOperations(m1, m2, null, op);
	}

	public MatrixBlock aggregateBinaryOperations(MatrixBlock m1, MatrixBlock m2, MatrixBlock ret, AggregateBinaryOperator op) {
		//check input types, dimensions, configuration
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
		if( NativeHelper.isNativeLibraryLoaded() )
			LibMatrixNative.matrixMult(m1, m2, ret, op.getNumThreads());
		else if( op.getNumThreads() > 1 )
			LibMatrixMult.matrixMult(m1, m2, ret, op.getNumThreads());
		else
			LibMatrixMult.matrixMult(m1, m2, ret);
		
		return ret;
	}

	public MatrixBlock aggregateTernaryOperations(MatrixBlock m1, MatrixBlock m2, MatrixBlock m3, MatrixBlock ret,
			AggregateTernaryOperator op, boolean inCP) {
		if(m2 instanceof CompressedMatrixBlock)
			m2 = ((CompressedMatrixBlock) m2).getUncompressed("Aggregate Ternay Operator arg2 " + op.getClass().getSimpleName());
		if(m3 instanceof CompressedMatrixBlock)
			m3 = ((CompressedMatrixBlock) m3).getUncompressed("Aggregate Ternay Operator arg3 " + op.getClass().getSimpleName());

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
		
		if(op.aggOp.existsCorrection() && inCP)
			ret.dropLastRowsOrColumns(op.aggOp.correction);
		return ret;
	}

	public MatrixBlock  uaggouterchainOperations(MatrixBlock mbLeft, MatrixBlock mbRight, MatrixBlock mbOut, BinaryOperator bOp, AggregateUnaryOperator uaggOp) {
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
	 */
	public final MatrixBlock groupedAggOperations(MatrixValue tgt, MatrixValue wghts, MatrixValue ret, int ngroups, Operator op) {
		//single-threaded grouped aggregate 
		return groupedAggOperations(tgt, wghts, ret, ngroups, op, 1);
	}

	public MatrixBlock groupedAggOperations(MatrixValue tgt, MatrixValue wghts, MatrixValue ret, int ngroups, Operator op, int k) {
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
		
		// Determine the number of groups
		if( ngroups <= 0 ) { //reuse if available
			double min = this.min();
			double max = this.max();
			if ( min <= 0 )
				throw new DMLRuntimeException("Invalid value (" + min + ") encountered in 'groups' while computing groupedAggregate");
			if ( max <= 0 )
				throw new DMLRuntimeException("Invalid value (" + max + ") encountered in 'groups' while computing groupedAggregate.");
			ngroups = (int) max;
		}
	
		// Allocate result matrix
		boolean rowVector = (target.getNumRows()==1 && target.getNumColumns()>1);
		MatrixBlock result = checkType(ret);
		boolean result_sparsity = estimateSparsityOnGroupedAgg(rlen, ngroups);
		if(result==null)
			result=new MatrixBlock(ngroups, rowVector?1:target.getNumColumns(), result_sparsity);
		else
			result.reset(ngroups, rowVector?1:target.getNumColumns(), result_sparsity);

		//execute grouped aggregate operation
		if( k > 1 )
			LibMatrixAgg.groupedAggregate(this, target, weights, result, ngroups, op, k);
		else
			LibMatrixAgg.groupedAggregate(this, target, weights, result, ngroups, op);
		
		return result;
	}

	public MatrixBlock removeEmptyOperations( MatrixBlock ret, boolean rows, boolean emptyReturn, MatrixBlock select ) {
		return LibMatrixReorg.rmempty(this, ret, rows, emptyReturn, select);
	}

	public final MatrixBlock removeEmptyOperations( MatrixBlock ret, boolean rows, boolean emptyReturn) {
		return removeEmptyOperations(ret, rows, emptyReturn, null);
	}

	public MatrixBlock rexpandOperations( MatrixBlock ret, double max, boolean rows, boolean cast, boolean ignore, int k ) {
		MatrixBlock result = checkType(ret);
		return LibMatrixReorg.rexpand(this, result, max, rows, cast, ignore, k);
	}
	
	
	@Override
	public MatrixBlock replaceOperations(MatrixValue result, double pattern, double replacement) {
		MatrixBlock ret = checkType(result);
		examSparsity(); //ensure its in the right format
		ret.reset(rlen, clen, sparse);
		//probe early abort conditions
		if( nonZeros == 0 && pattern != 0  )
			return ret;
		if( !containsValue(pattern) )
			return this; //avoid allocation + copy
		
		boolean NaNpattern = Double.isNaN(pattern);
		if( sparse ) //SPARSE
		{
			if( pattern != 0d ) //SPARSE <- SPARSE (sparse-safe)
			{
				ret.allocateSparseRowsBlock();
				SparseBlock a = sparseBlock;
				SparseBlock c = ret.sparseBlock;
				
				for( int i=0; i<rlen; i++ ) {
					if( !a.isEmpty(i) ) {
						c.allocate(i);
						int apos = a.pos(i);
						int alen = a.size(i);
						int[] aix = a.indexes(i);
						double[] avals = a.values(i);
						for( int j=apos; j<apos+alen; j++ ) {
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
				double[] c = ret.getDenseBlockValues();
				
				//initialize with replacement (since all 0 values, see SPARSITY_TURN_POINT)
				Arrays.fill(c, replacement);
				
				//overwrite with existing values (via scatter)
				if( a != null  ) //check for empty matrix
					for( int i=0, cix=0; i<rlen; i++, cix+=clen ) {
						if( !a.isEmpty(i) ) {
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
		else { //DENSE <- DENSE
			int mn = ret.rlen * ret.clen;
			ret.allocateDenseBlock();
			double[] a = getDenseBlockValues();
			double[] c = ret.getDenseBlockValues();
			for( int i=0; i<mn; i++ ) {
				c[i] = ( a[i]== pattern || (NaNpattern && Double.isNaN(a[i])) ) ?
					replacement : a[i];
			}
		}
		
		ret.recomputeNonZeros();
		ret.examSparsity();
		
		return ret;
	}
	
	public MatrixBlock extractTriangular(MatrixBlock ret, boolean lower, boolean diag, boolean values) {
		ret.reset(rlen, clen, sparse);
		if( isEmptyBlock(false) )
			return ret; //sparse-safe
		ret.allocateBlock();
		
		long nnz = 0;
		if( sparse ) { //SPARSE
			SparseBlock a = sparseBlock;
			SparseBlock c = ret.sparseBlock;
			for( int i=0; i<rlen; i++ ) {
				if( a.isEmpty(i) ) continue;
				int jbeg = Math.min(lower ? 0 : (diag ? i : i+1), clen);
				int jend = Math.min(lower ? (diag ? i+1 : i) : clen, clen);
				if( values ) {
					int k1 = a.posFIndexGTE(i, jbeg);
					int k2 = a.posFIndexGTE(i, jend);
					k1 = (k1 >= 0) ? k1 : a.size(i);
					k2 = (k2 >= 0) ? k2 : a.size(i);
					int apos = a.pos(i);
					int[] aix = a.indexes(i);
					double[] avals = a.values(i);
					c.allocate(i, k2-k1);
					for( int k=apos+k1; k<apos+k2; k++ )
						ret.appendValue(i, aix[k], avals[k]);
				}
				else {
					c.allocate(i, jend-jbeg);
					for( int j=jbeg; j<jend; j++ )
						ret.appendValue(i, j, 1);
				}
			}
			//nnz maintained internally
		}
		else { //DENSE <- DENSE
			DenseBlock a = denseBlock;
			DenseBlock c = ret.getDenseBlock();
			for(int i = 0; i < rlen; i++) {
				int jbeg = Math.min(lower ? 0 : (diag ? i : i+1), clen);
				int jend = Math.min(lower ? (diag ? i+1 : i) : clen, clen);
				double[] avals = a.values(i), cvals = c.values(i);
				int aix = a.pos(i,jbeg), cix = c.pos(i,jbeg);
				if( values ) {
					System.arraycopy(avals, aix, cvals, cix, jend-jbeg);
					nnz += UtilFunctions.countNonZeros(avals, aix, jend-jbeg);
				}
				else { //R semantics full reset, not just nnz
					Arrays.fill(cvals, cix, cix+(jend-jbeg), 1);
					nnz += (jend-jbeg);
				}
			}
		}
		ret.setNonZeros(nnz);
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
	public void ctableOperations(Operator op, double scalarThat,
			MatrixValue that2Val, CTableMap resultMap, MatrixBlock resultBlock) {
		MatrixBlock that2 = checkType(that2Val);
		CTable ctable = CTable.getCTableFnObject();
		double v2 = scalarThat;
		
		//sparse-unsafe ctable execution
		//(because input values of 0 are invalid and have to result in errors) 
		for( int i=0; i<rlen; i++ )
			for( int j=0; j<clen; j++ ) {
				double v1 = this.quickGetValue(i, j);
				double w = that2.quickGetValue(i, j);
				ctable.execute(v1, v2, w, false, resultMap, resultBlock);
			}
		
		//maintain nnz (if necessary)
		if( resultBlock!=null )
			resultBlock.recomputeNonZeros();
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
	public void ctableOperations(Operator op, double scalarThat,
			double scalarThat2, CTableMap resultMap, MatrixBlock resultBlock)
	{
		CTable ctable = CTable.getCTableFnObject();
		double v2 = scalarThat;
		double w = scalarThat2;
		
		//sparse-unsafe ctable execution
		//(because input values of 0 are invalid and have to result in errors) 
		for( int i=0; i<rlen; i++ )
			for( int j=0; j<clen; j++ ) {
				double v1 = this.quickGetValue(i, j);
				ctable.execute(v1, v2, w, false, resultMap, resultBlock);
			}
		
		//maintain nnz (if necessary)
		if( resultBlock!=null )
			resultBlock.recomputeNonZeros();
	}
	
	/**
	 * Specific ctable case of ctable(seq(...),X), where X is the only
	 * matrix input. The 'left' input parameter specifies if the seq appeared
	 * on the left, otherwise it appeared on the right.
	 * 
	 */
	@Override
	public void ctableOperations(Operator op, MatrixIndexes ix1, double scalarThat,
			boolean left, int blen, CTableMap resultMap, MatrixBlock resultBlock)
	{	
		CTable ctable = CTable.getCTableFnObject();
		double w = scalarThat;
		int offset = (int) ((ix1.getRowIndex()-1)*blen); 
		
		//sparse-unsafe ctable execution
		//(because input values of 0 are invalid and have to result in errors) 
		for( int i=0; i<rlen; i++ )
			for( int j=0; j<clen; j++ ) {
				double v1 = this.quickGetValue(i, j);
				if( left )
					ctable.execute(offset+i+1, v1, w, false, resultMap, resultBlock);
				else
					ctable.execute(v1, offset+i+1, w, false, resultMap, resultBlock);
			}
		
		//maintain nnz (if necessary)
		if( resultBlock!=null )
			resultBlock.recomputeNonZeros();
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
	public void ctableOperations(Operator op, MatrixValue thatVal, double scalarThat2, boolean ignoreZeros,
			     CTableMap resultMap, MatrixBlock resultBlock)
	{
		//setup ctable computation
		MatrixBlock that = checkType(thatVal);
		if(that instanceof CompressedMatrixBlock)
			that = ((CompressedMatrixBlock) that).getUncompressed("CTable " + op);

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
			for( int i=0; i<rlen; i++ ) {
				if( a.isEmpty(i) ) continue; 
				int alen = a.size(i);
				int apos = a.pos(i);
				double[] avals = a.values(i);
				int bpos = b.pos(i);
				double[] bvals = b.values(i); 
				for( int j=0; j<alen; j++ )
					ctable.execute(avals[apos+j], bvals[bpos+j], 
						w, ignoreZeros, resultMap, resultBlock);
			}
		}
		else //SPARSE-UNSAFE | GENERIC INPUTS
		{
			//sparse-unsafe ctable execution
			//(because input values of 0 are invalid and have to result in errors) 
			for( int i=0; i<rlen; i++ )
				for( int j=0; j<clen; j++ ) {
					double v1 = this.quickGetValue(i, j);
					double v2 = that.quickGetValue(i, j);
					ctable.execute(v1, v2, w, ignoreZeros, resultMap, resultBlock);
				}
		}
		
		//maintain nnz (if necessary)
		if( resultBlock!=null )
			resultBlock.recomputeNonZeros();
	}

	/**
	 * @param thatMatrix matrix value
	 * @param thatScalar scalar double
	 * @param resultBlock result matrix block
	 * @param updateClen when this matrix already has the desired number of columns updateClen can be set to false
	 * @return resultBlock
	 */
	public MatrixBlock ctableSeqOperations(MatrixValue thatMatrix, double thatScalar, MatrixBlock resultBlock, boolean updateClen) {
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
		if(updateClen) {
			resultBlock.clen = maxCol;
		}
		return resultBlock;
	}

	/**
	 *  D = ctable(seq,A,w)
	 *  this &lt;- seq; thatMatrix &lt;- A; thatScalar &lt;- w; result &lt;- D
	 *
	 * (i1,j1,v1) from input1 (this)
	 * (i1,j1,v2) from input2 (that)
	 * (w)  from scalar_input3 (scalarThat2)
	 *
	 * @param thatMatrix matrix value
	 * @param thatScalar scalar double
	 * @param resultBlock result matrix block
	 * @return resultBlock
	 */
	public final MatrixBlock ctableSeqOperations(MatrixValue thatMatrix, double thatScalar, MatrixBlock resultBlock) {
		return ctableSeqOperations(thatMatrix, thatScalar, resultBlock, true);
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
	 */
	public final void ctableOperations(Operator op, MatrixValue thatVal, MatrixValue that2Val, CTableMap resultMap) {
		ctableOperations(op, thatVal, that2Val, resultMap, null);
	}
	
	@Override
	public void ctableOperations(Operator op, MatrixValue thatVal, MatrixValue that2Val, CTableMap resultMap, MatrixBlock resultBlock) {
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
	
	public final MatrixBlock quaternaryOperations(QuaternaryOperator qop, MatrixBlock um, MatrixBlock vm, MatrixBlock wm, MatrixBlock out) {
		return quaternaryOperations(qop, um, vm, wm, out, 1);
	}

	public MatrixBlock quaternaryOperations(QuaternaryOperator qop, MatrixBlock U, MatrixBlock V, MatrixBlock wm, MatrixBlock out, int k) {
		//check input dimensions
		if( getNumRows() != U.getNumRows() )
			throw new DMLRuntimeException("Dimension mismatch rows on quaternary operation: "+getNumRows()+"!="+U.getNumRows());
		if( getNumColumns() != V.getNumRows() )
			throw new DMLRuntimeException("Dimension mismatch columns quaternary operation: "+getNumColumns()+"!="+V.getNumRows());
		
		//check input data types
		MatrixBlock X = this;
		MatrixBlock R = checkType(out);
		
		//prepare intermediates and output
		if( qop.wtype1 != null || qop.wtype4 != null )
			R.reset(1, 1, false);
		else if( qop.wtype2 != null || qop.wtype5 != null )
			R.reset(rlen, clen, sparse);
		else if( qop.wtype3 != null ) {
			DataCharacteristics mc = qop.wtype3.computeOutputCharacteristics(X.rlen, X.clen, U.clen);
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
			if( qop.getScalar() != 0 )
				W = new MatrixBlock(qop.getScalar());
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
	 */
	public static MatrixBlock randOperations(int rows, int cols, double sparsity, double min, double max, String pdf, long seed) {
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
	 */
	public static MatrixBlock randOperations(int rows, int cols, double sparsity, double min, double max, String pdf, long seed, int k) {
		RandomMatrixGenerator rgen = new RandomMatrixGenerator(pdf, rows, cols, 
				ConfigurationManager.getBlocksize(), sparsity, min, max);
		
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
	 */
	public static MatrixBlock randOperations(RandomMatrixGenerator rgen, long seed) {
		return randOperations(rgen, seed, 1);
	}
	
	/**
	 * Function to generate the random matrix with specified dimensions and block dimensions.
	 * 
	 * @param rgen random matrix generator
	 * @param seed seed value
	 * @param k ?
	 * @return matrix block
	 */
	public static MatrixBlock randOperations(RandomMatrixGenerator rgen, long seed, int k) {
		MatrixBlock out = new MatrixBlock();
		Well1024a bigrand = null;

		//setup seeds and nnz per block
		if( !LibMatrixDatagen.isShortcutRandOperation(rgen._min, rgen._max, rgen._sparsity, rgen._pdf) )
			bigrand = LibMatrixDatagen.setupSeedsForRand(seed);
		
		//generate rand data
		if (k > 1)
			out.randOperationsInPlace(rgen, bigrand, -1, k);
		else
			out.randOperationsInPlace(rgen, bigrand, -1);
		
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
	 * @param bigrand ?
	 * @param bSeed seed value
	 * @return matrix block
	 */
	public MatrixBlock randOperationsInPlace(RandomMatrixGenerator rgen, Well1024a bigrand, long bSeed ) {
		LibMatrixDatagen.generateRandomMatrix(this, rgen, bigrand, bSeed);
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
	 * @param bigrand ?
	 * @param bSeed seed value
	 * @param k ?
	 * @return matrix block
	 */
	public MatrixBlock randOperationsInPlace(RandomMatrixGenerator rgen, 
			Well1024a bigrand, long bSeed, int k) {
		LibMatrixDatagen.generateRandomMatrix(this, rgen, bigrand, bSeed, k);
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
	 */
	public static MatrixBlock seqOperations(double from, double to, double incr) {
		MatrixBlock out = new MatrixBlock();
		LibMatrixDatagen.generateSequence( out, from, to, incr );
		return out;
	}

	public MatrixBlock seqOperationsInPlace(double from, double to, double incr) {
		LibMatrixDatagen.generateSequence( this, from, to, incr );
		return this;
	}

	public static MatrixBlock sampleOperations(long range, int size, boolean replace, long seed) {
		MatrixBlock out = new MatrixBlock();
		LibMatrixDatagen.generateSample( out, range, size, replace, seed );
		return out;
	}
	
	////////
	// Misc methods
	
	private static MatrixBlock checkType(MatrixValue block) {
		if( block!=null && !(block instanceof MatrixBlock))
			throw new RuntimeException("Unsupported matrix value: "
				+ block.getClass().getSimpleName());
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
	
	/**
	 * Checks for existing NaN values in the matrix block.
	 * @throws DMLRuntimeException if the blocks contains at least one NaN.
	 */
	public void checkNaN() {
		if( isEmptyBlock(false) )
			return;
		if( sparse ) {
			SparseBlock sblock = sparseBlock;
			for(int i=0; i<rlen; i++) {
				if( sblock.isEmpty(i) ) continue;
				int alen = sblock.size(i);
				int apos = sblock.pos(i);
				int[] aix = sblock.indexes(i);
				double[] avals = sblock.values(i);
				for(int k=apos; k<apos+alen; k++) {
					if( Double.isNaN(avals[k]) )
						throw new DMLRuntimeException("NaN encountered at position ["+i+","+aix[k]+"].");
				}
			}
		}
		else {
			DenseBlock dblock = denseBlock;
			for(int i=0; i<rlen; i++) {
				int aix = dblock.pos(i);
				double[] avals = dblock.values(i);
				for(int j=0; j<clen; j++)
					if( Double.isNaN(avals[aix+j]) )
						throw new DMLRuntimeException("NaN encountered at position ["+i+","+j+"].");
			}
		}
	}
	
	@Override
	public final int compareTo(Object arg0) {
		throw new RuntimeException("CompareTo should never be called for matrix blocks.");
	}

	@Override
	public final boolean equals(Object arg0) {
		throw new RuntimeException("equals should never be called for matrix blocks.");
	}
	
	@Override
	public final int hashCode() {
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
		
		if( sparse && sparseBlock != null ) {
			//overloaded implementation in sparse blocks
			sb.append(sparseBlock.toString());
		}
		else if( !sparse && denseBlock!=null ) {
			//overloaded implementation in dense blocks
			sb.append(denseBlock.toString());
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
