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

package org.apache.sysds.runtime.compress.colgroup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.runtime.compress.DMLCompressionException;
import org.apache.sysds.runtime.compress.colgroup.dictionary.ADictionary;
import org.apache.sysds.runtime.compress.colgroup.dictionary.Dictionary;
import org.apache.sysds.runtime.compress.colgroup.dictionary.DictionaryFactory;
import org.apache.sysds.runtime.compress.colgroup.dictionary.MatrixBlockDictionary;
import org.apache.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.data.DenseBlockFP64;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.functionobjects.Builtin;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.matrix.data.LibMatrixMult;
import org.apache.sysds.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.ScalarOperator;

/**
 * Base class for column groups encoded with value dictionary. This include column groups such as DDC OLE and RLE.
 * 
 */
public abstract class ColGroupValue extends ColGroupCompressed implements Cloneable {
	private static final long serialVersionUID = -6835757655517301955L;

	/** Thread-local pairs of reusable temporary vectors for positions and values */
	private static ThreadLocal<Pair<int[], double[]>> memPool = new ThreadLocal<Pair<int[], double[]>>() {
		@Override
		protected Pair<int[], double[]> initialValue() {
			return null;
		}
	};

	private static ThreadLocal<double[]> tmpLeftMultDoubleArray = new ThreadLocal<double[]>() {
		@Override
		protected double[] initialValue() {
			return null;
		}
	};

	/**
	 * ColGroup Implementation Contains zero tuple. Note this is not if it contains a zero value. If false then the
	 * stored values are filling the ColGroup making it a dense representation, that can be leveraged in operations.
	 */
	protected boolean _zeros = false;

	/** Distinct value tuples associated with individual bitmaps. */
	protected ADictionary _dict;

	/** The count of each distinct value contained in the dictionary */
	private SoftReference<int[]> counts;

	protected ColGroupValue(int numRows) {
		super(numRows);
	}

	protected ColGroupValue(int[] colIndices, int numRows, ADictionary dict) {
		super(colIndices, numRows);
		_dict = dict;
	}

	protected ColGroupValue(int[] colIndices, int numRows, ADictionary dict, int[] cachedCounts) {
		super(colIndices, numRows);
		_dict = dict;
		counts = new SoftReference<>(cachedCounts);
	}

	@Override
	public final void decompressToBlockSafe(MatrixBlock target, int rl, int ru, int offT) {
		decompressToBlockUnSafe(target, rl, ru, offT);
		target.setNonZeros(getNumberNonZeros() + target.getNonZeros());
	}

	@Override
	public final void decompressToBlockUnSafe(MatrixBlock target, int rl, int ru, int offT) {
		if(_dict instanceof MatrixBlockDictionary) {
			final MatrixBlockDictionary md = (MatrixBlockDictionary) _dict;
			final MatrixBlock mb = md.getMatrixBlock();
			if(mb.isEmpty())
				return;
			else if(mb.isInSparseFormat())
				decompressToBlockUnSafeSparseDictionary(target, rl, ru, offT, mb.getSparseBlock());
			else
				decompressToBlockUnSafeDenseDictionary(target, rl, ru, offT, mb.getDenseBlockValues());
		}
		else
			decompressToBlockUnSafeDenseDictionary(target, rl, ru, offT, _dict.getValues());
	}

	/**
	 * Decompress to block using a sparse dictionary to lookup into.
	 * 
	 * @param target The dense target block to decompress into
	 * @param rl     The row to start decompression from
	 * @param ru     The row to end decompression at
	 * @param offT   The offset into target block to decompress to (use full if the target it a multi block matrix)
	 * @param sb     the sparse dictionary block to take value tuples from
	 */
	protected abstract void decompressToBlockUnSafeSparseDictionary(MatrixBlock target, int rl, int ru, int offT,
		SparseBlock sb);

	/**
	 * Decompress to block using a dense dictionary to lookup into.
	 * 
	 * @param target The dense target block to decompress into
	 * @param rl     The row to start decompression from
	 * @param ru     The row to end decompression at
	 * @param offT   The offset into target block to decompress to (use full if the target it a multi block matrix)
	 * @param values The dense dictionary values, linearized row major.
	 */
	protected abstract void decompressToBlockUnSafeDenseDictionary(MatrixBlock target, int rl, int ru, int offT,
		double[] values);

	@Override
	public final int getNumValues() {
		return _dict.getNumberOfValues(_colIndexes.length);
	}

	@Override
	public final double[] getValues() {
		return _dict != null ? _dict.getValues() : null;
	}

	public final ADictionary getDictionary() {
		return _dict;
	}

	@Override
	public final void addMinMax(double[] ret) {
		_dict.addMaxAndMin(ret, _colIndexes);
	}

	@Override
	public final MatrixBlock getValuesAsBlock() {
		_dict = _dict.getAsMatrixBlockDictionary(_colIndexes.length);
		MatrixBlock ret = ((MatrixBlockDictionary) _dict).getMatrixBlock();
		if(_zeros) {
			MatrixBlock tmp = new MatrixBlock();
			ret.append(new MatrixBlock(1, _colIndexes.length, 0), tmp, false);
			return tmp;
		}
		return ret;
	}

	/**
	 * Returns the counts of values inside the dictionary. If already calculated it will return the previous counts.
	 * This produce an overhead in cases where the count is calculated, but the overhead will be limited to number of
	 * distinct tuples in the dictionary.
	 * 
	 * The returned counts always contains the number of zeros as well if there are some contained, even if they are not
	 * materialized.
	 *
	 * @return the count of each value in the MatrixBlock.
	 */
	public final int[] getCounts() {
		int[] countsActual = null;
		if(_dict != null) {
			if(counts == null || counts.get() == null) {
				countsActual = getCounts(new int[getNumValues() + (_zeros ? 1 : 0)]);
				counts = new SoftReference<>(countsActual);
			}
			else
				countsActual = counts.get();

		}

		return countsActual;

	}

	/**
	 * Set the counts, this is used while compressing since the counts are cleanly available there, and therefore a
	 * iteration though the data, is not needed to construct the counts.
	 * 
	 * NOTE THIS IS UNSAFE since it does not verify that the counts given are correct.
	 * 
	 * @param counts The counts to set.
	 */
	protected final void setCounts(int[] counts) {
		this.counts = new SoftReference<>(counts);
	}

	/**
	 * Get the cached counts. If they are not materialized or the garbage collector have removed them, then null is
	 * returned
	 * 
	 * @return the counts or null.
	 */
	public final int[] getCachedCounts() {
		return counts != null ? counts.get() : null;
	}

	/**
	 * Returns the counts of values inside the MatrixBlock returned in getValuesAsBlock Throws an exception if the
	 * getIfCountsType is false.
	 * 
	 * The returned counts always contains the number of zeros as well if there are some contained, even if they are not
	 * materialized.
	 *
	 * @param rl the lower index of the interval of rows queried
	 * @param ru the the upper boundary of the interval of rows queried
	 * @return the count of each value in the MatrixBlock.
	 */
	public final int[] getCounts(int rl, int ru) {
		int[] tmp;
		if(_zeros) {
			tmp = allocIVector(getNumValues() + 1, true);
		}
		else {
			tmp = allocIVector(getNumValues(), true);
		}
		return getCounts(rl, ru, tmp);
	}

	public boolean getIfCountsType() {
		return true;
	}

	protected final double sumValues(int valIx, double[] b, double[] dictVals) {
		final int numCols = getNumCols();
		final int valOff = valIx * numCols;
		double val = 0;
		for(int i = 0; i < numCols; i++)
			val += dictVals[valOff + i] * b[_colIndexes[i]];
		return val;
	}

	protected final double sumValues(int valIx, double[] b, double[] dictVals, int off) {
		final int numCols = getNumCols();
		final int valOff = valIx * numCols;
		double val = 0;
		for(int i = 0; i < numCols; i++)
			val += dictVals[valOff + i] * b[_colIndexes[i] + off];
		return val;
	}

	private int[] getAggregateColumnsSetDense(double[] b, int cl, int cu, int cut) {
		Set<Integer> aggregateColumnsSet = new HashSet<>();
		final int retCols = (cu - cl);
		for(int k = 0; k < _colIndexes.length; k++) {
			int rowIdxOffset = _colIndexes[k] * cut;
			for(int h = cl; h < cu; h++) {
				double v = b[rowIdxOffset + h];
				if(v != 0.0) {
					aggregateColumnsSet.add(h);
				}
			}
			if(aggregateColumnsSet.size() == retCols)
				break;
		}

		int[] aggregateColumns = aggregateColumnsSet.stream().mapToInt(x -> x).toArray();
		Arrays.sort(aggregateColumns);
		return aggregateColumns;
	}

	private int[] getAggregateColumnsSetSparse(SparseBlock b, int retCols) {
		Set<Integer> aggregateColumnsSet = new HashSet<>();

		for(int h = 0; h < _colIndexes.length; h++) {
			int colIdx = _colIndexes[h];
			if(!b.isEmpty(colIdx)) {
				int[] sIndexes = b.indexes(colIdx);
				for(int i = b.pos(colIdx); i < b.size(colIdx) + b.pos(colIdx); i++) {
					aggregateColumnsSet.add(sIndexes[i]);
				}
			}
			if(aggregateColumnsSet.size() == retCols)
				break;
		}

		int[] aggregateColumns = aggregateColumnsSet.stream().mapToInt(x -> x).toArray();
		Arrays.sort(aggregateColumns);
		return aggregateColumns;
	}

	private double[] preaggValuesFromSparse(int numVals, SparseBlock b, int[] aggregateColumns, int cl, int cu,
		int cut) {
		final double[] ret = new double[numVals * aggregateColumns.length];
		for(int h = 0; h < _colIndexes.length; h++) {
			int colIdx = _colIndexes[h];
			if(!b.isEmpty(colIdx)) {
				double[] sValues = b.values(colIdx);
				int[] sIndexes = b.indexes(colIdx);
				int retIdx = 0;
				for(int i = b.pos(colIdx); i < b.size(colIdx) + b.pos(colIdx); i++) {
					while(aggregateColumns[retIdx] < sIndexes[i])
						retIdx++;
					if(sIndexes[i] == aggregateColumns[retIdx])
						for(int j = 0, offOrg = h;
							j < numVals * aggregateColumns.length;
							j += aggregateColumns.length, offOrg += _colIndexes.length) {
							ret[j + retIdx] += _dict.getValue(offOrg) * sValues[i];
						}
				}
			}
		}
		return ret;
	}

	protected final double computeMxx(double c, Builtin builtin) {
		if(_zeros)
			c = builtin.execute(c, 0);
		if(_dict != null)
			return _dict.aggregate(c, builtin);
		else
			return c;
	}

	protected final void computeColMxx(double[] c, Builtin builtin) {
		if(_zeros) {
			for(int x = 0; x < _colIndexes.length; x++)
				c[_colIndexes[x]] = builtin.execute(c[_colIndexes[x]], 0);
		}
		if(_dict != null)
			_dict.aggregateCols(c, builtin, _colIndexes);
	}

	/**
	 * Method for use by subclasses. Applies a scalar operation to the value metadata stored in the dictionary.
	 * 
	 * @param op scalar operation to perform
	 * @return transformed copy of value metadata for this column group
	 */
	protected final ADictionary applyScalarOp(ScalarOperator op) {
		return _dict.clone().apply(op);
	}

	/**
	 * Method for use by subclasses. Applies a scalar operation to the value metadata stored in the dictionary. This
	 * specific method is used in cases where an new entry is to be added in the dictionary.
	 * 
	 * Method should only be called if the newVal is not 0! Also the newVal should already have the operator applied.
	 * 
	 * @param op      The Operator to apply to the underlying data.
	 * @param newVal  The new Value to append to the underlying data.
	 * @param numCols The number of columns in the ColGroup, to specify how many copies of the newVal should be
	 *                appended.
	 * @return The new Dictionary containing the values.
	 */
	protected final ADictionary applyScalarOp(ScalarOperator op, double newVal, int numCols) {
		return _dict.applyScalarOp(op, newVal, numCols);
	}

	/**
	 * Apply the binary row-wise operator to the dictionary, and copy it appropriately if needed.
	 * 
	 * @param fn         The function to apply.
	 * @param v          The vector to apply on each tuple of the dictionary.
	 * @param sparseSafe Specify if the operation is sparseSafe. if false then allocate a new tuple.
	 * @param left       Specify which side the operation is executed on.
	 * @return The new Dictionary with values.
	 */
	protected final ADictionary applyBinaryRowOp(BinaryOperator op, double[] v, boolean sparseSafe, boolean left) {
		return sparseSafe ? _dict.clone().applyBinaryRowOp(op, v, sparseSafe, _colIndexes, left) : _dict
			.applyBinaryRowOp(op, v, sparseSafe, _colIndexes, left);
	}

	public static void setupThreadLocalMemory(int len) {
		if(memPool.get() == null || memPool.get().getLeft().length < len) {
			Pair<int[], double[]> p = new ImmutablePair<>(new int[len], new double[len]);
			memPool.set(p);
		}
	}

	public static void setupLeftMultThreadLocalMemory(int len) {
		if(tmpLeftMultDoubleArray.get() == null || tmpLeftMultDoubleArray.get().length < len)
			tmpLeftMultDoubleArray.set(new double[len]);
	}

	public static void cleanupThreadLocalMemory() {
		memPool.remove();
	}

	protected static double[] allocDVector(int len, boolean reset) {
		Pair<int[], double[]> p = memPool.get();
		// sanity check for missing setup
		if(p == null) {
			return new double[len];
		}

		if(p.getValue().length < len) {
			setupThreadLocalMemory(len);
			return p.getValue();
		}

		// get and reset if necessary
		double[] tmp = p.getValue();
		if(reset)
			Arrays.fill(tmp, 0, len, 0);
		return tmp;
	}

	protected static int[] allocIVector(int len, boolean reset) {
		Pair<int[], double[]> p = memPool.get();

		// sanity check for missing setup
		if(p == null)
			return new int[len + 1];

		if(p.getKey().length < len) {
			setupThreadLocalMemory(len);
			return p.getKey();
		}

		int[] tmp = p.getKey();
		if(reset)
			Arrays.fill(tmp, 0, len, 0);
		return tmp;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(" Is Lossy: " + _dict.isLossy() + " num Rows: " + getNumRows() + " contain zero row:" + _zeros);
		sb.append(super.toString());
		if(_dict != null) {
			sb.append(String.format("\n%15s ", "Values: " + _dict.getClass().getSimpleName()));
			sb.append(_dict.getString(_colIndexes.length));
		}
		return sb.toString();
	}

	@Override
	public final boolean isLossy() {
		return _dict.isLossy();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		_zeros = in.readBoolean();
		_dict = DictionaryFactory.read(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeBoolean(_zeros);
		_dict.write(out);
	}

	@Override
	public long getExactSizeOnDisk() {
		long ret = super.getExactSizeOnDisk();
		ret += 1; // zeros boolean
		ret += 1; // lossy boolean
		// distinct values (groups of values)
		ret += 1; // Dict exists boolean
		if(_dict != null)
			ret += _dict.getExactSizeOnDisk();

		return ret;
	}

	public abstract int[] getCounts(int[] out);

	public abstract int[] getCounts(int rl, int ru, int[] out);

	protected final void computeSum(double[] c, boolean square) {
		if(_dict != null)
			if(square)
				c[0] += _dict.sumsq(getCounts(), _colIndexes.length);
			else
				c[0] += _dict.sum(getCounts(), _colIndexes.length);
	}

	protected final void computeColSums(double[] c, boolean square) {
		_dict.colSum(c, getCounts(), _colIndexes, square);
	}

	protected Object clone() {
		try {
			return super.clone();
		}
		catch(CloneNotSupportedException e) {
			throw new DMLCompressionException("Error while cloning: " + getClass().getSimpleName(), e);
		}
	}

	public AColGroup copyAndSet(double[] newDictionary) {
		return copyAndSet(new Dictionary(newDictionary));
	}

	public AColGroup copyAndSet(ADictionary newDictionary) {
		ColGroupValue clone = (ColGroupValue) this.clone();
		clone._dict = newDictionary;
		return clone;
	}

	public AColGroup copyAndSet(int[] colIndexes, double[] newDictionary) {
		return copyAndSet(colIndexes, new Dictionary(newDictionary));
	}

	public AColGroup copyAndSet(int[] colIndexes, ADictionary newDictionary) {
		ColGroupValue clone = (ColGroupValue) this.clone();
		clone._dict = newDictionary;
		clone.setColIndices(colIndexes);
		return clone;
	}

	@Override
	public ColGroupValue copy() {
		return (ColGroupValue) this.clone();
	}

	@Override
	protected final AColGroup sliceSingleColumn(int idx) {
		ColGroupValue ret = (ColGroupValue) copy();
		ret._colIndexes = new int[] {0};
		if(ret._dict != null)
			if(_colIndexes.length == 1)
				ret._dict = ret._dict.clone();
			else
				ret._dict = ret._dict.sliceOutColumnRange(idx, idx + 1, _colIndexes.length);

		return ret;
	}

	@Override
	protected final AColGroup sliceMultiColumns(int idStart, int idEnd, int[] outputCols) {

		ColGroupValue ret = (ColGroupValue) copy();
		ret._dict = ret._dict != null ? ret._dict.sliceOutColumnRange(idStart, idEnd, _colIndexes.length) : null;
		ret._colIndexes = outputCols;

		return ret;
	}

	public static final MatrixBlock allocatePreAggregate(MatrixBlock m, int numVals, int rl, int ru) {
		final int lhsRows = ru - rl;
		final double[] vals = allocDVector(lhsRows * numVals, true);
		final DenseBlock retB = new DenseBlockFP64(new int[] {lhsRows, numVals}, vals);
		MatrixBlock preAgg = new MatrixBlock(lhsRows, numVals, retB);
		return preAgg;
	}

	/**
	 * Pre aggregate for left Multiplication.
	 * 
	 * @param m      Matrix to preAggregate
	 * @param preAgg Matrix to preAggregate into
	 * @param rl     Start row
	 * @param ru     End row
	 */
	public abstract void preAggregate(MatrixBlock m, MatrixBlock preAgg, int rl, int ru);

	public abstract void preAggregateDense(MatrixBlock m, MatrixBlock preAgg, int rl, int ru, int vl, int vu);

	/**
	 * Pre aggregate into a dictionary. It is assumed that "that" have more distinct values than, "this".
	 * 
	 * @param that      the other column group whose indexes are used for aggregation.
	 * @param preModify specifies if the matrix in this
	 * @return A aggregate dictionary
	 */
	public final Dictionary preAggregateThatIndexStructure(ColGroupValue that, boolean preModify) {
		int outputLength = that._colIndexes.length * this.getNumValues();
		Dictionary ret = new Dictionary(new double[outputLength]);

		if(that instanceof ColGroupDDC)
			return preAggregateThatDDCStructure((ColGroupDDC) that, ret);
		else if(that instanceof ColGroupSDC)
			return preAggregateThatSDCStructure((ColGroupSDC) that, ret, preModify);
		else if(that instanceof ColGroupSDCSingle)
			return preAggregateThatSDCSingleStructure((ColGroupSDCSingle) that, ret, preModify);
		else if(that instanceof ColGroupSDCSingleZeros)
			return preAggregateThatSDCSingleZerosStructure((ColGroupSDCSingleZeros) that, ret);
		else if(that instanceof ColGroupSDCZeros)
			return preAggregateThatSDCZerosStructure((ColGroupSDCZeros) that, ret);
		else if(that instanceof ColGroupConst)
			return preAggregateThatConstStructure((ColGroupConst) that, ret);

		throw new NotImplementedException("Not supported pre aggregate using index structure of :"
			+ that.getClass().getSimpleName() + " in " + this.getClass().getSimpleName());
	}

	protected int getIndexStructureHash() {
		throw new NotImplementedException("This base function should not be called");
	}

	protected Dictionary preAggregateThatDDCStructure(ColGroupDDC that, Dictionary ret) {
		throw new DMLCompressionException("Does not make sense to call this, implement function for sub class");
	}

	protected Dictionary preAggregateThatSDCStructure(ColGroupSDC that, Dictionary ret, boolean preModified) {
		throw new DMLCompressionException("Does not make sense to call this, implement function for sub class");
	}

	protected Dictionary preAggregateThatSDCZerosStructure(ColGroupSDCZeros that, Dictionary ret) {
		throw new DMLCompressionException("Does not make sense to call this, implement function for sub class");
	}

	protected Dictionary preAggregateThatSDCSingleZerosStructure(ColGroupSDCSingleZeros that, Dictionary ret) {
		throw new DMLCompressionException("Does not make sense to call this, implement function for sub class");
	}

	protected Dictionary preAggregateThatSDCSingleStructure(ColGroupSDCSingle that, Dictionary ret,
		boolean preModified) {
		throw new DMLCompressionException("Does not make sense to call this, implement function for sub class");
	}

	protected Dictionary preAggregateThatConstStructure(ColGroupConst that, Dictionary ret) {
		computeColSums(ret.getValues(), false);
		return ret;
	}

	@Override
	public final void leftMultByAColGroup(AColGroup lhs, MatrixBlock result) {
		if(lhs instanceof ColGroupEmpty)
			return;
		else if(lhs instanceof ColGroupValue)
			leftMultByColGroupValue((ColGroupValue) lhs, result);
		else if(lhs instanceof ColGroupUncompressed)
			leftMultByUncompressedColGroup((ColGroupUncompressed) lhs, result);
		else
			throw new DMLCompressionException(
				"Not supported left multiplication with A ColGroup of type: " + lhs.getClass().getSimpleName());
	}

	private void leftMultByColGroupValue(ColGroupValue lhs, MatrixBlock result) {
		final int nvL = lhs.getNumValues();
		final int nvR = this.getNumValues();
		// final double[] lhValues = lhs.getValues();
		// final double[] rhValues = this.getValues();
		final int lCol = lhs._colIndexes.length;
		final int rCol = this._colIndexes.length;
		final double[] resV = result.getDenseBlockValues();
		final int numCols = result.getNumColumns();

		final double CommonElementThreshold = 0.4;

		if(sameIndexStructure(lhs)) {
			if(this._dict == lhs._dict) {
				tsmmDictionaryWithScaling(_dict, getCounts(), lhs._colIndexes, this._colIndexes, resV, numCols);
			}
			else
				matrixMultDictionariesAndOutputToColIndexesWithScaling(lhs._dict, this._dict, lhs._colIndexes,
					this._colIndexes, result, getCounts());
		}
		else if(lhs instanceof ColGroupConst || this instanceof ColGroupConst) {
			ADictionary r = this instanceof ColGroupConst ? this._dict : new Dictionary(
				this._dict.colSum(getCounts(), rCol));
			ADictionary l = lhs instanceof ColGroupConst ? lhs._dict : new Dictionary(
				lhs._dict.colSum(lhs.getCounts(), lCol));
			matrixMultDictionariesAndOutputToColIndexes(l, r, lhs._colIndexes, this._colIndexes, result);
		}
		else {
			final int[] countsRight = getCounts();
			final int mostFrequentRight = Math.max(countsRight[0], countsRight[countsRight.length - 1]);
			final double percentageRight = (double) mostFrequentRight / this._numRows;
			final int[] countsLeft = lhs.getCounts();
			final int mostFrequentLeft = Math.max(countsLeft[0], countsLeft[countsLeft.length - 1]);
			final double percentageLeft = (double) mostFrequentLeft / this._numRows;

			// If exploiting common elements
			final double costRightSkipping = percentageRight * nvR * rCol;
			final double costLeftSkipping = percentageLeft * nvL * lCol;

			// If dense iteration
			final double costRightDense = nvR * rCol;
			final double costLeftDense = nvL * lCol;

			if(percentageRight > CommonElementThreshold && costRightSkipping < costLeftSkipping &&
				!(this instanceof ColGroupDDC)) {
				double[] mct = this._dict.getMostCommonTuple(this.getCounts(), rCol);
				double[] lhsSum = lhs._dict.colSum(lhs.getCounts(), lCol);
				if(mct != null)
					outerProduct(lhsSum, lhs._colIndexes, mct, this._colIndexes, resV, numCols);
				ColGroupValue thisM = (mct != null) ? (ColGroupValue) this
					.copyAndSet(this._dict.subtractTuple(mct)) : this;
				Dictionary preAgg = lhs.preAggregateThatIndexStructure(thisM, true);
				matrixMultDictionariesAndOutputToColIndexes(lhs._dict, preAgg, lhs._colIndexes, this._colIndexes,
					result);
			}
			else if(percentageLeft > CommonElementThreshold && costLeftSkipping < costRightDense &&
				!(lhs instanceof ColGroupDDC)) {
				double[] mct = lhs._dict.getMostCommonTuple(lhs.getCounts(), lCol);
				double[] thisColSum = this._dict.colSum(getCounts(), rCol);
				if(mct != null)
					outerProduct(mct, lhs._colIndexes, thisColSum, this._colIndexes, resV, numCols);

				ColGroupValue lhsM = (mct != null) ? (ColGroupValue) lhs.copyAndSet(lhs._dict.subtractTuple(mct)) : lhs;
				Dictionary preAgg = this.preAggregateThatIndexStructure(lhsM, true);
				matrixMultDictionariesAndOutputToColIndexes(preAgg, this._dict, lhs._colIndexes, this._colIndexes,
					result);
			}
			else if(costRightDense < costLeftDense) {
				Dictionary preAgg = lhs.preAggregateThatIndexStructure(this, false);
				matrixMultDictionariesAndOutputToColIndexes(lhs._dict, preAgg, lhs._colIndexes, this._colIndexes,
					result);
			}
			else {
				Dictionary preAgg = this.preAggregateThatIndexStructure(lhs, false);
				matrixMultDictionariesAndOutputToColIndexes(preAgg, this._dict, lhs._colIndexes, this._colIndexes,
					result);
			}
		}
	}

	private void leftMultByUncompressedColGroup(ColGroupUncompressed lhs, MatrixBlock result) {
		MatrixBlock ucCG = lhs.getData();
		if(this instanceof ColGroupConst) {
			AggregateUnaryOperator auop = InstructionUtils.parseBasicAggregateUnaryOperator("uac+", 1);
			MatrixBlock tmp = ucCG.aggregateUnaryOperations(auop, new MatrixBlock(),
				Math.max(ucCG.getNumRows(), ucCG.getNumColumns()), null, true);
			ADictionary l = new MatrixBlockDictionary(tmp);
			matrixMultDictionariesAndOutputToColIndexes(l, _dict, lhs._colIndexes, _colIndexes, result);
		}
		else {
			LOG.warn("Inefficient transpose of uncompressed to fit to "
				+ "template need t(UnCompressedColGroup) %*% AColGroup support");
			MatrixBlock tmp = new MatrixBlock(ucCG.getNumColumns(), ucCG.getNumRows(), ucCG.isInSparseFormat());
			LibMatrixReorg.transpose(ucCG, tmp, InfrastructureAnalyzer.getLocalParallelism());

			leftMultByMatrix(tmp, result, lhs._colIndexes);
		}
	}

	@Override
	public final void tsmm(MatrixBlock ret) {
		double[] result = ret.getDenseBlockValues();
		int numColumns = ret.getNumColumns();
		tsmm(result, numColumns);
	}

	private final void tsmm(double[] result, int numColumns) {

		final int[] counts = getCounts();

		_dict = _dict.getAsMatrixBlockDictionary(_colIndexes.length);
		if(_dict instanceof MatrixBlockDictionary) {
			MatrixBlockDictionary mbd = (MatrixBlockDictionary) _dict;
			MatrixBlock mb = mbd.getMatrixBlock();
			if(mb.isEmpty())
				return;
			else if(mb.isInSparseFormat())
				tsmmSparse(result, numColumns, mb.getSparseBlock(), counts);
			else
				tsmmDense(result, numColumns, mb.getDenseBlockValues(), counts);
		}
		else
			tsmmDense(result, numColumns, getValues(), counts);

	}

	private void tsmmDense(double[] result, int numColumns, double[] values, int[] counts) {
		if(values == null)
			return;
		final int nCol = _colIndexes.length;
		final int nRow = values.length / _colIndexes.length;
		for(int k = 0; k < nRow; k++) {
			final int offTmp = nCol * k;
			final int scale = counts[k];
			for(int i = 0; i < nCol; i++) {
				final int offRet = numColumns * _colIndexes[i];
				final double v = values[offTmp + i] * scale;
				if(v != 0)
					for(int j = i; j < nCol; j++)
						result[offRet + _colIndexes[j]] += v * values[offTmp + j];
			}
		}
	}

	private void tsmmSparse(double[] result, int numColumns, SparseBlock sb, int[] counts) {
		for(int row = 0; row < sb.numRows(); row++) {
			if(sb.isEmpty(row))
				continue;
			final int apos = sb.pos(row);
			final int alen = sb.size(row);
			final int[] aix = sb.indexes(row);
			final double[] avals = sb.values(row);
			for(int i = apos; i < apos + alen; i++) {
				final int offRet = _colIndexes[aix[i]] * numColumns;
				final double val = avals[i] * counts[row];
				for(int j = i; j < apos + alen; j++) {
					result[offRet + _colIndexes[aix[j]]] += val * avals[j];
				}
			}
		}
	}

	@Override
	public final boolean containsValue(double pattern) {
		if(pattern == 0 && _zeros)
			return true;
		return _dict.containsValue(pattern);
	}

	@Override
	public final long getNumberNonZeros() {
		int[] counts = getCounts();
		return _dict.getNumberNonZeros(counts, _colIndexes.length);
	}

	private static void matrixMultDictionariesAndOutputToColIndexesWithScaling(final ADictionary left,
		final ADictionary right, final int[] leftRows, final int[] rightColumns, final MatrixBlock result,
		final int[] counts) {
		final boolean modifyRight = right.getInMemorySize() > left.getInMemorySize();
		ADictionary rightM = modifyRight ? right.scaleTuples(counts, rightColumns.length) : right;
		ADictionary leftM = modifyRight ? left : left.scaleTuples(counts, leftRows.length);

		matrixMultDictionariesAndOutputToColIndexes(leftM, rightM, leftRows, rightColumns, result);

	}

	private static void tsmmDictionaryWithScaling(final ADictionary dict, final int[] counts, final int[] rows,
		final int[] cols, final double[] res, final int outCols) {

		if(dict instanceof MatrixBlockDictionary) {
			MatrixBlockDictionary mbd = (MatrixBlockDictionary) dict;
			MatrixBlock mb = mbd.getMatrixBlock();
			if(mb.isEmpty())
				return;
			else if(mb.isInSparseFormat()) {
				SparseBlock sb = mb.getSparseBlock();
				for(int row = 0; row < sb.numRows(); row++) {
					if(sb.isEmpty(row))
						continue;
					final int apos = sb.pos(row);
					final int alen = sb.size(row);
					final int[] aix = sb.indexes(row);
					final double[] avals = sb.values(row);
					for(int i = apos; i < apos + alen; i++) {
						final int offRet = rows[aix[i]] * outCols;
						final double val = avals[i] * counts[row];
						for(int j = i; j < apos + alen; j++) {
							res[offRet + cols[aix[j]]] += val * avals[j];
						}
					}
				}
			}
			else {
				throw new NotImplementedException();
			}
		}
		else {
			double[] values = dict.getValues();
			for(int row = 0; row < rows.length; row++) {
				final int offTmp = cols.length * row;
				final int offRet = outCols * rows[row];
				for(int col = 0; col < cols.length; col++) {
					final double v = values[offTmp + col] * counts[row];
					if(v != 0)
						for(int j = col; j < cols.length; j++)
							res[offRet + cols[col]] += v * values[offTmp + j];
				}
			}
		}
	}

	private static void outerProduct(final double[] left, final int[] leftRows, final double[] right,
		final int[] rightColumns, final double[] result, final int outCols) {
		if(left.length != leftRows.length)
			throw new DMLCompressionException(
				"Error left length " + left.length + " not equal columns length" + leftRows.length);

		if(right.length != rightColumns.length)
			throw new DMLCompressionException(
				"Error right not equal length " + right.length + " " + rightColumns.length);
		for(int row = 0; row < leftRows.length; row++) {
			final int outputRowOffset = leftRows[row] * outCols;
			final double vLeft = left[row];
			for(int col = 0; col < rightColumns.length; col++)
				result[outputRowOffset + rightColumns[col]] += vLeft * right[col];
		}
	}

	private static boolean logMM = true;

	/**
	 * Matrix Multiply the two matrices, note that the left side is transposed,
	 * 
	 * making the multiplication a: t(left) %*% right
	 * 
	 * @param left      The left side dictionary
	 * @param right     The right side dictionary
	 * @param rowsLeft  The number of rows and the row indexes on the left hand side
	 * @param colsRight The number of columns and the column indexes on the right hand side
	 * @param result    The result matrix to put the results into.
	 */
	private static void matrixMultDictionariesAndOutputToColIndexes(ADictionary left, ADictionary right, int[] rowsLeft,
		int[] colsRight, MatrixBlock result) {

		try {
			double[] leftV = null;
			double[] rightV = null;

			if(left instanceof MatrixBlockDictionary) {
				MatrixBlockDictionary leftD = left.getAsMatrixBlockDictionary(rowsLeft.length);
				MatrixBlock leftMB = leftD.getMatrixBlock();
				if(leftMB.isEmpty()) {
					return;
				}
				else if(right instanceof MatrixBlockDictionary) {
					MatrixBlockDictionary rightD = right.getAsMatrixBlockDictionary(colsRight.length);
					MatrixBlock rightMB = rightD.getMatrixBlock();
					if(rightMB.isEmpty())
						return;
					else if(rightMB.isInSparseFormat() && leftMB.isInSparseFormat())
						throw new NotImplementedException("Not Supported sparse sparse dictionary multiplication");
					else if(rightMB.isInSparseFormat())
						matrixMultDictionariesAndOutputToColIndecesDenseSparse(leftMB.getDenseBlockValues(),
							rightMB.getSparseBlock(), rowsLeft, colsRight, result);
					else if(leftMB.isInSparseFormat())
						matrixMultDictionariesAndOutputToColIndecesSparseDense(leftMB.getSparseBlock(),
							rightMB.getDenseBlockValues(), rowsLeft, colsRight, result);
					else
						matrixMultDictionariesAndOutputToColIndexesDenseDense(leftMB.getDenseBlockValues(),
							rightMB.getDenseBlockValues(), rowsLeft, colsRight, result);
					return;
				}
				else if(leftMB.isInSparseFormat()) {
					matrixMultDictionariesAndOutputToColIndecesSparseDense(leftMB.getSparseBlock(), right.getValues(),
						rowsLeft, colsRight, result);
					return;
				}
				else {
					leftV = leftMB.getDenseBlockValues();
				}
			}
			else {
				leftV = left.getValues();
			}

			if(right instanceof MatrixBlockDictionary) {
				MatrixBlockDictionary rightD = right.getAsMatrixBlockDictionary(colsRight.length);
				MatrixBlock rightMB = rightD.getMatrixBlock();

				if(rightMB.isEmpty()) {
					return;
				}
				else if(rightMB.isInSparseFormat()) {
					matrixMultDictionariesAndOutputToColIndecesDenseSparse(leftV, rightMB.getSparseBlock(), rowsLeft,
						colsRight, result);
					return;
				}
				else {
					rightV = rightMB.getDenseBlockValues();
				}
			}
			else {
				rightV = right.getValues();
			}

			if(leftV != null && rightV != null)
				matrixMultDictionariesAndOutputToColIndexesDenseDense(leftV, rightV, rowsLeft, colsRight, result);

		}
		catch(Exception e) {
			if(logMM) {
				LOG.error("\nLeft (transposed):\n" + left + "\nRight:\n" + right);
				logMM = false;
			}
			throw new DMLCompressionException("MM of pre aggregated colGroups failed", e);
		}
	}

	private static void matrixMultDictionariesAndOutputToColIndexesDenseDense(double[] left, double[] right,
		int[] rowsLeft, int[] colsRight, MatrixBlock result) {
		final int commonDim = Math.min(left.length / rowsLeft.length, right.length / colsRight.length);

		final double[] resV = result.getDenseBlockValues();
		for(int k = 0; k < commonDim; k++) {
			final int offL = k * rowsLeft.length;
			final int offR = k * colsRight.length;
			for(int i = 0; i < rowsLeft.length; i++) {
				final int offOut = rowsLeft[i] * result.getNumColumns();
				final double vl = left[offL + i];
				if(vl != 0)
					for(int j = 0; j < colsRight.length; j++) {
						final double vr = right[offR + j];
						resV[offOut + colsRight[j]] += vl * vr;
					}
			}
		}
	}

	private static void matrixMultDictionariesAndOutputToColIndecesSparseDense(SparseBlock left, double[] right,
		int[] rowsLeft, int[] colsRight, MatrixBlock result) {

		final double[] resV = result.getDenseBlockValues();
		final int commonDim = Math.min(left.numRows(), right.length / colsRight.length);
		for(int i = 0; i < commonDim; i++) {
			if(left.isEmpty(i))
				continue;
			final int apos = left.pos(i);
			final int alen = left.size(i) + apos;
			final int[] aix = left.indexes(i);
			final double[] leftVals = left.values(i);
			final int offRight = i * colsRight.length;
			for(int k = apos; k < alen; k++) {
				final int offOut = rowsLeft[aix[k]] * result.getNumColumns();
				final double v = leftVals[k];
				for(int j = 0; j < colsRight.length; j++)
					resV[offOut + colsRight[j]] += v * right[offRight + j];
			}
		}
	}

	private static void matrixMultDictionariesAndOutputToColIndecesDenseSparse(double[] left, SparseBlock right,
		int[] rowsLeft, int[] colsRight, MatrixBlock result) {
		final double[] resV = result.getDenseBlockValues();
		final int commonDim = Math.min(left.length / rowsLeft.length, right.numRows());
		for(int i = 0; i < commonDim; i++) {
			if(right.isEmpty(i))
				continue;
			final int apos = right.pos(i);
			final int alen = right.size(i) + apos;
			final int[] aix = right.indexes(i);
			final double[] rightVals = right.values(i);
			final int offLeft = i * rowsLeft.length;
			for(int j = 0; j < rowsLeft.length; j++) {
				final int offOut = rowsLeft[j] * result.getNumColumns();
				final double v = left[offLeft + j];
				if(v != 0)
					for(int k = apos; k < alen; k++) {
						resV[offOut + colsRight[aix[k]]] += v * rightVals[k];
					}
			}
		}
	}

	@Override
	public final boolean isDense() {
		return !_zeros;
	}

	/**
	 * Multiply with a matrix on the left.
	 * 
	 * @param matrix matrix to left multiply
	 * @param result matrix block result
	 * @param rl     The row to start the matrix multiplication from
	 * @param ru     The row to stop the matrix multiplication at.
	 */
	@Override
	public final void leftMultByMatrix(MatrixBlock matrix, MatrixBlock result, int rl, int ru) {
		try {
			final int numVals = getNumValues();
			// Pre aggregate the matrix into same size as dictionary
			MatrixBlock preAgg = allocatePreAggregate(matrix, numVals, rl, ru);
			preAggregate(matrix, preAgg, rl, ru);
			preAgg.recomputeNonZeros();
			MatrixBlock tmpRes = leftMultByPreAggregateMatrix(preAgg);
			addMatrixToResult(tmpRes, result, rl, ru);
		}
		catch(Exception e) {
			throw new DMLCompressionException(this.getClass().getSimpleName() + " Failed to Left Matrix Multiply", e);
		}
	}

	public final MatrixBlock leftMultByPreAggregateMatrix(MatrixBlock preAgg) {

		// Allocate temporary matrix to multiply into.
		final int tmpCol = _colIndexes.length;
		final int tmpRow = preAgg.getNumRows();
		double[] tmpLeftMultRes = tmpLeftMultDoubleArray.get();

		MatrixBlock tmpRes = null;
		if(tmpLeftMultRes != null && tmpLeftMultRes.length >= tmpCol * tmpRow) {
			tmpRes = new MatrixBlock(tmpRow, tmpCol, new DenseBlockFP64(new int[] {tmpRow, tmpCol}, tmpLeftMultRes));
			tmpRes.reset();
		}
		else {
			tmpRes = new MatrixBlock(tmpRow, tmpCol, false);
		}

		return leftMultByPreAggregateMatrix(preAgg, tmpRes);
	}

	public final MatrixBlock leftMultByPreAggregateMatrix(MatrixBlock preAgg, MatrixBlock tmpRes) {
		// Get dictionary.
		MatrixBlock dictM = forceMatrixBlockDictionary().getMatrixBlock();
		LibMatrixMult.matrixMult(preAgg, dictM, tmpRes);
		return tmpRes;
	}

	private void leftMultByMatrix(MatrixBlock matrix, MatrixBlock result, int[] outputRows) {
		try {
			final int numVals = getNumValues();
			MatrixBlock preAgg = allocatePreAggregate(matrix, numVals, 0, matrix.getNumRows());
			preAggregate(matrix, preAgg, 0, matrix.getNumRows());
			preAgg.recomputeNonZeros();
			MatrixBlock tmpRes = leftMultByPreAggregateMatrix(preAgg);
			addMatrixToResult(tmpRes, result, outputRows);

		}
		catch(Exception e) {
			throw new DMLCompressionException(
				this.getClass().getSimpleName() + " Failed to multiply with an uncompressed column group", e);
		}
	}

	private MatrixBlockDictionary forceMatrixBlockDictionary() {
		if(!(_dict instanceof MatrixBlockDictionary))
			_dict = _dict.getAsMatrixBlockDictionary(_colIndexes.length);
		return((MatrixBlockDictionary) _dict);
	}

	public void addMatrixToResult(MatrixBlock tmp, MatrixBlock result, int rl, int ru) {
		if(tmp.isEmpty())
			return;
		final double[] retV = result.getDenseBlockValues();
		final int nColRet = result.getNumColumns();
		if(tmp.isInSparseFormat()) {
			SparseBlock sb = tmp.getSparseBlock();
			for(int row = rl, offT = 0; row < ru; row++, offT++) {
				final int apos = sb.pos(offT);
				final int alen = sb.size(offT);
				final int[] aix = sb.indexes(offT);
				final double[] avals = sb.values(offT);
				final int offR = row * nColRet;
				for(int i = apos; i < apos + alen; i++) {
					retV[offR + _colIndexes[aix[i]]] += avals[i];
				}
			}
		}
		else {
			final double[] tmpV = tmp.getDenseBlockValues();
			final int nCol = _colIndexes.length;
			for(int row = rl, offT = 0; row < ru; row++, offT += nCol) {
				final int offR = row * nColRet;
				for(int col = 0; col < nCol; col++) {
					retV[offR + _colIndexes[col]] += tmpV[offT + col];
				}
			}
		}
	}

	private void addMatrixToResult(MatrixBlock tmp, MatrixBlock result, int[] rowIndexes) {
		if(tmp.isEmpty())
			return;
		final double[] retV = result.getDenseBlockValues();
		final int nColRet = result.getNumColumns();
		if(tmp.isInSparseFormat()) {
			SparseBlock sb = tmp.getSparseBlock();
			for(int row = 0; row < rowIndexes.length; row++) {
				final int apos = sb.pos(row);
				final int alen = sb.size(row);
				final int[] aix = sb.indexes(row);
				final double[] avals = sb.values(row);
				final int offR = rowIndexes[row] * nColRet;
				for(int i = apos; i < apos + alen; i++) {
					retV[offR + _colIndexes[aix[i]]] += avals[i];
				}
			}
		}
		else {
			final double[] tmpV = tmp.getDenseBlockValues();
			final int nCol = _colIndexes.length;
			for(int row = 0, offT = 0; row < rowIndexes.length; row++, offT += nCol) {
				final int offR = rowIndexes[row] * nColRet;
				for(int col = 0; col < nCol; col++) {
					retV[offR + _colIndexes[col]] += tmpV[offT + col];
				}
			}
		}
	}

	public final AColGroup rightMultByMatrix(MatrixBlock right) {

		if(right.isEmpty())
			return null;
		final int cl = 0;
		final int cu = right.getNumColumns();
		final int cut = right.getNumColumns();
		final int nCol = right.getNumColumns();
		final int numVals = getNumValues();
		if(right.isInSparseFormat()) {
			final SparseBlock sb = right.getSparseBlock();
			final int[] agCols = getAggregateColumnsSetSparse(sb, nCol);
			if(agCols.length == 0)
				return null;
			return copyAndSet(agCols, preaggValuesFromSparse(numVals, sb, agCols, cl, cu, cut));
		}
		else {
			final double[] rightV = right.getDenseBlockValues();
			final int[] agCols = getAggregateColumnsSetDense(rightV, cl, cu, cut);
			if(agCols.length == 0)
				return null;
			ADictionary d = _dict.preaggValuesFromDense(numVals, _colIndexes, agCols, rightV, cut);
			if(d == null)
				return null;
			return copyAndSet(agCols, d);
		}
	}

	@Override
	public long estimateInMemorySize() {
		long size = super.estimateInMemorySize();
		size += 8; // Dictionary Reference.
		size += 8; // Counts reference
		size += 1; // _zeros boolean reference
		size += 1; // _lossy boolean reference
		size += 2; // padding
		size += _dict.getInMemorySize();
		return size;
	}

	@Override
	public AColGroup replace(double pattern, double replace) {
		ADictionary replaced = _dict.replace(pattern, replace, _colIndexes.length, _zeros);
		return copyAndSet(replaced);
	}
}
