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
import java.util.Arrays;

import org.apache.commons.lang.NotImplementedException;
import org.apache.sysds.runtime.compress.colgroup.dictionary.ADictionary;
import org.apache.sysds.runtime.compress.colgroup.dictionary.Dictionary;
import org.apache.sysds.runtime.compress.colgroup.offset.AIterator;
import org.apache.sysds.runtime.compress.colgroup.offset.AOffset;
import org.apache.sysds.runtime.compress.colgroup.offset.OffsetFactory;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.functionobjects.Builtin;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.ScalarOperator;

/**
 * Column group that sparsely encodes the dictionary values. The idea is that all values is encoded with indexes except
 * the most common one. the most common one can be inferred by not being included in the indexes. If the values are very
 * sparse then the most common one is zero.
 * 
 * This column group is handy in cases where sparse unsafe operations is executed on very sparse columns. Then the zeros
 * would be materialized in the group without any overhead.
 */
public class ColGroupSDCSingle extends ColGroupValue {
	private static final long serialVersionUID = 3883228464052204200L;
	/**
	 * Sparse row indexes for the data
	 */
	protected AOffset _indexes;

	/**
	 * Constructor for serialization
	 * 
	 * @param numRows Number of rows contained
	 */
	protected ColGroupSDCSingle(int numRows) {
		super(numRows);
	}

	protected ColGroupSDCSingle(int[] colIndices, int numRows, ADictionary dict, int[] indexes) {
		super(colIndices, numRows, dict);
		_indexes = OffsetFactory.create(indexes, numRows);
		_zeros = false;
	}

	protected ColGroupSDCSingle(int[] colIndices, int numRows, ADictionary dict, int[] indexes, int[] cachedCounts) {
		super(colIndices, numRows, dict, cachedCounts);
		_indexes = OffsetFactory.create(indexes, numRows);
		_zeros = false;
	}

	protected ColGroupSDCSingle(int[] colIndices, int numRows, ADictionary dict, AOffset offsets, int[] cachedCounts) {
		super(colIndices, numRows, dict, cachedCounts);
		_indexes = offsets;
		_zeros = false;
	}

	@Override
	public CompressionType getCompType() {
		return CompressionType.SDC;
	}

	@Override
	public ColGroupType getColGroupType() {
		return ColGroupType.SDCSingle;
	}

	@Override
	protected void decompressToBlockUnSafeDenseDictionary(MatrixBlock target, int rl, int ru, int offT,
		double[] values) {
		final int nCol = _colIndexes.length;
		final int tCol = target.getNumColumns();
		final int offsetToDefault = values.length - nCol;

		double[] c = target.getDenseBlockValues();
		offT = offT * tCol;
		int i = rl;
		AIterator it = _indexes.getIterator();
		it.skipTo(rl);
		for(; i < ru && it.hasNext(); i++, offT += tCol) {
			if(it.value() == i) {
				for(int j = 0; j < nCol; j++)
					c[offT + _colIndexes[j]] += values[j];
				it.next();
			}
			else
				for(int j = 0; j < nCol; j++)
					c[offT + _colIndexes[j]] += values[offsetToDefault + j];
		}

		for(; i < ru; i++, offT += tCol)
			for(int j = 0; j < nCol; j++)
				c[offT + _colIndexes[j]] += values[offsetToDefault + j];
	}

	@Override
	protected void decompressToBlockUnSafeSparseDictionary(MatrixBlock target, int rl, int ru, int offT,
		SparseBlock values) {
		throw new NotImplementedException();
	}

	@Override
	public double get(int r, int c) {
		// find local column index
		int ix = Arrays.binarySearch(_colIndexes, c);
		if(ix < 0)
			throw new RuntimeException("Column index " + c + " not in group.");

		AIterator it = _indexes.getIterator(r);
		if(it.value() == r)
			return _dict.getValue(ix);
		else
			return _dict.getValue(_colIndexes.length + ix);

	}

	@Override
	public void countNonZerosPerRow(int[] rnnz, int rl, int ru) {
		throw new NotImplementedException("Not Implemented");
	}

	@Override
	protected void computeRowSums(double[] c, boolean square, int rl, int ru) {

		// // pre-aggregate nnz per value tuple
		final double[] vals = _dict.sumAllRowsToDouble(square, _colIndexes.length);
		final AIterator it = _indexes.getIterator();

		int rix = rl;
		it.skipTo(rl);
		for(; rix < ru && it.hasNext(); rix++) {
			if(it.value() != rix)
				c[rix] += vals[1];
			else {
				c[rix] += vals[0];
				it.next();
			}
		}
		for(; rix < ru; rix++) {
			c[rix] += vals[1];
		}
	}

	@Override
	protected void computeRowMxx(double[] c, Builtin builtin, int rl, int ru) {

		final double[] vals = _dict.aggregateTuples(builtin, _colIndexes.length);
		final AIterator it = _indexes.getIterator();

		it.skipTo(rl);
		int rix = rl;

		for(; rix < ru && it.hasNext(); rix++) {
			if(it.value() != rix)
				c[rix] = builtin.execute(c[rix], vals[1]);
			else {
				c[rix] = builtin.execute(c[rix], vals[0]);
				it.next();
			}
		}
		for(; rix < ru; rix++) {
			c[rix] = builtin.execute(c[rix], vals[1]);
		}
	}

	@Override
	public int[] getCounts(int[] counts) {
		counts[0] = _indexes.getSize();
		counts[1] = _numRows - counts[0];
		return counts;
	}

	@Override
	public int[] getCounts(int rl, int ru, int[] counts) {
		final AIterator it = _indexes.getIterator();
		it.skipTo(rl);

		while(it.hasNext() && it.value() < ru) {
			it.next();
			counts[0]++;
		}

		counts[1] = ru - rl - counts[0];

		return counts;
	}

	@Override
	public void preAggregate(MatrixBlock m, MatrixBlock preAgg, int rl, int ru) {
		if(m.isInSparseFormat())
			preAggregateSparse(m.getSparseBlock(), preAgg, rl, ru);
		else
			preAggregateDense(m, preAgg, rl, ru);
	}

	@Override
	public void preAggregateDense(MatrixBlock m, MatrixBlock preAgg, int rl, int ru, int cl, int cu) {
		final double[] mV = m.getDenseBlockValues();
		final double[] preAV = preAgg.getDenseBlockValues();
		final int numVals = getNumValues();

		final int blockSize = 2000;
		for(int block = cl; block < cu; block += blockSize) {
			final int blockEnd = Math.min(block + blockSize, cu);
			final AIterator itStart = _indexes.getIterator(block);
			for(int rowLeft = rl, offOut = 0; rowLeft < ru; rowLeft++, offOut += numVals) {
				final int offLeft = rowLeft * _numRows;
				final int def = offOut + numVals - 1;
				final AIterator it = itStart.clone();
				int rc = 0;
				for(; rc < blockEnd && it.value() < blockEnd && it.hasNext(); rc++) {
					final int pointer = it.value();
					for(; rc < pointer && rc < blockEnd; rc++) {
						preAV[def] += mV[offLeft + rc];
					}
					preAV[offOut] += mV[offLeft + rc];
					it.next();
				}

				for(; rc < blockEnd; rc++) {
					preAV[def] += mV[offLeft + rc];
				}
			}
		}
	}

	private void preAggregateDense(MatrixBlock m, MatrixBlock preAgg, int rl, int ru) {
		final double[] preAV = preAgg.getDenseBlockValues();
		final double[] mV = m.getDenseBlockValues();
		final int numVals = getNumValues();
		for(int rowLeft = rl, offOut = 0; rowLeft < ru; rowLeft++, offOut += numVals) {
			final AIterator it = _indexes.getIterator();
			final int def = offOut + 1;
			int rc = 0;
			int offLeft = rowLeft * _numRows;
			for(; rc < _numRows; rc++, offLeft++) {
				if(it.value() == rc) {
					preAV[offOut] += mV[offLeft];
					it.next();
				}
				else
					preAV[def] += mV[offLeft];
			}
			for(; rc < _numRows; rc++, offLeft++) {
				preAV[def] += mV[offLeft];
			}
		}
	}

	private void preAggregateSparse(SparseBlock sb, MatrixBlock preAgg, int rl, int ru) {
		final double[] preAV = preAgg.getDenseBlockValues();
		final int numVals = getNumValues();
		for(int rowLeft = rl, offOut = 0; rowLeft < ru; rowLeft++, offOut += numVals) {
			if(sb.isEmpty(rowLeft))
				continue;
			final AIterator it = _indexes.getIterator();
			final int apos = sb.pos(rowLeft);
			final int alen = sb.size(rowLeft) + apos;
			final int[] aix = sb.indexes(rowLeft);
			final double[] avals = sb.values(rowLeft);
			final int def = offOut + 1;
			int j = apos;
			for(; it.hasNext() && j < alen; j++) {
				final int index = aix[j];
				it.skipTo(index);
				if(index == it.value()) {
					preAV[offOut] += avals[j];
					it.next();
				}
				else
					preAV[def] += avals[j];
			}

			for(; j < alen; j++) {
				preAV[def] += avals[j];
			}
		}
	}

	@Override
	public long estimateInMemorySize() {
		long size = super.estimateInMemorySize();
		size += _indexes.getInMemorySize();
		return size;
	}

	@Override
	public AColGroup scalarOperation(ScalarOperator op) {
		return new ColGroupSDCSingle(_colIndexes, _numRows, applyScalarOp(op), _indexes, getCachedCounts());
	}

	@Override
	public AColGroup binaryRowOp(BinaryOperator op, double[] v, boolean sparseSafe, boolean left) {
		return new ColGroupSDCSingle(_colIndexes, _numRows, applyBinaryRowOp(op, v, true, left), _indexes,
			getCachedCounts());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		_indexes.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		_indexes = OffsetFactory.readIn(in);
	}

	@Override
	public long getExactSizeOnDisk() {
		long ret = super.getExactSizeOnDisk();
		ret += _indexes.getExactSizeOnDisk();
		return ret;
	}

	@Override
	public boolean sameIndexStructure(ColGroupCompressed that) {
		return that instanceof ColGroupSDCSingle && ((ColGroupSDCSingle) that)._indexes == _indexes;
	}

	@Override
	public int getIndexStructureHash() {
		return _indexes.hashCode();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(String.format("\n%15s ", "Indexes: "));
		sb.append(_indexes.toString());
		return sb.toString();
	}

	@Override
	public Dictionary preAggregateThatDDCStructure(ColGroupDDC that, Dictionary ret) {
		throw new NotImplementedException();
	}

	@Override
	public Dictionary preAggregateThatSDCStructure(ColGroupSDC that, Dictionary ret, boolean preModified) {
		final AIterator itThat = that._indexes.getIterator();
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;

		if(preModified) {
			while(itThat.hasNext()) {
				final int thatV = itThat.value();
				final int fr = that.getIndex(itThat.getDataIndexAndIncrement());
				if(thatV == itThis.skipTo(thatV))
					that._dict.addToEntry(ret, fr, 0, nCol);
				else
					that._dict.addToEntry(ret, fr, 1, nCol);
			}
			return ret;
		}
		else {
			throw new NotImplementedException();
		}
	}

	@Override
	public Dictionary preAggregateThatSDCZerosStructure(ColGroupSDCZeros that, Dictionary ret) {
		throw new NotImplementedException();
	}

	@Override
	public Dictionary preAggregateThatSDCSingleZerosStructure(ColGroupSDCSingleZeros that, Dictionary ret) {
		final AIterator itThat = that._indexes.getIterator();
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;
		while(itThat.hasNext()) {
			final int thatV = itThat.value();
			if(thatV == itThis.skipTo(thatV))
				that._dict.addToEntry(ret, 0, 0, nCol);
			else
				that._dict.addToEntry(ret, 0, 1, nCol);
			itThat.next();
		}

		return ret;

	}

	@Override
	public Dictionary preAggregateThatSDCSingleStructure(ColGroupSDCSingle that, Dictionary ret, boolean preModified) {
		final AIterator itThat = that._indexes.getIterator();
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;
		if(preModified) {
			while(itThat.hasNext()) {
				final int thatV = itThat.value();
				if(thatV == itThis.skipTo(thatV))
					that._dict.addToEntry(ret, 0, 0, nCol);
				else
					that._dict.addToEntry(ret, 0, 1, nCol);
				itThat.next();
			}

			return ret;
		}
		else {
			int i = 0;
			for(; i < _numRows && itThat.hasNext() && itThis.hasNext(); i++) {
				int to = 1;
				if(itThis.value() == i) {
					itThis.next();
					to = 0;
				}
				int fr = 1;
				if(itThat.value() == i) {
					itThat.next();
					fr = 0;
				}
				that._dict.addToEntry(ret, fr, to, nCol);
			}

			for(; i < _numRows && itThat.hasNext(); i++) {
				int fr = 1;
				if(itThat.value() == i) {
					itThat.next();
					fr = 0;
				}
				that._dict.addToEntry(ret, fr, 1, nCol);
			}

			for(; i < _numRows && itThis.hasNext(); i++) {
				int to = 1;
				if(itThis.value() == i) {
					itThis.next();
					to = 0;
				}
				that._dict.addToEntry(ret, 1, to, nCol);
			}

			for(; i < _numRows; i++)
				that._dict.addToEntry(ret, 1, 1, nCol);

			return ret;
		}

	}

	public ColGroupSDCSingleZeros extractCommon(double[] constV) {
		double[] commonV = _dict.getTuple(getNumValues() - 1, _colIndexes.length);

		for(int i = 0; i < _colIndexes.length; i++)
			constV[_colIndexes[i]] += commonV[i];

		ADictionary subtractedDict = _dict.subtractTuple(commonV);
		return new ColGroupSDCSingleZeros(_colIndexes, _numRows, subtractedDict, _indexes, getCounts());
	}

}
