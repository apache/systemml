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
import org.apache.sysds.runtime.compress.colgroup.mapping.AMapToData;
import org.apache.sysds.runtime.compress.colgroup.mapping.MapToFactory;
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
 * the most common one. the most common one can be inferred by not being included in the indexes.
 * 
 * If the values are very sparse then the most common one is zero. This is the case for this column group, that
 * specifically exploits that the column contain lots of zero values.
 * 
 * This column group is handy in cases where sparse unsafe operations is executed on very sparse columns.
 */
public class ColGroupSDCZeros extends ColGroupValue {
	private static final long serialVersionUID = -3703199743391937991L;

	/**
	 * Sparse row indexes for the data
	 */
	protected AOffset _indexes;

	/**
	 * Pointers to row indexes in the dictionary. Note the dictionary has one extra entry.
	 */
	protected AMapToData _data;

	/**
	 * Constructor for serialization
	 * 
	 * @param numRows Number of rows contained
	 */
	protected ColGroupSDCZeros(int numRows) {
		super(numRows);
	}

	protected ColGroupSDCZeros(int[] colIndices, int numRows, ADictionary dict, int[] indexes, AMapToData data) {
		super(colIndices, numRows, dict);
		_indexes = OffsetFactory.create(indexes, numRows);
		_data = data;
		_zeros = true;
	}

	protected ColGroupSDCZeros(int[] colIndices, int numRows, ADictionary dict, int[] indexes, AMapToData data,
		int[] cachedCounts) {
		super(colIndices, numRows, dict, cachedCounts);
		_indexes = OffsetFactory.create(indexes, numRows);
		_data = data;
		_zeros = true;
	}

	protected ColGroupSDCZeros(int[] colIndices, int numRows, ADictionary dict, AOffset offsets, AMapToData data,
		int[] cachedCounts) {
		super(colIndices, numRows, dict, cachedCounts);
		_indexes = offsets;
		_data = data;
		_zeros = true;
	}

	@Override
	public CompressionType getCompType() {
		return CompressionType.SDC;
	}

	@Override
	public ColGroupType getColGroupType() {
		return ColGroupType.SDCZeros;
	}

	@Override
	protected void decompressToBlockUnSafeDenseDictionary(MatrixBlock target, int rl, int ru, int offT,
		double[] values) {
		final int nCol = _colIndexes.length;
		final int tCol = target.getNumColumns();
		final int offTCorrected = offT - rl;
		final double[] c = target.getDenseBlockValues();
		AIterator it = _indexes.getIterator(rl);
		offT = offT * tCol;
		while(it.hasNext() && it.value() < ru) {
			int rc = (offTCorrected + it.value()) * tCol;
			int offC = getIndex(it.getDataIndexAndIncrement()) * nCol;
			for(int j = 0; j < nCol; j++) {
				c[rc + _colIndexes[j]] += values[offC + j];
			}
		}
	}

	@Override
	protected void decompressToBlockUnSafeSparseDictionary(MatrixBlock target, int rl, int ru, int offT,
		SparseBlock sb) {

		final int tCol = target.getNumColumns();
		final int offTCorrected = offT - rl;
		final double[] c = target.getDenseBlockValues();
		AIterator it = _indexes.getIterator(rl);
		while(it.hasNext() && it.value() < ru) {
			final int rc = (offTCorrected + it.value()) * tCol;
			final int dictIndex = getIndex(it.getDataIndexAndIncrement());
			if(sb.isEmpty(dictIndex))
				continue;

			final int apos = sb.pos(dictIndex);
			final int alen = sb.size(dictIndex) + apos;
			final double[] avals = sb.values(dictIndex);
			final int[] aix = sb.indexes(dictIndex);
			for(int j = apos; j < alen; j++)
				c[rc + _colIndexes[aix[j]]] += avals[j];
		}
	}

	@Override
	public double get(int r, int c) {
		int ix = Arrays.binarySearch(_colIndexes, c);
		if(ix < 0)
			throw new RuntimeException("Column index " + c + " not in group.");

		final AIterator it = _indexes.getIterator(r);
		if(it.value() == r)
			return _dict.getValue(getIndex(it.getDataIndex()) * _colIndexes.length + ix);
		else
			return 0.0;

	}

	@Override
	public void countNonZerosPerRow(int[] rnnz, int rl, int ru) {
		final int nCol = _colIndexes.length;
		final AIterator it = _indexes.getIterator(rl);
		while(it.hasNext() && it.value() < ru) {
			rnnz[it.value() - rl] += nCol;
			it.next();
		}
	}

	@Override
	protected void computeRowSums(double[] c, boolean square, int rl, int ru) {
		final double[] vals = _dict.sumAllRowsToDouble(square, _colIndexes.length);
		final AIterator it = _indexes.getIterator(rl);
		while(it.hasNext() && it.value() < ru)
			c[it.value()] += vals[getIndex(it.getDataIndexAndIncrement())];
	}

	@Override
	protected void computeRowMxx(double[] c, Builtin builtin, int rl, int ru) {
		final double[] vals = _dict.aggregateTuples(builtin, _colIndexes.length);
		final AIterator it = _indexes.getIterator(rl);

		int rix = rl;
		for(; rix < ru && it.hasNext(); rix++) {
			if(it.value() != rix)
				c[rix] = builtin.execute(c[rix], 0);
			else
				c[rix] = builtin.execute(c[rix], vals[_data.getIndex(it.getDataIndexAndIncrement())]);
		}

		for(; rix < ru; rix++) {
			c[rix] = builtin.execute(c[rix], 0);
		}
	}

	@Override
	public int[] getCounts(int[] counts) {
		return getCounts(0, _numRows, counts);
	}

	@Override
	public int[] getCounts(int rl, int ru, int[] counts) {

		int i = rl;
		final AIterator it = _indexes.getIterator(rl);

		int zeros = 0;
		while(it.hasNext() && it.value() < ru) {
			int oldI = i;
			i = it.value();
			zeros += i - oldI - 1;
			counts[_data.getIndex(it.getDataIndexAndIncrement())]++;
		}

		counts[counts.length - 1] += zeros + ru - i;

		return counts;
	}

	public int getIndex(int r) {
		return _data.getIndex(r);
	}

	@Override
	public void preAggregate(MatrixBlock m, MatrixBlock preAgg, int rl, int ru) {
		if(m.isInSparseFormat())
			preAggregateSparse(m.getSparseBlock(), preAgg, rl, ru);
		else
			preAggregateDenseOld(m, preAgg, rl, ru);
	}

	@Override
	public void preAggregateDense(MatrixBlock m, MatrixBlock preAgg, int rl, int ru, int cl, int cu) {
		final double[] mV = m.getDenseBlockValues();
		final double[] preAV = preAgg.getDenseBlockValues();
		final int numVals = getNumValues();

		final AIterator itStart = _indexes.getIterator(cl);
		AIterator it = null;
		for(int rowLeft = rl, offOut = 0; rowLeft < ru; rowLeft++, offOut += numVals) {
			final int offLeft = rowLeft * _numRows;
			it = itStart.clone();
			while(it.value() < cu && it.hasNext()) {
				final int i = it.value();
				preAV[offOut + getIndex(it.getDataIndexAndIncrement())] += mV[offLeft + i];
			}
		}
		if(it != null)
			_indexes.cacheIterator(it, cu+1);
	}

	private void preAggregateDenseOld(MatrixBlock m, MatrixBlock preAgg, int rl, int ru) {
		final double[] preAV = preAgg.getDenseBlockValues();
		final double[] mV = m.getDenseBlockValues();
		final int numVals = getNumValues();
		for(int rowLeft = rl, offOut = 0; rowLeft < ru; rowLeft++, offOut += numVals) {
			final AIterator it = _indexes.getIterator();
			final int offLeft = rowLeft * _numRows;
			while(it.hasNext()) {
				final int i = it.value();
				preAV[offOut + getIndex(it.getDataIndexAndIncrement())] += mV[offLeft + i];
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
			int j = apos;
			while(it.hasNext() && j < alen) {
				final int index = aix[j];
				final int val = it.value();
				if(index < val)
					j++;
				else if(index == val)
					preAV[offOut + _data.getIndex(it.getDataIndexAndIncrement())] += avals[j++];
				else
					it.next();
			}
		}
	}

	@Override
	public long estimateInMemorySize() {
		long size = super.estimateInMemorySize();
		size += _indexes.getInMemorySize();
		size += _data.getInMemorySize();
		return size;
	}

	@Override
	public AColGroup scalarOperation(ScalarOperator op) {
		double val0 = op.executeScalar(0);
		boolean isSparseSafeOp = op.sparseSafe || val0 == 0;
		if(isSparseSafeOp)
			return new ColGroupSDCZeros(_colIndexes, _numRows, applyScalarOp(op), _indexes, _data, getCachedCounts());
		else {
			ADictionary rValues = applyScalarOp(op, val0, getNumCols());
			return new ColGroupSDC(_colIndexes, _numRows, rValues, _indexes, _data, getCachedCounts());
		}
	}

	@Override
	public AColGroup binaryRowOp(BinaryOperator op, double[] v, boolean sparseSafe, boolean left) {
		if(sparseSafe)
			return new ColGroupSDCZeros(_colIndexes, _numRows, applyBinaryRowOp(op, v, sparseSafe, left), _indexes,
				_data, getCachedCounts());
		else
			return new ColGroupSDC(_colIndexes, _numRows, applyBinaryRowOp(op, v, sparseSafe, left), _indexes, _data,
				getCachedCounts());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		_indexes.write(out);
		_data.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		_indexes = OffsetFactory.readIn(in);
		_data = MapToFactory.readIn(in);
	}

	@Override
	public long getExactSizeOnDisk() {
		long ret = super.getExactSizeOnDisk();
		ret += _indexes.getExactSizeOnDisk();
		ret += _data.getInMemorySize();
		return ret;
	}

	@Override
	public boolean sameIndexStructure(ColGroupCompressed that) {
		return that instanceof ColGroupSDCZeros && ((ColGroupSDCZeros) that)._indexes == _indexes &&
			((ColGroupSDCZeros) that)._data == _data;
	}

	@Override
	public int getIndexStructureHash() {
		return _indexes.hashCode() + _data.hashCode();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(String.format("\n%15s ", "Indexes: "));
		sb.append(_indexes.toString());
		sb.append(String.format("\n%15s ", "Data: "));
		sb.append(_data);
		return sb.toString();
	}

	@Override
	public Dictionary preAggregateThatDDCStructure(ColGroupDDC that, Dictionary ret) {
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;

		while(itThis.hasNext()) {
			final int fr = that._data.getIndex(itThis.value());
			final int to = getIndex(itThis.getDataIndexAndIncrement());
			that._dict.addToEntry(ret, fr, to, nCol);
		}

		return ret;
	}

	@Override
	public Dictionary preAggregateThatSDCStructure(ColGroupSDC that, Dictionary ret, boolean preModified) {
		throw new NotImplementedException();
	}

	@Override
	public Dictionary preAggregateThatSDCZerosStructure(ColGroupSDCZeros that, Dictionary ret) {
		final AIterator itThat = that._indexes.getIterator();
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;

		while(itThat.hasNext() && itThis.hasNext()) {
			if(itThat.value() == itThis.value()) {
				final int fr = that.getIndex(itThat.getDataIndexAndIncrement());
				final int to = getIndex(itThis.getDataIndexAndIncrement());
				that._dict.addToEntry(ret, fr, to, nCol);
			}
			else if(itThat.value() < itThis.value())
				itThat.next();
			else
				itThis.next();
		}
		return ret;
	}

	@Override
	public Dictionary preAggregateThatSDCSingleZerosStructure(ColGroupSDCSingleZeros that, Dictionary ret) {
		final AIterator itThat = that._indexes.getIterator();
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;

		while(itThat.hasNext() && itThis.hasNext()) {
			if(itThat.value() == itThis.value()) {
				final int to = getIndex(itThis.getDataIndexAndIncrement());
				that._dict.addToEntry(ret, 0, to, nCol);
				itThat.next();
			}
			else if(itThat.value() < itThis.value())
				itThat.next();
			else
				itThis.next();
		}
		return ret;
	}

	@Override
	public Dictionary preAggregateThatSDCSingleStructure(ColGroupSDCSingle that, Dictionary re, boolean preModified) {
		throw new NotImplementedException();
	}

}
