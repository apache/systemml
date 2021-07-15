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
 * the most common one. the most common one can be inferred by not being included in the indexes. If the values are very
 * sparse then the most common one is zero.
 * 
 * This column group is handy in cases where sparse unsafe operations is executed on very sparse columns. Then the zeros
 * would be materialized in the group without any overhead.
 */
public class ColGroupSDC extends ColGroupValue {
	private static final long serialVersionUID = 769993538831949086L;
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
	protected ColGroupSDC(int numRows) {
		super(numRows);
	}

	protected ColGroupSDC(int[] colIndices, int numRows, ADictionary dict, int[] indexes, AMapToData data) {
		super(colIndices, numRows, dict);
		_indexes = OffsetFactory.create(indexes, numRows);
		_data = data;
		_zeros = false;
	}

	protected ColGroupSDC(int[] colIndices, int numRows, ADictionary dict, int[] indexes, AMapToData data,
		int[] cachedCounts) {
		super(colIndices, numRows, dict, cachedCounts);
		_indexes = OffsetFactory.create(indexes, numRows);
		_data = data;
		_zeros = false;
	}

	protected ColGroupSDC(int[] colIndices, int numRows, ADictionary dict, AOffset offsets, AMapToData data,
		int[] cachedCounts) {
		super(colIndices, numRows, dict, cachedCounts);
		_indexes = offsets;
		_data = data;
		_zeros = false;
	}

	@Override
	public CompressionType getCompType() {
		return CompressionType.SDC;
	}

	@Override
	public ColGroupType getColGroupType() {
		return ColGroupType.SDC;
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
		AIterator it = _indexes.getIterator(rl);
		for(; i < ru && it.hasNext(); i++, offT += tCol) {
			if(it.value() == i) {
				int offset = _data.getIndex(it.getDataIndexAndIncrement()) * nCol;
				for(int j = 0; j < nCol; j++)
					c[offT + _colIndexes[j]] += values[offset + j];
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
		SparseBlock sb) {
		final int tCol = target.getNumColumns();
		final int offsetToDefault = sb.numRows() - 1;
		if(sb.isEmpty(offsetToDefault)) {
			throw new NotImplementedException("Implement a SDCZeros decompress if this is the case");
		}

		final int defApos = sb.pos(offsetToDefault);
		final int defAlen = sb.size(offsetToDefault) + defApos;
		final double[] defAvals = sb.values(offsetToDefault);
		final int[] defAix = sb.indexes(offsetToDefault);

		double[] c = target.getDenseBlockValues();
		offT = offT * tCol;
		int i = rl;
		AIterator it = _indexes.getIterator(rl);
		for(; i < ru && it.hasNext(); i++, offT += tCol) {
			if(it.value() == i) {
				int dictIndex = _data.getIndex(it.getDataIndexAndIncrement());
				if(sb.isEmpty(dictIndex))
					continue;
				final int apos = sb.pos(dictIndex);
				final int alen = sb.size(dictIndex) + apos;
				final double[] avals = sb.values(dictIndex);
				final int[] aix = sb.indexes(dictIndex);
				for(int j = apos; j < alen; j++)
					c[offT + _colIndexes[aix[j]]] += avals[j];
			}
			else
				for(int j = defApos; j < defAlen; j++)
					c[offT + _colIndexes[defAix[j]]] += defAvals[j];
		}

		for(; i < ru; i++, offT += tCol)
			for(int j = defApos; j < defAlen; j++)
				c[offT + _colIndexes[defAix[j]]] += defAvals[j];

	}

	@Override
	public double get(int r, int c) {
		// find local column index
		int ix = Arrays.binarySearch(_colIndexes, c);
		if(ix < 0)
			throw new RuntimeException("Column index " + c + " not in group.");

		// get value
		AIterator it = _indexes.getIterator(r);
		final int nCol = _colIndexes.length;
		if(it.value() == r)
			return _dict.getValue(_data.getIndex(it.getDataIndex()) * nCol + ix);
		else
			return _dict.getValue(getNumValues() * nCol - nCol + ix);
	}

	@Override
	public void countNonZerosPerRow(int[] rnnz, int rl, int ru) {
		throw new NotImplementedException("Not Implemented");
	}

	@Override
	protected void computeRowSums(double[] c, boolean square, int rl, int ru) {
		final int numVals = getNumValues();
		// // pre-aggregate nnz per value tuple
		double[] vals = _dict.sumAllRowsToDouble(square, _colIndexes.length);

		int rix = rl;
		AIterator it = _indexes.getIterator(rl);
		for(; rix < ru && it.hasNext(); rix++) {
			if(it.value() != rix)
				c[rix] += vals[numVals - 1];
			else {
				c[rix] += vals[_data.getIndex(it.getDataIndexAndIncrement())];
			}
		}
		for(; rix < ru; rix++) {
			c[rix] += vals[numVals - 1];
		}

	}

	@Override
	protected void computeRowMxx(double[] c, Builtin builtin, int rl, int ru) {

		final int numVals = getNumValues();

		double[] vals = _dict.aggregateTuples(builtin, _colIndexes.length);

		AIterator it = _indexes.getIterator(rl);

		int rix = rl;
		for(; rix < ru && it.hasNext(); rix++) {
			if(it.value() != rix)
				c[rix] = builtin.execute(c[rix], vals[numVals - 1]);
			else
				c[rix] = builtin.execute(c[rix], vals[_data.getIndex(it.getDataIndexAndIncrement())]);
		}

		for(; rix < ru; rix++) {
			c[rix] = builtin.execute(c[rix], vals[numVals - 1]);
		}

	}

	@Override
	public int[] getCounts(int[] counts) {
		return getCounts(0, _numRows, counts);
	}

	@Override
	public int[] getCounts(int rl, int ru, int[] counts) {
		final int def = counts.length - 1;

		int i = rl;
		AIterator it = _indexes.getIterator(rl);

		for(; i < ru && it.hasNext(); i++) {
			if(i == it.value())
				counts[_data.getIndex(it.getDataIndexAndIncrement())]++;
			else
				counts[def]++;
		}

		if(i < ru)
			counts[def] += ru - i;

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
			preAggregateDense(m, preAgg, rl, ru);
	}

	@Override
	public void preAggregateDense(MatrixBlock m, MatrixBlock preAgg, int rl, int ru, int cl, int cu) {
		final double[] mV = m.getDenseBlockValues();
		final double[] preAV = preAgg.getDenseBlockValues();
		final int numVals = getNumValues();
		AIterator itStart = _indexes.getIterator(cl);
		AIterator it = null;
		for(int rowLeft = rl, offOut = 0; rowLeft < ru; rowLeft++, offOut += numVals) {
			final int offLeft = rowLeft * _numRows;
			final int def = offOut + numVals - 1;
			it = itStart.clone();
			int rc = 0;
			int pointer = it.value();
			for(; pointer < cu && it.hasNext(); rc++) {
				for(; rc < pointer; rc++) {
					preAV[def] += mV[offLeft + rc];
				}
				preAV[offOut + _data.getIndex(it.getDataIndexAndIncrement())] += mV[offLeft + rc];
				pointer = it.value();
			}

			for(; rc < cu; rc++) {
				preAV[def] += mV[offLeft + rc];
			}
		}
		if(it != null)
			_indexes.cacheIterator(it, cu + 1);
	}

	private void preAggregateDense(MatrixBlock m, MatrixBlock preAgg, int rl, int ru) {
		final double[] preAV = preAgg.getDenseBlockValues();
		final double[] mV = m.getDenseBlockValues();
		final int numVals = getNumValues();
		for(int rowLeft = rl, offOut = 0; rowLeft < ru; rowLeft++, offOut += numVals) {
			final int def = offOut + numVals - 1;
			final AIterator it = _indexes.getIterator();
			int rc = 0;
			int offLeft = rowLeft * _numRows;
			for(; rc < _numRows && it.hasNext(); rc++, offLeft++) {
				for(; it.value() > rc && rc < _numRows; rc++, offLeft++) {
					preAV[def] += mV[offLeft];
				}
				// if(it.value() == rc)
				preAV[offOut + _data.getIndex(it.getDataIndexAndIncrement())] += mV[offLeft];
				// else
				// preAV[def] += mV[offLeft];
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
			final int def = offOut + numVals - 1;
			final int apos = sb.pos(rowLeft);
			final int alen = sb.size(rowLeft) + apos;
			final int[] aix = sb.indexes(rowLeft);
			final double[] avals = sb.values(rowLeft);
			int j = apos;
			for(; j < alen && it.hasNext(); j++) {
				final int index = aix[j];
				it.skipTo(index);
				if(it.value() == index)
					preAV[offOut + _data.getIndex(it.getDataIndexAndIncrement())] += avals[j];
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
		size += _data.getInMemorySize();
		return size;
	}

	@Override
	public AColGroup scalarOperation(ScalarOperator op) {
		return new ColGroupSDC(_colIndexes, _numRows, applyScalarOp(op), _indexes, _data, getCachedCounts());
	}

	@Override
	public AColGroup binaryRowOp(BinaryOperator op, double[] v, boolean sparseSafe, boolean left) {
		return new ColGroupSDC(_colIndexes, _numRows, applyBinaryRowOp(op, v, true, left), _indexes, _data,
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
		ret += _data.getExactSizeOnDisk();
		ret += _indexes.getExactSizeOnDisk();
		return ret;
	}

	@Override
	public boolean sameIndexStructure(ColGroupCompressed that) {
		// TODO add such that if the column group switched from Zeros type it also matches.
		return that instanceof ColGroupSDC && ((ColGroupSDC) that)._indexes == _indexes &&
			((ColGroupSDC) that)._data == _data;
	}

	@Override
	public int getIndexStructureHash() {
		return _data.hashCode() + _indexes.hashCode();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(String.format("\n%15s ", "Indexes: "));
		sb.append(_indexes.toString());
		sb.append(String.format("\n%15s ", "Data: "));
		sb.append(_data.toString());
		return sb.toString();
	}

	@Override
	public Dictionary preAggregateThatDDCStructure(ColGroupDDC that, Dictionary ret) {

		final AIterator it = _indexes.getIterator();
		final int offsetToDefault = this.getNumValues() - 1;
		final int nCol = that._colIndexes.length;

		int i = 0;

		for(; i < _numRows && it.hasNext(); i++) {
			int to = (it.value() == i) ? getIndex(it.getDataIndexAndIncrement()) : offsetToDefault;
			that._dict.addToEntry(ret, that._data.getIndex(i), to, nCol);
		}

		for(; i < _numRows; i++)
			that._dict.addToEntry(ret, that._data.getIndex(i), offsetToDefault, nCol);

		return ret;
	}

	@Override
	public Dictionary preAggregateThatSDCStructure(ColGroupSDC that, Dictionary ret, boolean preModified) {
		final AIterator itThat = that._indexes.getIterator();
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;
		// final int offsetToDefaultThat = that.getNumValues() - 1;
		final int offsetToDefaultThis = getNumValues() - 1;

		if(preModified) {
			while(itThat.hasNext() && itThis.hasNext()) {
				final int v = itThat.value();
				while(itThis.value() < v && itThis.hasNext())
					itThis.next();

				if(itThat.value() == itThis.value()) {
					final int fr = that.getIndex(itThat.getDataIndexAndIncrement());
					final int to = getIndex(itThis.getDataIndexAndIncrement());
					that._dict.addToEntry(ret, fr, to, nCol);
				}
				else if(itThat.value() < itThis.value()) {
					final int fr = that.getIndex(itThat.getDataIndexAndIncrement());
					that._dict.addToEntry(ret, fr, offsetToDefaultThis, nCol);
				}

			}

			while(itThat.hasNext()) {
				final int fr = that.getIndex(itThat.getDataIndexAndIncrement());
				that._dict.addToEntry(ret, fr, offsetToDefaultThis, nCol);
			}
		}
		else {
			throw new NotImplementedException();
			// int i = 0;

			// for(; i < _numRows && itThat.hasNext() && itThis.hasNext(); i++) {
			// final int to = (itThis.value() == i) ? getIndex(
			// itThis.getDataIndexAndIncrement()) : offsetToDefaultThis;
			// final int fr = (itThat.value() == i) ? that
			// .getIndex(itThat.getDataIndexAndIncrement()) : offsetToDefaultThat;
			// that._dict.addToEntry(ret, fr, to, nCol);
			// }

			// if(itThat.hasNext()) {
			// for(; i < _numRows && itThat.hasNext(); i++) {
			// int fr = (itThat.value() == i) ? that
			// .getIndex(itThat.getDataIndexAndIncrement()) : offsetToDefaultThat;
			// that._dict.addToEntry(ret, fr, offsetToDefaultThis, nCol);
			// }
			// }

			// if(itThis.hasNext()) {
			// for(; i < _numRows && itThis.hasNext(); i++) {
			// int to = (itThis.value() == i) ? getIndex(itThis.getDataIndexAndIncrement()) : offsetToDefaultThis;
			// that._dict.addToEntry(ret, offsetToDefaultThat, to, nCol);
			// }
			// }

			// for(; i < _numRows; i++)
			// that._dict.addToEntry(ret, offsetToDefaultThat, offsetToDefaultThis, nCol);
		}

		return ret;
	}

	@Override
	public Dictionary preAggregateThatSDCZerosStructure(ColGroupSDCZeros that, Dictionary ret) {

		final AIterator itThat = that._indexes.getIterator();
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;
		final int defThis = this.getNumValues() * nCol - nCol;

		while(itThat.hasNext()) {
			final int thatV = itThat.value();
			final int fr = that.getIndex(itThat.getDataIndexAndIncrement());
			if(thatV == itThis.skipTo(thatV)) {
				final int to = getIndex(itThis.getDataIndexAndIncrement());
				that._dict.addToEntry(ret, fr, to, nCol);
			}
			else
				that._dict.addToEntry(ret, fr, defThis, nCol);
		}
		return ret;

	}

	@Override
	public Dictionary preAggregateThatSDCSingleZerosStructure(ColGroupSDCSingleZeros that, Dictionary ret) {
		throw new NotImplementedException();
	}

	@Override
	public Dictionary preAggregateThatSDCSingleStructure(ColGroupSDCSingle that, Dictionary ret, boolean preModified) {
		final AIterator itThat = that._indexes.getIterator();
		final AIterator itThis = _indexes.getIterator();
		final int nCol = that._colIndexes.length;
		final int defThis = this.getNumValues() - 1;

		if(preModified) {
			while(itThat.hasNext()) {
				final int thatV = itThat.value();
				if(thatV == itThis.skipTo(thatV))
					that._dict.addToEntry(ret, 0, getIndex(itThis.getDataIndexAndIncrement()), nCol);
				else
					that._dict.addToEntry(ret, 0, defThis, nCol);
				itThat.next();
			}

			return ret;
		}
		else {
			throw new NotImplementedException();
		}
	}

	public ColGroupSDCZeros extractCommon(double[] constV) {
		double[] commonV = _dict.getTuple(getNumValues() - 1, _colIndexes.length);

		for(int i = 0; i < _colIndexes.length; i++) {
			constV[_colIndexes[i]] += commonV[i];
		}
		ADictionary subtractedDict = _dict.subtractTuple(commonV);
		return new ColGroupSDCZeros(_colIndexes, _numRows, subtractedDict, _indexes, _data, getCounts());
	}

}
