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

package org.apache.sysds.runtime.compress.estim;

import java.util.HashMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.compress.colgroup.mapping.AMapToData;
import org.apache.sysds.runtime.compress.colgroup.mapping.MapToFactory;
import org.apache.sysds.runtime.compress.estim.sample.SampleEstimatorFactory;
import org.apache.sysds.runtime.compress.lib.BitmapEncoder;
import org.apache.sysds.runtime.compress.utils.ABitmap;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.data.SparseBlockMCSR;
import org.apache.sysds.runtime.data.SparseRow;
import org.apache.sysds.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.UtilFunctions;

public class CompressedSizeEstimatorSample extends CompressedSizeEstimator {

	private int[] _sampleRows;
	private MatrixBlock _sample;
	private HashMap<Integer, Double> _solveCache = null;
	private final int _k;

	/**
	 * CompressedSizeEstimatorSample, samples from the input data and estimates the size of the compressed matrix.
	 * 
	 * @param data       The input data toSample from
	 * @param cs         The Settings used for the sampling, and compression, contains information such as seed.
	 * @param sampleSize The size to sample from the data.
	 * @param k          The parallelization degree allowed.
	 */
	public CompressedSizeEstimatorSample(MatrixBlock data, CompressionSettings cs, int sampleSize, int k) {
		super(data, cs);
		_k = k;
		_sample = sampleData(sampleSize);
	}

	public MatrixBlock getSample() {
		return _sample;
	}

	public MatrixBlock sampleData(int sampleSize) {
		_sampleRows = CompressedSizeEstimatorSample.getSortedUniformSample(_numRows, sampleSize, _cs.seed);
		_solveCache = new HashMap<>();
		MatrixBlock sampledMatrixBlock;
		if(_data.isInSparseFormat() && !_cs.transposed) {
			sampledMatrixBlock = new MatrixBlock(_sampleRows.length, _data.getNumColumns(), true);
			SparseRow[] rows = new SparseRow[_sampleRows.length];
			SparseBlock in = _data.getSparseBlock();
			for(int i = 0; i < _sampleRows.length; i++)
				rows[i] = in.get(_sampleRows[i]);

			sampledMatrixBlock.setSparseBlock(new SparseBlockMCSR(rows, false));
			sampledMatrixBlock.recomputeNonZeros();
			_transposed = true;
			sampledMatrixBlock = LibMatrixReorg.transposeInPlace(sampledMatrixBlock, _k);
		}
		else {
			MatrixBlock select = (_cs.transposed) ? new MatrixBlock(_data.getNumColumns(), 1,
				false) : new MatrixBlock(_data.getNumRows(), 1, false);
			for(int i = 0; i < _sampleRows.length; i++)
				select.appendValue(_sampleRows[i], 0, 1);

			sampledMatrixBlock = _data.removeEmptyOperations(new MatrixBlock(), !_cs.transposed, true, select);
		}

		if(sampledMatrixBlock.isEmpty())
			return null;
		else
			return sampledMatrixBlock;

	}

	@Override
	public CompressedSizeInfoColGroup estimateCompressedColGroupSize(int[] colIndexes, int nrUniqueUpperBound) {

		// extract statistics from sample
		final ABitmap ubm = BitmapEncoder.extractBitmap(colIndexes, _sample, _transposed, nrUniqueUpperBound / 2);
		final EstimationFactors sampleFacts = EstimationFactors.computeSizeEstimationFactors(ubm, false, colIndexes);
		final AMapToData map = MapToFactory.create(ubm);

		// result facts
		EstimationFactors em = estimateCompressionFactors(sampleFacts, map, colIndexes, nrUniqueUpperBound);
		return new CompressedSizeInfoColGroup(em, _cs.validCompressions, map);
	}

	@Override
	public CompressedSizeInfoColGroup estimateJoinCompressedSize(int[] joined, CompressedSizeInfoColGroup g1,
		CompressedSizeInfoColGroup g2) {
		final int g1V = g1.getMap().getUnique();
		final int g2V = g2.getMap().getUnique();
		final int nrUniqueUpperBound = g1V * g2V;

		final AMapToData map = MapToFactory.join(g1.getMap(), g2.getMap());
		EstimationFactors sampleFacts = EstimationFactors.computeSizeEstimation(joined, map,
			_cs.validCompressions.contains(CompressionType.RLE), map.size(), false);

		// result facts
		EstimationFactors em = estimateCompressionFactors(sampleFacts, map, joined, nrUniqueUpperBound);
		return new CompressedSizeInfoColGroup(em, _cs.validCompressions, map);
	}

	private EstimationFactors estimateCompressionFactors(EstimationFactors sampleFacts, AMapToData map,
		int[] colIndexes, int nrUniqueUpperBound) {
		final int numZerosInSample = sampleFacts.numRows - sampleFacts.numOffs;
		final int sampleSize = _sampleRows.length;

		if(numZerosInSample == sampleSize) {
			final int nCol = sampleFacts.cols.length;
			/**
			 * Since we sample, and this column seems to be empty we set the return to 1 value detected. aka 1 value,
			 * and 1 offset. This makes it more robust in the coCoding of Columns
			 */
			final int largestInstanceCount = _numRows - 1;
			return new EstimationFactors(colIndexes, 1, 1, largestInstanceCount, new int[] {largestInstanceCount}, 2, 1,
				_numRows, sampleFacts.lossy, true, (double) 1 / _numRows, (double) 1 / nCol);
		}
		else {

			final double scalingFactor = ((double) _numRows / sampleSize);
			// Estimate number of distinct values (incl fixes for anomalies w/ large sample fraction)
			final int totalCardinality = Math.max(map.getUnique(),
				Math.min(_numRows, getEstimatedDistinctCount(sampleFacts.frequencies, nrUniqueUpperBound)));

			// estimate number of non-zeros (conservatively round up)
			final double C = Math.max(1 - (double) sampleFacts.numSingle / sampleSize, (double) sampleSize / _numRows);
			final int numNonZeros = Math.max((int) Math.floor(_numRows - scalingFactor * C * numZerosInSample),
				totalCardinality);

			final int totalNumRuns = getNumRuns(map, sampleFacts.numVals, sampleSize, _numRows, _sampleRows);

			final int largestInstanceCount = Math.min(_numRows,
				(int) Math.floor(sampleFacts.largestOff * scalingFactor));

			return new EstimationFactors(colIndexes, totalCardinality, numNonZeros, largestInstanceCount,
				sampleFacts.frequencies, totalNumRuns, sampleFacts.numSingle, _numRows, sampleFacts.lossy,
				sampleFacts.zeroIsMostFrequent, sampleFacts.overAllSparsity, sampleFacts.tupleSparsity);
		}
	}

	private int getEstimatedDistinctCount(int[] frequencies, int upperBound) {
		return Math.min(SampleEstimatorFactory.distinctCount(frequencies, _numRows, _sampleRows.length,
			_cs.estimationType, _solveCache), upperBound);
	}

	private int getNumRuns(AMapToData map, int numVals, int sampleSize, int totalNumRows, int[] sampleRows) {
		// estimate number of segments and number of runs incl correction for
		// empty segments and empty runs (via expected mean of offset value)
		// int numUnseenSeg = (int) (unseenVals * Math.ceil((double) _numRows / BitmapEncoder.BITMAP_BLOCK_SZ / 2));
		return _cs.validCompressions.contains(CompressionType.RLE) && numVals > 0 ? getNumRuns(map, sampleSize,
			_numRows, _sampleRows) : 0;
	}

	// Fix getNumRuns when adding RLE back.
	@SuppressWarnings("unused")
	private static int getNumRuns(ABitmap ubm, int sampleSize, int totalNumRows, int[] sampleRows) {
		int numVals = ubm.getNumValues();
		double numRuns = 0;
		for(int vi = 0; vi < numVals; vi++) {
			int[] offsets = ubm.getOffsetsList(vi).extractValues();
			int offsetsSize = ubm.getNumOffsets(vi);
			double offsetsRatio = ((double) offsetsSize) / sampleSize;
			double avgAdditionalOffsets = offsetsRatio * totalNumRows / sampleSize;
			if(avgAdditionalOffsets < 1) {
				// Ising-Stevens does not hold
				// fall-back to using the expected number of offsets as an upper
				// bound on the number of runs
				numRuns += ((double) offsetsSize) * totalNumRows / sampleSize;
				continue;
			}
			int intervalEnd, intervalSize;
			double additionalOffsets;
			// probability of an index being non-offset in current and previous
			// interval respectively
			double nonOffsetProb, prevNonOffsetProb = 1;
			boolean reachedSampleEnd = false;
			// handling the first interval separately for simplicity
			int intervalStart = -1;
			if(sampleRows[0] == 0) {
				// empty interval
				intervalStart = 0;
			}
			else {
				intervalEnd = sampleRows[0];
				intervalSize = intervalEnd - intervalStart - 1;
				// expected value of a multivariate hypergeometric distribution
				additionalOffsets = offsetsRatio * intervalSize;
				// expected value of an Ising-Stevens distribution
				numRuns += (intervalSize - additionalOffsets) * additionalOffsets / intervalSize;
				intervalStart = intervalEnd;
				prevNonOffsetProb = (intervalSize - additionalOffsets) / intervalSize;
			}
			// for handling separators

			int withinSepRun = 0;
			boolean seenNonOffset = false, startedWithOffset = false, endedWithOffset = false;
			int offsetsPtrs = 0;
			for(int ix = 1; ix < sampleSize; ix++) {
				// start of a new separator
				// intervalStart will always be pointing at the current value
				// in the separator block

				if(offsetsPtrs < offsetsSize && offsets[offsetsPtrs] == intervalStart) {
					startedWithOffset = true;
					offsetsPtrs++;
					endedWithOffset = true;
				}
				else {
					seenNonOffset = true;
					endedWithOffset = false;
				}
				while(intervalStart + 1 == sampleRows[ix]) {
					intervalStart = sampleRows[ix];
					if(seenNonOffset) {
						if(offsetsPtrs < offsetsSize && offsets[offsetsPtrs] == intervalStart) {
							withinSepRun = 1;
							offsetsPtrs++;
							endedWithOffset = true;
						}
						else {
							numRuns += withinSepRun;
							withinSepRun = 0;
							endedWithOffset = false;
						}
					}
					else if(offsetsPtrs < offsetsSize && offsets[offsetsPtrs] == intervalStart) {
						offsetsPtrs++;
						endedWithOffset = true;
					}
					else {
						seenNonOffset = true;
						endedWithOffset = false;
					}
					//
					ix++;
					if(ix == sampleSize) {
						// end of sample which searching for a start
						reachedSampleEnd = true;
						break;
					}
				}

				// runs within an interval of unknowns
				if(reachedSampleEnd)
					break;
				intervalEnd = sampleRows[ix];
				intervalSize = intervalEnd - intervalStart - 1;
				// expected value of a multivariate hypergeometric distribution
				additionalOffsets = offsetsRatio * intervalSize;
				// expected value of an Ising-Stevens distribution
				numRuns += (intervalSize - additionalOffsets) * additionalOffsets / intervalSize;
				nonOffsetProb = (intervalSize - additionalOffsets) / intervalSize;

				// additional runs resulting from x's on the boundaries of the
				// separators
				// endedWithOffset = findInArray(offsets, intervalStart) != -1;
				if(seenNonOffset) {
					if(startedWithOffset) {
						// add p(y in the previous interval)
						numRuns += prevNonOffsetProb;
					}
					if(endedWithOffset) {
						// add p(y in the current interval)
						numRuns += nonOffsetProb;
					}
				}
				else {
					// add p(y in the previous interval and y in the current
					// interval)
					numRuns += prevNonOffsetProb * nonOffsetProb;
				}
				prevNonOffsetProb = nonOffsetProb;
				intervalStart = intervalEnd;
				// reseting separator variables
				seenNonOffset = startedWithOffset = endedWithOffset = false;
				withinSepRun = 0;

			}
			// last possible interval
			if(intervalStart != totalNumRows - 1) {
				intervalEnd = totalNumRows;
				intervalSize = intervalEnd - intervalStart - 1;
				// expected value of a multivariate hypergeometric distribution
				additionalOffsets = offsetsRatio * intervalSize;
				// expected value of an Ising-Stevens distribution
				numRuns += (intervalSize - additionalOffsets) * additionalOffsets / intervalSize;
				nonOffsetProb = (intervalSize - additionalOffsets) / intervalSize;
			}
			else {
				nonOffsetProb = 1;
			}
			// additional runs resulting from x's on the boundaries of the
			// separators
			endedWithOffset = intervalStart == offsets[offsetsSize - 1];
			if(seenNonOffset) {
				if(startedWithOffset) {
					numRuns += prevNonOffsetProb;
				}
				if(endedWithOffset) {
					// add p(y in the current interval)
					numRuns += nonOffsetProb;
				}
			}
			else {
				if(endedWithOffset)
					// add p(y in the previous interval and y in the current
					// interval)
					numRuns += prevNonOffsetProb * nonOffsetProb;
			}
		}
		return (int) Math.min(Math.round(numRuns), Integer.MAX_VALUE);
	}

	private static int getNumRuns(AMapToData map, int sampleSize, int totalNumRows, int[] sampleRows) {

		throw new NotImplementedException("Not Supported ever since the ubm was replaced by the map");
	}

	/**
	 * Returns a sorted array of n integers, drawn uniformly from the range [0,range).
	 * 
	 * @param range      the range
	 * @param sampleSize sample size
	 * @return sorted array of integers
	 */
	private static int[] getSortedUniformSample(int range, int sampleSize, long seed) {
		return UtilFunctions.getSortedSampleIndexes(range, sampleSize, seed);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(" sampleSize: ");
		sb.append(_sampleRows.length);
		sb.append(" transposed: ");
		sb.append(_transposed);
		sb.append(" cols: ");
		sb.append(_numCols);
		sb.append(" rows: ");
		sb.append(_numRows);
		return sb.toString();
	}
}
