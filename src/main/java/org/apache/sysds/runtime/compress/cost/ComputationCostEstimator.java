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

package org.apache.sysds.runtime.compress.cost;

import java.util.Collection;

import org.apache.commons.lang.NotImplementedException;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfoColGroup;

public class ComputationCostEstimator implements ICostEstimate {

	private static final long serialVersionUID = -1205636215389161815L;

	private final boolean _isCompareAll;
	
	private final int _nRows;
	// private final int _nColsInMatrix;

	// Iteration through each row of decompressed.
	private final int _scans;
	private final int _decompressions;
	private final int _overlappingDecompressions;
	// The number of left (rows) multiplied on the matrix.
	private final int _leftMultiplications;
	private final int _rightMultiplications;
	private final int _compressedMultiplication;
	// private final int _rowBasedOps;
	private final int _dictionaryOps;

	/**
	 * A Cost based estimator based on the WTree that is parsed in IPA.
	 * 
	 * @param tree The tree to estimate cost from.
	 */
	protected ComputationCostEstimator(int nRows, int nCols, boolean compareAll, InstructionTypeCounter counts) {
		_nRows = nRows;
		// _nColsInMatrix = nCols;
		_isCompareAll = compareAll;
		_scans = counts.scans;
		_decompressions = counts.decompressions;
		_overlappingDecompressions = counts.overlappingDecompressions;
		_leftMultiplications = counts.leftMultiplications;
		_compressedMultiplication = counts.compressedMultiplications;
		_rightMultiplications = counts.rightMultiplications;
		_dictionaryOps = counts.dictionaryOps;
		// _rowBasedOps = counts.rowBasedOps;
		if(LOG.isDebugEnabled())
			LOG.debug(this);
	}

	@Override
	public double getUncompressedCost(int nRows, int nCols, int sparsity) {
		throw new NotImplementedException();
	}

	@Override
	public double getCostOfColumnGroup(CompressedSizeInfoColGroup g) {
		if(g == null)
			return Double.POSITIVE_INFINITY;
		double cost = 0;
		cost += _scans * scanCost(g);
		cost += _decompressions * decompressionCost(g);
		cost += _overlappingDecompressions * overlappingDecompressionCost(g);
		// 16 is assuming that the left side is 16 rows.
		cost += _leftMultiplications * leftMultCost(g) * 16;
		// 16 is assuming that the right side is 16 rows.
		cost += _rightMultiplications * rightMultCost(g) * 16;
		cost += _dictionaryOps * dictionaryOpsCost(g);
		return cost;
	}

	private double scanCost(CompressedSizeInfoColGroup g) {
		return _nRows;
	}

	private double leftMultCost(CompressedSizeInfoColGroup g) {
		final int nCols = g.getColumns().length;
		// final double preAggregateCost = _nRows * 2.5;
		final double preAggregateCost = _nRows * 1.5;
		// final double preAggregateCost = _nRows * 0.2;

		final int numberTuples = g.getNumVals();
		final double tupleSparsity = g.getTupleSparsity();
		final double postScalingCost = (nCols > 1 && tupleSparsity > 0.4) ? numberTuples * nCols : numberTuples *
			nCols * tupleSparsity;
		if(numberTuples > 64000)
			return preAggregateCost + postScalingCost * 2;
			
		return preAggregateCost + postScalingCost;
	}

	private static double rightMultCost(CompressedSizeInfoColGroup g) {
		final int nCols = g.getColumns().length;
		final int numberTuples = g.getNumVals() * 10;
		final double tupleSparsity = g.getTupleSparsity();
		final double postScalingCost = (nCols > 1 && tupleSparsity > 0.4) ? numberTuples * nCols : numberTuples *
			nCols * tupleSparsity;

		return postScalingCost;
	}

	private double decompressionCost(CompressedSizeInfoColGroup g) {
		return _nRows * g.getColumns().length * (g.getNumVals() / 64000 + 1);
	}

	private double overlappingDecompressionCost(CompressedSizeInfoColGroup g) {
		// final int nVal = g.getNumVals();
		// return nVal < 512 ? _nRows : _nRows * _nColsInMatrix * (nVal / 64000 + 1);
		return  _nRows * 16 * (g.getNumVals() / 64000 + 1);
	}

	private static double dictionaryOpsCost(CompressedSizeInfoColGroup g) {
		return g.getColumns().length * g.getNumVals();
	}

	@Override
	public double getCostOfCollectionOfGroups(Collection<CompressedSizeInfoColGroup> gs) {
		double cost = 0;
		for(CompressedSizeInfoColGroup g1 : gs) {
			cost += getCostOfColumnGroup(g1);
			for(CompressedSizeInfoColGroup g2 : gs)
				cost += getCostOfTwoGroups(g1, g2);
		}
		return cost;
	}

	@Override
	public double getCostOfCollectionOfGroups(Collection<CompressedSizeInfoColGroup> gs, CompressedSizeInfoColGroup g) {
		double cost = getCostOfColumnGroup(g);
		for(CompressedSizeInfoColGroup g1 : gs) {
			cost += getCostOfColumnGroup(g1);
			cost += getCostOfTwoGroups(g1, g);
			for(CompressedSizeInfoColGroup g2 : gs)
				cost += getCostOfTwoGroups(g1, g2);
		}
		return cost;
	}

	@Override
	public double getCostOfTwoGroups(CompressedSizeInfoColGroup g1, CompressedSizeInfoColGroup g2) {
		return getCostOfCompressedMultiplication(g1, g2);
	}

	private double getCostOfCompressedMultiplication(CompressedSizeInfoColGroup g1, CompressedSizeInfoColGroup g2) {
		if(g1 == g2)
			return getCostOfSelfMultiplication(g1);

		final int nColsL = g1.getColumns().length;
		final int nColsR = g1.getColumns().length;

		// final double preAggLeft = (nRows / (1 - gl.getMostCommonFraction())) * nColsL;
		// final double preAggRight = (nRows / (1 - gr.getMostCommonFraction())) * nColsR;

		final double preAggLeft = _nRows;
		final double preAggRight = _nRows;

		final double tsL = g1.getTupleSparsity();
		final double tsR = g2.getTupleSparsity();

		// final double tsL = 1;
		// final double tsR = 1;

		final int nvL = g1.getNumVals();
		final int nvR = g2.getNumVals();

		final double postScaleLeft = nColsL > 1 && tsL > 0.4 ? nvL * nColsL : nvL * nColsL * tsL;
		final double postScaleRight = nColsR > 1 && tsR > 0.4 ? nvR * nColsR : nvR * nColsR * tsR;

		final double costLeft = preAggLeft + postScaleLeft * 5;
		final double costRight = preAggRight + postScaleRight * 5;

		return Math.min(costLeft, costRight);
	}

	private static double getCostOfSelfMultiplication(CompressedSizeInfoColGroup g) {
		final int nv = g.getNumVals();
		final int nCols = g.getColumns().length;
		final double ts = g.getTupleSparsity();
		return nv * nCols * ts;
	}

	@Override
	public boolean isCompareAll() {
		return _isCompareAll;
	}

	@Override
	public boolean shouldAnalyze(CompressedSizeInfoColGroup g1, CompressedSizeInfoColGroup g2) {
		return true;
	}

	@Override
	public boolean shouldTryJoin(CompressedSizeInfoColGroup g1, CompressedSizeInfoColGroup g2) {
		return true;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getSimpleName());
		sb.append("\n");
		sb.append(_nRows + "  ");
		sb.append(_scans + " ");
		sb.append(_decompressions + " ");
		sb.append(_overlappingDecompressions + " ");
		sb.append(_leftMultiplications + " ");
		sb.append(_rightMultiplications + " ");
		sb.append(_compressedMultiplication + " ");
		sb.append(_dictionaryOps + " ");
		return sb.toString();
	}

}
