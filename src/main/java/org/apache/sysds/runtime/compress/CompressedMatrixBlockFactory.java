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

package org.apache.sysds.runtime.compress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.compress.cocode.CoCoderFactory;
import org.apache.sysds.runtime.compress.colgroup.AColGroup;
import org.apache.sysds.runtime.compress.colgroup.ColGroupConst;
import org.apache.sysds.runtime.compress.colgroup.ColGroupEmpty;
import org.apache.sysds.runtime.compress.colgroup.ColGroupFactory;
import org.apache.sysds.runtime.compress.colgroup.ColGroupValue;
import org.apache.sysds.runtime.compress.colgroup.dictionary.Dictionary;
import org.apache.sysds.runtime.compress.cost.CostEstimatorFactory;
import org.apache.sysds.runtime.compress.cost.ICostEstimate;
import org.apache.sysds.runtime.compress.cost.MemoryCostEstimator;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimatorFactory;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfo;
import org.apache.sysds.runtime.compress.utils.DblArrayIntListHashMap;
import org.apache.sysds.runtime.compress.utils.DoubleCountHashMap;
import org.apache.sysds.runtime.compress.workload.WTreeRoot;
import org.apache.sysds.runtime.controlprogram.parfor.stat.Timing;
import org.apache.sysds.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.utils.DMLCompressionStatistics;

/**
 * Factory pattern to compress a Matrix Block into a CompressedMatrixBlock.
 */
public class CompressedMatrixBlockFactory {

	private static final Log LOG = LogFactory.getLog(CompressedMatrixBlockFactory.class.getName());

	/** Timing object to measure the time of each phase in the compression */
	private Timing time = new Timing(true);
	/** Time stamp of last phase */
	private double lastPhase;
	/** Compression statistics gathered throughout the compression */
	private CompressionStatistics _stats = new CompressionStatistics();
	/** Pointer to the original matrix Block that is about to be compressed. */
	private MatrixBlock mb;
	/** Parallelization degree */
	private int k;
	/** Compression settings used for this compression */
	private CompressionSettings compSettings;
	/** The resulting compressed matrix */
	private CompressedMatrixBlock res;
	/** The current Phase ID */
	private int phase = 0;
	/** Compression information gathered through the sampling, used for the actual compression decided */
	private CompressedSizeInfo coCodeColGroups;
	/** The main cost estimator used for the compression */
	private ICostEstimate costEstimator;

	private CompressedMatrixBlockFactory(MatrixBlock mb, int k, CompressionSettings compSettings, WTreeRoot root) {
		this.mb = mb;
		this.k = k;
		this.compSettings = compSettings;
		costEstimator = CostEstimatorFactory.create(compSettings, root, mb.getNumRows(), mb.getNumColumns());
	}

	private CompressedMatrixBlockFactory(MatrixBlock mb, int k, CompressionSettings compSettings,
		ICostEstimate costEstimator) {
		this.mb = mb;
		this.k = k;
		this.compSettings = compSettings;
		this.costEstimator = costEstimator;
	}

	/**
	 * Default sequential compression with no parallelization
	 * 
	 * @param mb The matrixBlock to compress
	 * @return A Pair of a Matrix Block and Compression Statistics.
	 */
	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb) {
		return compress(mb, 1, new CompressionSettingsBuilder().create(), (WTreeRoot) null);
	}

	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb, WTreeRoot root) {
		return compress(mb, 1, new CompressionSettingsBuilder().create(), root);
	}

	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb,
		CompressionSettings customSettings) {
		return compress(mb, 1, customSettings, (WTreeRoot) null);
	}

	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb, int k) {
		return compress(mb, k, new CompressionSettingsBuilder().create(), (WTreeRoot) null);
	}

	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb, int k, WTreeRoot root) {
		return compress(mb, k, new CompressionSettingsBuilder().create(), root);
	}

	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb, int k,
		ICostEstimate costEstimator) {
		return compress(mb, k, new CompressionSettingsBuilder().create(), costEstimator);
	}

	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb, int k,
		CompressionSettings compSettings) {
		return compress(mb, k, compSettings, (WTreeRoot) null);
	}

	/**
	 * The main method for compressing the input matrix.
	 * 
	 * 
	 * @param mb           The matrix block to compress
	 * @param k            The number of threads used to execute the compression
	 * @param compSettings The Compression settings used
	 * @param root         The root instruction compressed, and used for calculating the computation cost of the
	 *                     compression
	 * @return A pair of an possibly compressed matrix block and compression statistics.
	 */
	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb, int k,
		CompressionSettings compSettings, WTreeRoot root) {
		CompressedMatrixBlockFactory cmbf = new CompressedMatrixBlockFactory(mb, k, compSettings, root);
		return cmbf.compressMatrix();
	}

	public static Pair<MatrixBlock, CompressionStatistics> compress(MatrixBlock mb, int k,
		CompressionSettings compSettings, ICostEstimate costEstimator) {
		CompressedMatrixBlockFactory cmbf = new CompressedMatrixBlockFactory(mb, k, compSettings, costEstimator);
		return cmbf.compressMatrix();
	}

	/**
	 * Method for constructing a compressed matrix out of an constant input.
	 * 
	 * Since the input is a constant value it is trivially compressable, therefore we skip the entire compression
	 * planning and directly return a compressed constant matrix
	 * 
	 * @param numRows The number of Rows in the matrix
	 * @param numCols The number of Columns in the matrix
	 * @param value   The value contained in the matrix
	 * @return The Compressed Constant matrix.
	 */
	public static CompressedMatrixBlock createConstant(int numRows, int numCols, double value) {
		CompressedMatrixBlock block = new CompressedMatrixBlock(numRows, numCols);
		AColGroup cg = ColGroupFactory.genColGroupConst(numRows, numCols, value);
		block.allocateColGroup(cg);
		block.recomputeNonZeros();
		return block;
	}

	private Pair<MatrixBlock, CompressionStatistics> compressMatrix() {
		// Check for redundant compression
		if(mb instanceof CompressedMatrixBlock) {
			LOG.info("MatrixBlock already compressed or is Empty");
			return new ImmutablePair<>(mb, null);
		}
		else if(mb.isEmpty()) {
			LOG.info("Empty input to compress, returning a compressed Matrix block with empty column group");
			CompressedMatrixBlock ret = new CompressedMatrixBlock(mb.getNumRows(), mb.getNumColumns());
			ColGroupEmpty cg = ColGroupEmpty.generate(mb.getNumColumns(), mb.getNumRows());
			ret.allocateColGroup(cg);
			ret.setNonZeros(0);
			return new ImmutablePair<>(ret, null);
		}

		_stats.denseSize = MatrixBlock.estimateSizeInMemory(mb.getNumRows(), mb.getNumColumns(), 1.0);
		_stats.originalSize = mb.getInMemorySize();

		res = new CompressedMatrixBlock(mb); // copy metadata and allocate soft reference

		classifyPhase();
		if(coCodeColGroups == null)
			return abortCompression();
		transposePhase();
		compressPhase();
		sharePhase();
		cleanupPhase();

		if(res == null)
			return abortCompression();
		final long oldNNZ = mb.getNonZeros();
		if(oldNNZ != -1 && oldNNZ != 0)
			res.setNonZeros(oldNNZ);
		else
			res.recomputeNonZeros();

		return new ImmutablePair<>(res, _stats);
	}

	private void classifyPhase() {
		CompressedSizeEstimator sizeEstimator = CompressedSizeEstimatorFactory.getSizeEstimator(mb, compSettings, k);
		CompressedSizeInfo sizeInfos = sizeEstimator.computeCompressedSizeInfos(k);
		_stats.estimatedSizeCols = sizeInfos.memoryEstimate();
		logPhase();

		if(!(costEstimator instanceof MemoryCostEstimator) || _stats.estimatedSizeCols < _stats.originalSize)
			coCodePhase(sizeEstimator, sizeInfos, costEstimator);
		else {
			LOG.info("Estimated Size of singleColGroups: " + _stats.estimatedSizeCols);
			LOG.info("Original size                    : " + _stats.originalSize);
		}
	}

	private void coCodePhase(CompressedSizeEstimator sizeEstimator, CompressedSizeInfo sizeInfos,
		ICostEstimate costEstimator) {
		coCodeColGroups = CoCoderFactory.findCoCodesByPartitioning(sizeEstimator, sizeInfos, k, costEstimator,
			compSettings);

		_stats.estimatedSizeCoCoded = coCodeColGroups.memoryEstimate();
		logPhase();
	}

	private void transposePhase() {
		boolean sparse = mb.isInSparseFormat();
		transposeHeuristics();
		if(compSettings.transposed)
			mb = LibMatrixReorg.transpose(mb, new MatrixBlock(mb.getNumColumns(), mb.getNumRows(), sparse), k, true);

		logPhase();
	}

	private void transposeHeuristics() {
		switch(compSettings.transposeInput) {
			case "true":
				compSettings.transposed = true;
				break;
			case "false":
				compSettings.transposed = false;
				break;
			default:
				if(mb.isInSparseFormat()) {
					// compSettings.transposed = true;
					// compSettings.transposed = false;
					boolean isAboveRowNumbers = mb.getNumRows() > 500000;
					boolean isAboveThreadToColumnRatio = coCodeColGroups.getNumberColGroups() > mb.getNumColumns() / 4;
					compSettings.transposed = isAboveRowNumbers && isAboveThreadToColumnRatio;
				}
				else
					compSettings.transposed = false;
		}
	}

	private void compressPhase() {
		res.allocateColGroupList(ColGroupFactory.compressColGroups(mb, coCodeColGroups, compSettings, k));
		_stats.compressedInitialSize = res.getInMemorySize();
		logPhase();
	}

	private void sharePhase() {
		// Combine Constant type column groups, both empty and const.
		List<AColGroup> e = new ArrayList<>();
		List<AColGroup> c = new ArrayList<>();
		List<AColGroup> o = new ArrayList<>();
		for(AColGroup g : res.getColGroups()) {
			if(g instanceof ColGroupEmpty)
				e.add(g);
			else if(g instanceof ColGroupConst)
				c.add(g);
			else
				o.add(g);
		}

		if(!e.isEmpty())
			o.add(combineEmpty(e));
		if(!c.isEmpty())
			o.add(combineConst(c));

		res.allocateColGroupList(o);

		logPhase();
	}

	private static AColGroup combineEmpty(List<AColGroup> e) {
		return new ColGroupEmpty(combineColIndexes(e), e.get(0).getNumRows());
	}

	private static AColGroup combineConst(List<AColGroup> c) {
		int[] resCols = combineColIndexes(c);

		double[] values = new double[resCols.length];

		for(int i = 0; i < resCols.length; i++) {
			for(AColGroup g : c) {
				ColGroupConst cg = (ColGroupConst) g;
				int[] cols = cg.getColIndices();
				int index = Arrays.binarySearch(cols, resCols[i]);
				if(index >= 0) {
					values[i] = cg.getDictionary().getValue(index);
					break;
				}
			}
		}
		Dictionary dict = new Dictionary(values);

		return new ColGroupConst(resCols, c.get(0).getNumRows(), dict);
	}

	private static int[] combineColIndexes(List<AColGroup> gs) {
		int numCols = 0;
		for(AColGroup g : gs)
			numCols += g.getNumCols();

		int[] resCols = new int[numCols];

		int index = 0;
		for(AColGroup g : gs)
			for(int c : g.getColIndices())
				resCols[index++] = c;

		Arrays.sort(resCols);
		return resCols;
	}

	private void cleanupPhase() {

		res.cleanupBlock(true, true);

		_stats.size = res.estimateCompressedSizeInMemory();

		final double ratio = _stats.getRatio();

		if(ratio < 1) {
			LOG.info("--dense size:        " + _stats.denseSize);
			LOG.info("--original size:     " + _stats.originalSize);
			LOG.info("--compressed size:   " + _stats.size);
			LOG.info("--compression ratio: " + ratio);
			LOG.info("Abort block compression because compression ratio is less than 1.");
			res = null;
			setNextTimePhase(time.stop());
			DMLCompressionStatistics.addCompressionTime(getLastTimePhase(), phase);
			return;
		}

		_stats.setColGroupsCounts(res.getColGroups());

	
		logPhase();

	}

	private Pair<MatrixBlock, CompressionStatistics> abortCompression() {
		LOG.warn("Compression aborted at phase: " + phase);

		if(compSettings.transposed)
			LibMatrixReorg.transposeInPlace(mb, k);

		return new ImmutablePair<>(mb, _stats);
	}

	private void logPhase() {
		setNextTimePhase(time.stop());
		DMLCompressionStatistics.addCompressionTime(getLastTimePhase(), phase);
		if(LOG.isDebugEnabled()) {
			switch(phase) {
				case 0:
					LOG.debug("--compression phase " + phase + " Classify  : " + getLastTimePhase());
					LOG.debug("--Individual Columns Estimated Compression: " + _stats.estimatedSizeCols);
					break;
				case 1:
					LOG.debug("--compression phase " + phase + " Grouping  : " + getLastTimePhase());
					LOG.debug("Grouping using: " + compSettings.columnPartitioner);
					LOG.debug("--Cocoded Columns estimated Compression:" + _stats.estimatedSizeCoCoded);
					break;
				case 2:
					LOG.debug("--compression phase " + phase + " Transpose : " + getLastTimePhase());
					LOG.debug("Did transpose: " + compSettings.transposed);
					break;
				case 3:
					LOG.debug("--compression phase " + phase + " Compress  : " + getLastTimePhase());
					LOG.debug("--compression Hash collisions:" + "(" +DblArrayIntListHashMap.hashMissCount+","+DoubleCountHashMap.hashMissCount+")");
					DblArrayIntListHashMap.hashMissCount = 0;
					DoubleCountHashMap.hashMissCount = 0;
					LOG.debug("--compressed initial actual size:" + _stats.compressedInitialSize);
					break;
				case 4:
					LOG.debug("--compression phase " + phase + " Share     : " + getLastTimePhase());
					break;
				case 5:
					LOG.debug("--num col groups: " + res.getColGroups().size());
					LOG.debug("--compression phase " + phase + " Cleanup   : " + getLastTimePhase());
					LOG.debug("--col groups types " + _stats.getGroupsTypesString());
					LOG.debug("--col groups sizes " + _stats.getGroupsSizesString());
					LOG.debug("--dense size:        " + _stats.denseSize);
					LOG.debug("--original size:     " + _stats.originalSize);
					LOG.debug("--compressed size:   " + _stats.size);
					LOG.debug("--compression ratio: " + _stats.getRatio());
					int[] lengths = new int[res.getColGroups().size()];
					int i = 0;
					for(AColGroup colGroup : res.getColGroups())
						lengths[i++] = colGroup.getNumValues();

					LOG.debug("--compressed colGroup dictionary sizes: " + Arrays.toString(lengths));
					if(LOG.isTraceEnabled()) {
						for(AColGroup colGroup : res.getColGroups()) {
							LOG.trace("--colGroups type       : " + colGroup.getClass().getSimpleName() + " size: "
								+ colGroup.estimateInMemorySize()
								+ ((colGroup instanceof ColGroupValue) ? "  numValues :"
									+ ((ColGroupValue) colGroup).getNumValues() : "")
								+ "  colIndexes : " + Arrays.toString(colGroup.getColIndices()));
						}
					}
				default:
			}
		}
		phase++;
	}

	public void setNextTimePhase(double time) {
		lastPhase = time;
	}

	public double getLastTimePhase() {
		return lastPhase;
	}

}
