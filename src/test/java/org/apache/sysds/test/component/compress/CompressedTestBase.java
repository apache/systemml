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

package org.apache.sysds.test.component.compress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.lops.MMTSJ.MMTSJType;
import org.apache.sysds.lops.MapMultChain.ChainType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.compress.CompressedMatrixBlockFactory;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.CompressionSettingsBuilder;
import org.apache.sysds.runtime.compress.CompressionStatistics;
import org.apache.sysds.runtime.compress.colgroup.AColGroup;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.compress.colgroup.ColGroupFactory;
import org.apache.sysds.runtime.compress.colgroup.ColGroupUncompressed;
import org.apache.sysds.runtime.compress.cost.CostEstimatorFactory.CostType;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfoColGroup;
import org.apache.sysds.runtime.compress.estim.EstimationFactors;
import org.apache.sysds.runtime.compress.lib.BitmapEncoder;
import org.apache.sysds.runtime.compress.utils.ABitmap;
import org.apache.sysds.runtime.functionobjects.Divide;
import org.apache.sysds.runtime.functionobjects.Equals;
import org.apache.sysds.runtime.functionobjects.GreaterThan;
import org.apache.sysds.runtime.functionobjects.GreaterThanEquals;
import org.apache.sysds.runtime.functionobjects.LessThan;
import org.apache.sysds.runtime.functionobjects.LessThanEquals;
import org.apache.sysds.runtime.functionobjects.Minus;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.functionobjects.Power2;
import org.apache.sysds.runtime.functionobjects.SwapIndex;
import org.apache.sysds.runtime.functionobjects.ValueFunction;
import org.apache.sysds.runtime.functionobjects.Xor;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.LeftScalarOperator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.matrix.operators.ScalarOperator;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestUtils;
import org.apache.sysds.test.component.compress.TestConstants.MatrixTypology;
import org.apache.sysds.test.component.compress.TestConstants.OverLapping;
import org.apache.sysds.test.component.compress.TestConstants.SparsityType;
import org.apache.sysds.test.component.compress.TestConstants.ValueRange;
import org.apache.sysds.test.component.compress.TestConstants.ValueType;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public abstract class CompressedTestBase extends TestBase {
	protected static final Log LOG = LogFactory.getLog(CompressedTestBase.class.getName());

	protected static SparsityType[] usedSparsityTypes = new SparsityType[] {SparsityType.FULL, SparsityType.SPARSE,
		SparsityType.ULTRA_SPARSE};

	protected static ValueType[] usedValueTypes = new ValueType[] {ValueType.RAND_ROUND, ValueType.OLE_COMPRESSIBLE,
		ValueType.RLE_COMPRESSIBLE};

	protected static ValueRange[] usedValueRanges = new ValueRange[] {ValueRange.BOOLEAN, ValueRange.SMALL,
		ValueRange.NEGATIVE};

	protected static OverLapping[] overLapping = new OverLapping[] {
		// OverLapping.COL,
		OverLapping.PLUS,
		// OverLapping.MATRIX,
		OverLapping.NONE, OverLapping.APPEND_CONST, OverLapping.APPEND_EMPTY
		// OverLapping.MATRIX_PLUS,
		// OverLapping.SQUASH,
		// OverLapping.MATRIX_MULT_NEGATIVE
	};

	protected static CompressionType[] forcedColGroups = new CompressionType[] {CompressionType.DDC,
		CompressionType.SDC, CompressionType.UNCOMPRESSED};

	private static final int compressionSeed = 7;

	protected static CompressionSettingsBuilder[] usedCompressionSettings = new CompressionSettingsBuilder[] {
		// CLA TESTS!

		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.DDC)),

		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.OLE)),
		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.RLE)),

		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.SDC)),

		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.SDC, CompressionType.DDC)),
		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.OLE, CompressionType.SDC, CompressionType.DDC)),

		new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed).setTransposeInput("false"),

		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed).setTransposeInput("true")
		// .setColumnPartitioner(PartitionerType.BIN_PACKING),
		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed).setTransposeInput("true")
		// .setColumnPartitioner(PartitionerType.STATIC),

		new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed).setTransposeInput("true")
			.setCostType(CostType.LEFT_MATRIX_MULT),

		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed),
		// new CompressionSettingsBuilder().setSamplingRatio(1.0).setSeed(compressionSeed)
		// .setAllowSharedDictionary(false).setmaxStaticColGroupCoCode(1),

		// LOSSY TESTS!

		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.DDC)).setLossy(true).create(),
		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.OLE)).setLossy(true).create(),
		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .setValidCompressions(EnumSet.of(CompressionType.RLE)).setLossy(true).create(),
		// new CompressionSettingsBuilder().setSamplingRatio(0.1).setSeed(compressionSeed)
		// .create(),
		// new CompressionSettingsBuilder().setSamplingRatio(1.0).setSeed(compressionSeed)
		// .setAllowSharedDictionary(false).setmaxStaticColGroupCoCode(1).setLossy(true).create(),

		// CO CODING TESTS!!

		// new CompressionSettingsBuilder().setSamplingRatio(1.0).setSeed(compressionSeed)
		// .setAllowSharedDDCDictionary(false).setmaxStaticColGroupCoCode(20).create(),
		// new CompressionSettingsBuilder().setSamplingRatio(1.0).setSeed(compressionSeed)
		// .setAllowSharedDDCDictionary(false).setmaxStaticColGroupCoCode(20).setLossy(true).create()

		// SHARED DICTIONARY TESTS!!

	};

	protected static MatrixTypology[] usedMatrixTypology = new MatrixTypology[] { // Selected Matrix Types
		// MatrixTypology.SMALL,
		MatrixTypology.FEW_COL,
		// MatrixTypology.FEW_ROW,
		MatrixTypology.LARGE,
		// // MatrixTypology.SINGLE_COL,
		// MatrixTypology.SINGLE_ROW,
		// MatrixTypology.L_ROWS,
		// MatrixTypology.XL_ROWS,
		// MatrixTypology.SINGLE_COL_L
	};

	// Compressed Block
	protected MatrixBlock cmb;
	protected CompressionStatistics cmbStats;

	// Decompressed Result

	/** number of threads used for the operation */
	protected final int _k;

	protected int sampleTolerance = 4096 * 4;

	protected double lossyTolerance;

	public CompressedTestBase(SparsityType sparType, ValueType valType, ValueRange valueRange,
		CompressionSettingsBuilder compSettings, MatrixTypology MatrixTypology, OverLapping ov, int parallelism,
		Collection<CompressionType> ct) {
		super(sparType, valType, valueRange, compSettings, MatrixTypology, ov, ct);

		_k = parallelism;

		try {
			if(_cs == null && ct == null) {
				Pair<MatrixBlock, CompressionStatistics> pair = (_k == 1) ? CompressedMatrixBlockFactory
					.compress(mb) : CompressedMatrixBlockFactory.compress(mb, _k);
				cmb = pair.getLeft();
				cmbStats = pair.getRight();
			}
			else if(_cs == null) {

				if(ct.contains(CompressionType.CONST)) {
					cmb = CompressedMatrixBlockFactory.createConstant(mb.getNumRows(), mb.getNumColumns(),
						mb.getValue(0, 0));
				}
				else {
					cmb = new CompressedMatrixBlock(mb.getNumRows(), mb.getNumColumns());
					List<AColGroup> colGroups = new ArrayList<>();
					int index = 0;
					CompressionSettings cs = new CompressionSettingsBuilder().setValidCompressions(EnumSet.copyOf(ct))
						.create();
					// _cs = cs;
					final int groupSize = (mb.getNumColumns() + ct.size() - 1) / ct.size();
					// String columns = "";
					for(CompressionType c : ct) {
						int size = Math.min(groupSize, mb.getNumColumns() - (groupSize * index));
						if(size == 0)
							continue;
						int[] colIndexes = new int[Math.min(groupSize, mb.getNumColumns() - (groupSize * index))];
						for(int x = 0; x < colIndexes.length; x++) {
							int y = index * groupSize + x;
							colIndexes[x] = y;
						}
						// columns += Arrays.toString(colIndexes);
						ABitmap ubm = BitmapEncoder.extractBitmap(colIndexes, mb, false, 8);

						EstimationFactors ef = CompressedSizeEstimator.estimateCompressedColGroupSize(ubm, colIndexes,
							mb.getNumRows(), cs);
						CompressedSizeInfoColGroup cgi = new CompressedSizeInfoColGroup(ef, cs.validCompressions, ubm);
						AColGroup cg = ColGroupFactory.compress(colIndexes, mb.getNumRows(), ubm, c, cs, mb,
							cgi.getTupleSparsity());
						colGroups.add(cg);
						index++;
					}
					((CompressedMatrixBlock) cmb).allocateColGroupList(colGroups);
					cmb.recomputeNonZeros();
				}
			}
			else {

				if(_cs != null && (_cs.lossy || ov == OverLapping.SQUASH))
					setLossyTolerance(valueRange);

				if(_cs.validCompressions.size() == 2) {
					/**
					 * In case only Uncompressed and Const colgroups are available. filter the big tests from
					 * uncompressed colgroup tests since the functionality should be verified even with smaller matrices
					 */

					if(rows < 10000) {

						int[] colIndexes = new int[mb.getNumColumns()];
						for(int i = 0; i < colIndexes.length; i++)
							colIndexes[i] = i;
						cmb = new CompressedMatrixBlock(mb.getNumRows(), mb.getNumColumns());
						((CompressedMatrixBlock) cmb).allocateColGroup(new ColGroupUncompressed(colIndexes, mb, false));
					}
				}
				else {
					Pair<MatrixBlock, CompressionStatistics> pair = CompressedMatrixBlockFactory.compress(mb, _k, _cs);
					cmb = pair.getLeft();
					cmbStats = pair.getRight();
				}
			}
			MatrixBlock tmp = null;
			switch(ov) {
				case COL:
					tmp = DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 1, 0.5, 1.5, 1.0, 6));
					lossyTolerance = lossyTolerance * 80;
					cols = 1;
					break;
				case MATRIX:
				case MATRIX_MULT_NEGATIVE:
				case MATRIX_PLUS:
				case SQUASH:
					tmp = DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 2, 0.5, 1.5, 1.0, 2));
					lossyTolerance = lossyTolerance * 160;
					cols = 2;
					break;
				case APPEND_EMPTY:
					tmp = new MatrixBlock(rows, 1, 0);
					break;
				case APPEND_CONST:
					tmp = new MatrixBlock(rows, 1, 0)
						.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 1), new MatrixBlock());
					break;
				default:
					break;
			}
			if(cmb instanceof CompressedMatrixBlock) {
				if(tmp != null && ov == OverLapping.APPEND_EMPTY || ov == OverLapping.APPEND_CONST) {
					mb = mb.append(tmp, new MatrixBlock());
					cmb = cmb.append(tmp, new MatrixBlock());
					cols += tmp.getNumColumns();
				}
				else if(tmp != null) {
					// Make Operator
					AggregateBinaryOperator abop = InstructionUtils.getMatMultOperator(_k);

					// vector-matrix uncompressed
					mb = mb.aggregateBinaryOperations(mb, tmp, new MatrixBlock(), abop);

					// vector-matrix compressed
					cmb = cmb.aggregateBinaryOperations(cmb, tmp, new MatrixBlock(), abop);

					if(ov == OverLapping.MATRIX_PLUS) {
						ScalarOperator sop = new LeftScalarOperator(Plus.getPlusFnObject(), 15);
						mb = mb.scalarOperations(sop, new MatrixBlock());
						cmb = cmb.scalarOperations(sop, new MatrixBlock());
					}
					else if(ov == OverLapping.MATRIX_MULT_NEGATIVE) {
						ScalarOperator sop = new LeftScalarOperator(Multiply.getMultiplyFnObject(), -1.3);
						mb = mb.scalarOperations(sop, new MatrixBlock());
						cmb = cmb.scalarOperations(sop, new MatrixBlock());
					}
					else if(ov == OverLapping.SQUASH) {
						if(cmb instanceof CompressedMatrixBlock)
							cmb = ((CompressedMatrixBlock) cmb).squash(_k);
					}
				}
				if(ov == OverLapping.PLUS) {
					ScalarOperator sop = new LeftScalarOperator(Plus.getPlusFnObject(), 5);
					mb = mb.scalarOperations(sop, new MatrixBlock());
					cmb = cmb.scalarOperations(sop, new MatrixBlock());
				}
			}

		}
		catch(Exception e) {
			e.printStackTrace();
			fail("\nCompressionTest Init failed with settings: " + this.toString());
		}

	}

	/**
	 * Tolerance for encoding values is the maximum value in dataset divided by number distinct values available in a
	 * single Byte (since we encode our quntization in Byte)
	 * 
	 * @param valueRange The value range used as input
	 */
	private void setLossyTolerance(ValueRange valueRange) {
		lossyTolerance = (double) (Math.max(TestConstants.getMaxRangeValue(valueRange),
			Math.abs(TestConstants.getMinRangeValue(valueRange)))) * (1.0 / 127.0) / 2.0;
	}

	@Parameters
	public static Collection<Object[]> data() {
		ArrayList<Object[]> tests = new ArrayList<>();

		for(SparsityType st : usedSparsityTypes)
			for(ValueType vt : usedValueTypes)
				for(ValueRange vr : usedValueRanges)
					if((st == SparsityType.ULTRA_SPARSE && vr == ValueRange.LARGE) || st != SparsityType.ULTRA_SPARSE)
						for(CompressionSettingsBuilder cs : usedCompressionSettings)
							for(MatrixTypology mt : usedMatrixTypology)
								for(OverLapping ov : overLapping)
									if((ov == OverLapping.APPEND_CONST || ov == OverLapping.APPEND_EMPTY)) {
										if(vr == ValueRange.BOOLEAN)
											tests.add(new Object[] {st, vt, vr, cs, mt, ov, null});
									}
									else
										tests.add(new Object[] {st, vt, vr, cs, mt, ov, null});
		for(CompressionSettingsBuilder cs : usedCompressionSettings)
			for(MatrixTypology mt : usedMatrixTypology)
				for(OverLapping ov : overLapping) {
					tests.add(new Object[] {SparsityType.EMPTY, ValueType.RAND, ValueRange.BOOLEAN, cs, mt, ov, null});
					tests.add(new Object[] {SparsityType.FULL, ValueType.CONST, ValueRange.LARGE, cs, mt, ov, null});
					tests.add(new Object[] {SparsityType.FULL, ValueType.ONE_HOT_ENCODED, ValueRange.BOOLEAN, cs, mt,
						ov, null});
				}

		for(ValueType vt : usedValueTypes)
			for(SparsityType st : usedSparsityTypes)
				for(MatrixTypology mt : usedMatrixTypology)
					for(OverLapping ov : overLapping) {
						// add test using default constructor.
						tests.add(new Object[] {st, vt, ValueRange.BOOLEAN, null, mt, ov, null});
						// add test using constant value.
						tests.add(new Object[] {st, vt, ValueRange.LARGE, null, mt, ov,
							Collections.singletonList(CompressionType.CONST)});
						for(CompressionType ct : forcedColGroups) {
							// tests.add(
							// new Object[] {st, vt, ValueRange.SMALL, null, mt, ov, Collections.singletonList(ct)});
							for(CompressionType ctn : forcedColGroups) {
								List<CompressionType> ctl = new ArrayList<>();
								ctl.add(ct);
								ctl.add(ctn);
								ctl.add(ct);
								ctl.add(ctn);
								tests.add(new Object[] {st, vt, ValueRange.SMALL, null, mt, ov, ctl});
							}
						}
					}

		return tests;
	}

	@Test
	public void testDecompress() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			MatrixBlock decompressedMatrixBlock = ((CompressedMatrixBlock) cmb).decompress(_k);
			compareResultMatrices(mb, decompressedMatrixBlock, 1);
			
			assertEquals(this.toString() + " number of non zeros should be equal after decompression", mb.getNonZeros(),
				decompressedMatrixBlock.getNonZeros());
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testMatrixMultChainXtXv() {
		testMatrixMultChain(ChainType.XtXv);
	}

	@Test
	public void testMatrixMultChainXtwXv() {
		testMatrixMultChain(ChainType.XtwXv);
	}

	@Test
	@Ignore
	public void testMatrixMultChainXtXvy() {
		testMatrixMultChain(ChainType.XtXvy);
	}

	public void testMatrixMultChain(ChainType ctype) {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			MatrixBlock vector1 = DataConverter
				.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 1, 0.9, 1.5, 1.0, 3));

			MatrixBlock vector2 = (ctype == ChainType.XtwXv) ? DataConverter
				.convertToMatrixBlock(TestUtils.generateTestMatrix(rows, 1, 0.9, 1.5, 1.0, 3)) : null;

			// matrix-vector uncompressed
			MatrixBlock ret1 = mb.chainMatrixMultOperations(vector1, vector2, new MatrixBlock(), ctype, _k);

			// matrix-vector compressed
			MatrixBlock ret2 = cmb.chainMatrixMultOperations(vector1, vector2, new MatrixBlock(), ctype, _k);

			// compare result with input
			TestUtils.compareMatricesPercentageDistance(DataConverter.convertToDoubleMatrix(ret1),
				DataConverter.convertToDoubleMatrix(ret2), 0.9, 0.9, this.toString());

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testVectorMatrixMult() {
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(1, rows, 0.9, 1.5, 1.0, 3));
		testLeftMatrixMatrix(vector);
	}

	@Test
	public void testLeftMatrixMatrixMultSmall() {
		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(3, rows, 0.9, 1.5, 1.0, 3));
		testLeftMatrixMatrix(matrix);

	}

	@Test
	public void testLeftMatrixMatrixMultMedium() {
		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(50, rows, 0.9, 1.5, 1.0, 3));
		testLeftMatrixMatrix(matrix);
	}

	@Test
	public void testLeftMatrixMatrixMultSparse() {
		MatrixBlock matrix = DataConverter.convertToMatrixBlock(TestUtils.generateTestMatrix(2, rows, 0.9, 1.5, .1, 3));
		testLeftMatrixMatrix(matrix);
	}

	@Test
	public void testLeftMatrixMatrixMultSparseCustom() {
		MatrixBlock matrix = new MatrixBlock(2, rows, true);
		matrix.quickSetValue(1, rows - 1, 99);
		testLeftMatrixMatrix(matrix);
	}

	@Test
	public void testLeftMatrixMatrixMultSparseCustom2() {
		MatrixBlock matrix = new MatrixBlock(2, rows, true);
		matrix.quickSetValue(1, 0, 99);
		testLeftMatrixMatrix(matrix);
	}

	@Test
	public void testLeftMatrixMatrixMultSparseCustom3() {
		MatrixBlock matrix = new MatrixBlock(2, rows, true);
		matrix.quickSetValue(0, 0, -99);
		matrix.quickSetValue(1, 0, 99);
		testLeftMatrixMatrix(matrix);
	}

	@Test
	public void testLeftMatrixMatrixMultSparseCustom4() {
		MatrixBlock matrix = new MatrixBlock(2, rows, true);
		matrix.quickSetValue(0, rows - 1, -99);
		matrix.quickSetValue(1, 0, 99);
		testLeftMatrixMatrix(matrix);
	}

	public void testLeftMatrixMatrix(MatrixBlock matrix) {
		if(!(cmb instanceof CompressedMatrixBlock))
			return; // Input was not compressed then just pass test
		try {
			// Make Operator
			AggregateBinaryOperator abop = InstructionUtils.getMatMultOperator(_k);

			MatrixBlock ret1 = mb.aggregateBinaryOperations(matrix, mb, new MatrixBlock(), abop);
			MatrixBlock ret2 = cmb.aggregateBinaryOperations(matrix, cmb, new MatrixBlock(), abop);

			// compare result with input
			compareResultMatrices(ret1, ret2, 100);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testMatrixVectorMult01() {
		testMatrixVectorMult(1.0, 1.1);
	}

	@Test
	public void testMatrixVectorMult02() {
		testMatrixVectorMult(0.7, 1.0);
	}

	@Test
	public void testMatrixVectorMult03() {
		testMatrixVectorMult(-1.0, 1.0);
	}

	@Test
	public void testMatrixVectorMult04() {
		testMatrixVectorMult(1.0, 5.0);
	}

	public void testMatrixVectorMult(double min, double max) {

		if(!(cmb instanceof CompressedMatrixBlock))
			return; // Input was not compressed then just pass test

		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 1, min, max, 1.0, 3));
		testRightMatrixMatrix(vector);
	}

	@Test
	public void testRightMatrixMatrixMultSmall() {

		if(!(cmb instanceof CompressedMatrixBlock))
			return; // Input was not compressed then just pass test

		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 2, 0.9, 1.5, 1.0, 3));

		testRightMatrixMatrix(matrix);
	}

	@Test
	public void testRightMatrixMatrixMultMedium() {

		if(!(cmb instanceof CompressedMatrixBlock))
			return; // Input was not compressed then just pass test

		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 16, 0.9, 1.5, 1.0, 3));
		testRightMatrixMatrix(matrix);

	}

	@Test
	public void testRightMatrixMatrixMultSparse() {

		if(!(cmb instanceof CompressedMatrixBlock))
			return; // Input was not compressed then just pass test

		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 25, 0.9, 1.5, 0.2, 3));
		testRightMatrixMatrix(matrix);
	}

	public void testRightMatrixMatrix(MatrixBlock matrix) {
		try {
			matrix.quickSetValue(0, 0, 10);
			// Make Operator
			AggregateBinaryOperator abop = InstructionUtils.getMatMultOperator(_k);

			// vector-matrix uncompressed
			MatrixBlock ret1 = mb.aggregateBinaryOperations(mb, matrix, new MatrixBlock(), abop);

			// vector-matrix compressed
			MatrixBlock ret2 = cmb.aggregateBinaryOperations(cmb, matrix, new MatrixBlock(), abop);

			// compare result with input

			compareResultMatrices(ret1, ret2, 10);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testLeftMatrixMatrixMultTransposedLeftSide() {
		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(rows, 2, 0.9, 1.5, 1.0, 3));
		testLeftMatrixMatrixMultiplicationTransposed(matrix, true, false, false, true);
	}

	@Test
	public void testLeftMatrixMatrixMultTransposedRightSide() {
		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(2, cols, 0.9, 1.5, 1.0, 3));
		testLeftMatrixMatrixMultiplicationTransposed(matrix, false, true, false, false);
	}

	@Test
	public void testLeftMatrixMatrixMultTransposedBothSides() {
		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(cols, 1, 0.9, 1.5, 1.0, 3));
		testLeftMatrixMatrixMultiplicationTransposed(matrix, true, true, false, false);
	}

	@Test
	public void testLeftMatrixMatrixMultDoubleCompressedTransposedLeftSideSmaller() {
		MatrixBlock matrix = CompressibleInputGenerator.getInput(rows, 1, CompressionType.OLE, 5, 20, -20, 1.0, 3);
		ReorgOperator r_op = new ReorgOperator(SwapIndex.getSwapIndexFnObject(), _k);
		matrix = matrix.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0);
		testLeftMatrixMatrixMultiplicationTransposed(matrix, true, false, true, true);

	}

	@Test
	public void testLeftMatrixMatrixMultDoubleCompressedTransposedLeftSideBigger() {
		if(rows < 6000) {
			MatrixBlock matrix = CompressibleInputGenerator.getInput(rows, cols + 1, CompressionType.OLE, 5, 20, -20,
				1.0, 3);
			ReorgOperator r_op = new ReorgOperator(SwapIndex.getSwapIndexFnObject(), _k);
			matrix = matrix.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0);
			testLeftMatrixMatrixMultiplicationTransposed(matrix, true, false, true, true);
		}

	}

	// // @Test(expected = DMLCompressionException.class)
	// // public void testLeftMatrixMatrixMultDoubleCompressedTransposedRightSide() {
	// // MatrixBlock matrix = CompressibleInputGenerator.getInput(1, rows, CompressionType.OLE, 5, 1.0, 3);
	// // testLeftMatrixMatrixMultiplicationTransposed(matrix, false, true, true);
	// // }

	// @Test
	// public void testLeftMatrixMatrixMultDoubleCompressedTransposedBothSides() {
	// if(rows < 1000) {
	// MatrixBlock matrix = CompressibleInputGenerator.getInput(1, rows, CompressionType.OLE, 5, 1.0, 3);
	// testLeftMatrixMatrixMultiplicationTransposed(matrix, true, true, true);
	// }
	// }

	public void testLeftMatrixMatrixMultiplicationTransposed(MatrixBlock matrix, boolean transposeLeft,
		boolean transposeRight, boolean compressMatrix, boolean comparePercent) {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test
			// vector-matrix compressed
			CompressionSettings cs = new CompressionSettingsBuilder()
				.setValidCompressions(EnumSet.of(CompressionType.DDC)).create();
			MatrixBlock compMatrix = compressMatrix ? CompressedMatrixBlockFactory.compress(matrix, cs)
				.getLeft() : matrix;
			if(compressMatrix && !(compMatrix instanceof CompressedMatrixBlock))
				return; // Early termination since the test does not test what we wanted.

			// Make Operator
			AggregateBinaryOperator abop = InstructionUtils.getMatMultOperator(_k);
			AggregateBinaryOperator abopSingle = InstructionUtils.getMatMultOperator(1);

			// vector-matrix uncompressed
			ReorgOperator r_op = new ReorgOperator(SwapIndex.getSwapIndexFnObject(), _k);

			// vector-matrix compressed
			MatrixBlock left = transposeLeft ? matrix.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0) : matrix;
			MatrixBlock right = transposeRight ? mb.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0) : mb;
			MatrixBlock ret1 = right.aggregateBinaryOperations(left, right, new MatrixBlock(), abopSingle);

			MatrixBlock ret2 = ((CompressedMatrixBlock) cmb).aggregateBinaryOperations(compMatrix, cmb,
				new MatrixBlock(), abop, transposeLeft, transposeRight);

			if(comparePercent && overlappingType == OverLapping.SQUASH)
				TestUtils.compareMatricesPercentageDistance(DataConverter.convertToDoubleMatrix(ret1),
					DataConverter.convertToDoubleMatrix(ret2), 0.40, 0.9, this.toString());
			else
				compareResultMatrices(ret1, ret2, 100);

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testRightMatrixMatrixMultTransposedLeftSide() {
		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(rows, 2, 0.9, 1.5, 1.0, 3));
		testRightMatrixMatrixMultiplicationTransposed(matrix, true, false, false);
	}

	@Test
	public void testRightMatrixMatrixMultTransposedRightSide() {
		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(10, cols, 0.9, 1.5, 1.0, 3));
		testRightMatrixMatrixMultiplicationTransposed(matrix, false, true, false);
	}

	@Test
	public void testRightMatrixMatrixMultTransposedBothSides() {
		MatrixBlock matrix = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(1, rows, 0.9, 1.5, 1.0, 3));
		testRightMatrixMatrixMultiplicationTransposed(matrix, true, true, false);
	}

	public void testRightMatrixMatrixMultiplicationTransposed(MatrixBlock matrix, boolean transposeLeft,
		boolean transposeRight, boolean compressMatrix) {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			// Make Operator
			AggregateBinaryOperator abop = InstructionUtils.getMatMultOperator(_k);

			// vector-matrix uncompressed
			ReorgOperator r_op = new ReorgOperator(SwapIndex.getSwapIndexFnObject(), _k);

			MatrixBlock left = transposeLeft ? mb.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0) : mb;
			MatrixBlock right = transposeRight ? matrix.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0) : matrix;
			MatrixBlock ret1 = right.aggregateBinaryOperations(left, right, new MatrixBlock(), abop);

			// vector-matrix compressed
			MatrixBlock compMatrix = compressMatrix ? CompressedMatrixBlockFactory.compress(matrix, _k)
				.getLeft() : matrix;
			MatrixBlock ret2 = ((CompressedMatrixBlock) cmb).aggregateBinaryOperations(cmb, compMatrix,
				new MatrixBlock(), abop, transposeLeft, transposeRight);

			compareResultMatrices(ret1, ret2, 100);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testTransposeSelfMatrixMultLeft() {
		// TSMM tsmm
		try {
			testTransposeSelfMatrixMult(MMTSJType.LEFT);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test(expected = DMLRuntimeException.class)
	public void testTransposeSelfMatrixMultRight() {
		if(!(cmb instanceof CompressedMatrixBlock))
			throw new DMLRuntimeException("Make test pass");
		testTransposeSelfMatrixMult(MMTSJType.RIGHT);
	}

	@Test(expected = DMLRuntimeException.class)
	public void testTransposeSelfMatrixMultNONE() {
		if(!(cmb instanceof CompressedMatrixBlock))
			throw new DMLRuntimeException("Make test pass");
		testTransposeSelfMatrixMult(MMTSJType.NONE);
	}

	public void testTransposeSelfMatrixMult(MMTSJType mType) {
		if(!(cmb instanceof CompressedMatrixBlock))
			return; // Input was not compressed then just pass test
		if(_k != 1) {
			// matrix-vector compressed
			MatrixBlock ret2 = cmb.transposeSelfMatrixMultOperations(new MatrixBlock(), mType, _k);
			// matrix-vector uncompressed
			MatrixBlock ret1 = mb.transposeSelfMatrixMultOperations(new MatrixBlock(), mType, _k);
			// compare result with input
			compareResultMatrices(ret1, ret2, 100);
		}
		else {
			// matrix-vector compressed
			MatrixBlock ret2 = cmb.transposeSelfMatrixMultOperations(new MatrixBlock(), mType);
			// matrix-vector uncompressed
			MatrixBlock ret1 = mb.transposeSelfMatrixMultOperations(new MatrixBlock(), mType);
			// compare result with input
			compareResultMatrices(ret1, ret2, 100);
		}
	}

	@Test
	public void testScalarOpRightMultiplyPositive() {
		double mult = 7;
		ScalarOperator sop = new RightScalarOperator(Multiply.getMultiplyFnObject(), mult, _k);
		testScalarOperations(sop, lossyTolerance * 7);
	}

	@Test
	public void testScalarOpRightDivide() {
		double mult = 0.2;
		ScalarOperator sop = new RightScalarOperator(Divide.getDivideFnObject(), mult, _k);
		testScalarOperations(sop, lossyTolerance * 7);
	}

	@Test
	public void testScalarOpRightMultiplyNegative() {
		double mult = -7;
		ScalarOperator sop = new RightScalarOperator(Multiply.getMultiplyFnObject(), mult, _k);
		testScalarOperations(sop, lossyTolerance * 7);
	}

	@Test
	public void testScalarRightOpAddition() {
		double addValue = 4;
		ScalarOperator sop = new RightScalarOperator(Plus.getPlusFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.05);
	}

	@Test
	public void testScalarRightOpSubtract() {
		double addValue = 15;
		ScalarOperator sop = new RightScalarOperator(Minus.getMinusFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarRightOpLess() {
		double addValue = 0.11;
		ScalarOperator sop = new RightScalarOperator(LessThan.getLessThanFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarRightOpLessThanEqual() {
		double addValue = -50;
		ScalarOperator sop = new RightScalarOperator(LessThanEquals.getLessThanEqualsFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarRightOpGreater() {
		double addValue = 0.11;
		ScalarOperator sop = new RightScalarOperator(GreaterThan.getGreaterThanFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarRightOpEquals() {
		double addValue = 1.0;
		ScalarOperator sop = new RightScalarOperator(Equals.getEqualsFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarRightOpPower2() {
		double addValue = 2;
		ScalarOperator sop = new RightScalarOperator(Power2.getPower2FnObject(), addValue);
		testScalarOperations(sop, lossyTolerance * 2);
	}

	@Test
	public void testScalarOpLeftMultiplyPositive() {
		double mult = 7;
		ScalarOperator sop = new LeftScalarOperator(Multiply.getMultiplyFnObject(), mult, _k);
		testScalarOperations(sop, lossyTolerance * 7);
	}

	@Test
	public void testScalarOpLeftMultiplyNegative() {
		double mult = -7;
		ScalarOperator sop = new LeftScalarOperator(Multiply.getMultiplyFnObject(), mult, _k);
		testScalarOperations(sop, lossyTolerance * 7);
	}

	@Test
	public void testScalarLeftOpAddition() {
		double addValue = 4;
		ScalarOperator sop = new LeftScalarOperator(Plus.getPlusFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.05);
	}

	@Test
	public void testScalarLeftOpSubtract() {
		double addValue = 15;
		ScalarOperator sop = new LeftScalarOperator(Minus.getMinusFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarLeftOpLess() {
		double addValue = 0.11;
		ScalarOperator sop = new LeftScalarOperator(LessThan.getLessThanFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarLeftOpLessSmallValue() {
		double addValue = -1000000.11;
		ScalarOperator sop = new LeftScalarOperator(LessThanEquals.getLessThanEqualsFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarLeftOpLessThanEqualSmallValue() {
		double addValue = -1000000.11;
		ScalarOperator sop = new LeftScalarOperator(LessThanEquals.getLessThanEqualsFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarLeftOpGreaterThanEqualsSmallValue() {
		double addValue = -1001310000.11;
		ScalarOperator sop = new LeftScalarOperator(GreaterThanEquals.getGreaterThanEqualsFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarLeftOpGreaterThanLargeValue() {
		double addValue = 10132400000.11;
		ScalarOperator sop = new LeftScalarOperator(GreaterThan.getGreaterThanFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarLeftOpGreater() {
		double addValue = 0.11;
		ScalarOperator sop = new LeftScalarOperator(GreaterThan.getGreaterThanFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarLeftOpEqual() {
		double addValue = 1.0;
		ScalarOperator sop = new LeftScalarOperator(Equals.getEqualsFnObject(), addValue);
		testScalarOperations(sop, lossyTolerance + 0.1);
	}

	@Test
	public void testScalarLeftOpDivide() {
		double addValue = 14.0;
		ScalarOperator sop = new LeftScalarOperator(Divide.getDivideFnObject(), addValue);
		testScalarOperations(sop, (lossyTolerance + 0.1) * 10);
	}

	public void testScalarOperations(ScalarOperator sop, double tolerance) {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test
			MatrixBlock ret1 = mb.scalarOperations(sop, new MatrixBlock());

			// matrix-scalar compressed
			MatrixBlock ret2 = cmb.scalarOperations(sop, new MatrixBlock());
			// compare result with input
			compareResultMatrices(ret1, ret2, tolerance);

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testBinaryMVAdditionROW() {
		ValueFunction vf = Plus.getPlusFnObject();
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(1, cols, -1.0, 1.5, 1.0, 3));
		testBinaryMV(vf, vector);
	}

	@Test
	public void testBinaryMVAdditionCOL() {
		ValueFunction vf = Plus.getPlusFnObject();
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(rows, 1, -1.0, 1.5, 1.0, 3));
		testBinaryMV(vf, vector);
	}

	@Test
	public void testBinaryMVMultiplyROW() {
		ValueFunction vf = Multiply.getMultiplyFnObject();
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(1, cols, -1.0, 1.5, 1.0, 3));
		testBinaryMV(vf, vector);
	}

	@Test
	public void testBinaryMVDivideROW() {
		ValueFunction vf = Divide.getDivideFnObject();
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(1, cols, -1.0, 1.5, 1.0, 3));
		testBinaryMV(vf, vector);
	}

	// Currently not supporting left hand side operations on Binary operations
	@Test
	@Ignore
	public void testBinaryMVDivideROWLeft() {
		ValueFunction vf = Divide.getDivideFnObject();
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(1, cols, -1.0, 1.5, 1.0, 3));
		testBinaryMV(vf, vector, false);
	}

	@Test
	public void testBinaryMVMultiplyCOL() {
		ValueFunction vf = Multiply.getMultiplyFnObject();
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(rows, 1, -1.0, 1.5, 1.0, 3));
		testBinaryMV(vf, vector);
	}

	@Test
	public void testBinaryMVMinusROW() {
		ValueFunction vf = Minus.getMinusFnObject();
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(1, cols, -1.0, 1.5, 1.0, 3));
		testBinaryMV(vf, vector);
	}

	@Test
	public void testBinaryMVXorROW() {
		ValueFunction vf = Xor.getXorFnObject();
		MatrixBlock vector = DataConverter
			.convertToMatrixBlock(TestUtils.generateTestMatrix(1, cols, -1.0, 1.5, 1.0, 3));
		testBinaryMV(vf, vector);
	}

	public void testBinaryMV(ValueFunction vf, MatrixBlock vector) {
		testBinaryMV(vf, vector, true);
	}

	public void testBinaryMV(ValueFunction vf, MatrixBlock vector, boolean right) {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			BinaryOperator bop = new BinaryOperator(vf);
			MatrixBlock ret1, ret2;
			if(right) {
				ret1 = mb.binaryOperations(bop, vector, new MatrixBlock());
				ret2 = cmb.binaryOperations(bop, vector, new MatrixBlock());
			}
			else {
				ret1 = vector.binaryOperations(bop, mb, new MatrixBlock());
				ret2 = vector.binaryOperations(bop, cmb, new MatrixBlock());
			}

			compareResultMatrices(ret1, ret2, 2);

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testSliceRows() {
		testSlice(rows / 5, Math.min(rows - 1, (rows / 5) * 2), 0, cols - 1);
	}

	@Test
	public void testSliceFirstColumn() {
		testSlice(0, rows - 1, 0, 0);
	}

	@Test
	public void testSliceLastColumn() {
		testSlice(0, rows - 1, cols - 1, cols - 1);
	}

	@Test
	public void testSliceAllButFirstColumn() {
		testSlice(0, rows - 1, Math.min(1, cols - 1), cols - 1);
	}

	@Test
	public void testSliceInternal() {
		testSlice(rows / 5, Math.min(rows - 1, (rows / 5) * 2), Math.min(cols - 1, cols / 5),
			Math.min(cols - 1, cols / 5 + 1));
	}

	@Test
	public void testSliceFirstValue() {
		testSlice(0, 0, 0, 0);
	}

	@Test
	public void testSliceEntireMatrix() {
		testSlice(0, rows - 1, 0, cols - 1);
	}

	public void testSlice(int rl, int ru, int cl, int cu) {
		try {

			if(!(cmb instanceof CompressedMatrixBlock))
				return;
			MatrixBlock ret2 = cmb.slice(rl, ru, cl, cu);
			MatrixBlock ret1 = mb.slice(rl, ru, cl, cu);
			if(!(ret2 instanceof CompressedMatrixBlock))
				assertEquals(ret1.getNonZeros(), ret2.getNonZeros());
			compareResultMatrices(ret1, ret2, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException("Error in Slicing", e);
		}
	}

	@Test(expected = DMLRuntimeException.class)
	public void testSliceInvalid_01() {
		if(!(cmb instanceof CompressedMatrixBlock))
			throw new DMLRuntimeException("Not Compressed Input");
		testSliceWithException(-1, 0, 0, 0);
	}

	@Test(expected = DMLRuntimeException.class)
	public void testSliceInvalid_02() {
		if(!(cmb instanceof CompressedMatrixBlock))
			throw new DMLRuntimeException("Not Compressed Input");
		testSliceWithException(rows, rows, 0, 0);
	}

	@Test(expected = DMLRuntimeException.class)
	public void testSliceInvalid_03() {
		if(!(cmb instanceof CompressedMatrixBlock))
			throw new DMLRuntimeException("Not Compressed Input");
		testSliceWithException(0, 0, cols, cols);
	}

	@Test(expected = DMLRuntimeException.class)
	public void testSliceInvalid_04() {
		if(!(cmb instanceof CompressedMatrixBlock))
			throw new DMLRuntimeException("Not Compressed Input");
		testSliceWithException(0, 0, -1, 0);
	}

	public void testSliceWithException(int rl, int ru, int cl, int cu) {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return;
			MatrixBlock ret2 = cmb.slice(rl, ru, cl, cu);
			MatrixBlock ret1 = mb.slice(rl, ru, cl, cu);
			if(!(ret2 instanceof CompressedMatrixBlock))
				assertEquals(ret1.getNonZeros(), ret2.getNonZeros());
			compareResultMatrices(ret1, ret2, 1);
		}
		catch(Exception e) {
			throw new DMLRuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testCompressAgain() {
		try {
			MatrixBlock cmba = CompressedMatrixBlockFactory.compress(cmb, _k).getLeft();
			compareResultMatrices(mb, cmba, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void append() {
		if(!(cmb instanceof CompressedMatrixBlock))
			return;
		MatrixBlock ap = new MatrixBlock(mb.getNumRows(), 1, false, 132);
		MatrixBlock ret1 = mb.append(ap, new MatrixBlock());
		MatrixBlock ret2 = cmb.append(ap, new MatrixBlock());
		compareResultMatrices(ret1, ret2, 1);
	}

	@Test
	public void appendCBindTrue() {
		if(!(cmb instanceof CompressedMatrixBlock))
			return;
		MatrixBlock ap = new MatrixBlock(mb.getNumRows(), 1, false, 132);
		MatrixBlock ret1 = mb.append(ap, new MatrixBlock(), true);
		MatrixBlock ret2 = cmb.append(ap, new MatrixBlock(), true);
		compareResultMatrices(ret1, ret2, 1);
	}

	@Test
	public void appendCBindFalse() {
		if(!(cmb instanceof CompressedMatrixBlock))
			return;
		MatrixBlock ap = new MatrixBlock(1, mb.getNumColumns(), false, 132);
		MatrixBlock ret1 = mb.append(ap, new MatrixBlock(), false);
		MatrixBlock ret2 = cmb.append(ap, new MatrixBlock(), false);
		compareResultMatrices(ret1, ret2, 1);
	}

	protected void compareResultMatrices(double[][] expected, double[][] result, double toleranceMultiplier) {
		if(_cs != null && _cs.lossy)
			TestUtils.compareMatricesPercentageDistance(expected, result, 0.25, 0.83, this.toString());
		else if(overlappingType == OverLapping.SQUASH)
			TestUtils.compareMatrices(expected, result, lossyTolerance * toleranceMultiplier * 1.3, this.toString());
		else if(rows > 65000)
			TestUtils.compareMatricesPercentageDistance(expected, result, 0.99, 0.99, this.toString());
		else if(OverLapping.effectOnOutput(overlappingType))
			TestUtils.compareMatricesPercentageDistance(expected, result, 0.99, 0.99, this.toString());
		else
			TestUtils.compareMatricesBitAvgDistance(expected, result, (long) (27000 * toleranceMultiplier), 1024,
				this.toString());

	}

	protected void compareResultMatrices(MatrixBlock expected, MatrixBlock result, double toleranceMultiplier) {
		if(expected instanceof CompressedMatrixBlock)
			expected = ((CompressedMatrixBlock) expected).decompress();
		if(result instanceof CompressedMatrixBlock)
			result = ((CompressedMatrixBlock) result).decompress();

		// compare result with input
		double[][] d1 = DataConverter.convertToDoubleMatrix(expected);
		double[][] d2 = DataConverter.convertToDoubleMatrix(result);
		compareResultMatrices(d1, d2, toleranceMultiplier);
	}
}
