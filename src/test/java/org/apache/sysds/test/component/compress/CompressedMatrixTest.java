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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collection;

import org.apache.commons.math3.random.Well1024a;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.compress.CompressionSettingsBuilder;
import org.apache.sysds.runtime.compress.CompressionStatistics;
import org.apache.sysds.runtime.compress.colgroup.AColGroup;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.matrix.data.LibMatrixCountDistinct;
import org.apache.sysds.runtime.matrix.data.LibMatrixDatagen;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.RandomMatrixGenerator;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.runtime.matrix.operators.CountDistinctOperator;
import org.apache.sysds.runtime.matrix.operators.CountDistinctOperator.CountDistinctTypes;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestUtils;
import org.apache.sysds.test.component.compress.TestConstants.MatrixTypology;
import org.apache.sysds.test.component.compress.TestConstants.OverLapping;
import org.apache.sysds.test.component.compress.TestConstants.SparsityType;
import org.apache.sysds.test.component.compress.TestConstants.ValueRange;
import org.apache.sysds.test.component.compress.TestConstants.ValueType;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openjdk.jol.datamodel.X86_64_DataModel;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.layouters.HotSpotLayouter;
import org.openjdk.jol.layouters.Layouter;

@RunWith(value = Parameterized.class)
public class CompressedMatrixTest extends AbstractCompressedUnaryTests {

	public CompressedMatrixTest(SparsityType sparType, ValueType valType, ValueRange valRange,
		CompressionSettingsBuilder compSettings, MatrixTypology matrixTypology, OverLapping ov,
		Collection<CompressionType> ct) {
		super(sparType, valType, valRange, compSettings, matrixTypology, ov, 1, ct);
	}

	@Test
	public void testGetValue() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			for(int i = 0; i < rows; i++)
				for(int j = 0; j < cols; j++) {
					double ulaVal = mb.getValue(i, j);
					double claVal = cmb.getValue(i, j); // calls quickGetValue internally
					if(_cs != null && (_cs.lossy || overlappingType == OverLapping.SQUASH))
						assertTrue(this.toString(), TestUtils.compareCellValue(ulaVal, claVal, lossyTolerance, false));
					else if(OverLapping.effectOnOutput(overlappingType)) {
						double percentDistance = TestUtils.getPercentDistance(ulaVal, claVal, true);
						assertTrue(this.toString(), percentDistance > .99);
					}
					else
						TestUtils.compareScalarBitsJUnit(ulaVal, claVal, 0,
							"Coordinates: " + i + " " + j + "\n" + this.toString()); // Should be exactly same value
				}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testQuickGetValue() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			for(int i = 0; i < rows; i++)
				for(int j = 0; j < cols; j++) {
					double ulaVal = mb.quickGetValue(i, j);
					double claVal = cmb.quickGetValue(i, j); // calls quickGetValue internally
					if(_cs != null && (_cs.lossy || overlappingType == OverLapping.SQUASH))
						assertTrue(this.toString(), TestUtils.compareCellValue(ulaVal, claVal, lossyTolerance, false));
					else if(OverLapping.effectOnOutput(overlappingType)) {
						double percentDistance = TestUtils.getPercentDistance(ulaVal, claVal, true);
						assertTrue(this.toString(), percentDistance > .99);
					}
					else
						TestUtils.compareScalarBitsJUnit(ulaVal, claVal, 0,
							"Coordinates: " + i + " " + j + "\n" + this.toString()); // Should be exactly same value
				}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testAppend() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			MatrixBlock vector = DataConverter
				.convertToMatrixBlock(TestUtils.generateTestMatrix(rows, 1, 1, 1, 1.0, 3));

			// matrix-vector uncompressed
			MatrixBlock ret1 = mb.append(vector, new MatrixBlock());

			// matrix-vector compressed
			MatrixBlock ret2 = cmb.append(vector, new MatrixBlock());
			if(ret2 instanceof CompressedMatrixBlock)
				ret2 = ((CompressedMatrixBlock) ret2).decompress();

			compareResultMatrices(ret1, ret2, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	@Ignore
	public void testCountDistinct() {
		try {
			// Counting distinct is potentially wrong in cases with overlapping, resulting in a few to many or few
			// elements.
			if(!(cmb instanceof CompressedMatrixBlock) || (overlappingType == OverLapping.MATRIX_MULT_NEGATIVE))
				return; // Input was not compressed then just pass test

			CountDistinctOperator op = new CountDistinctOperator(CountDistinctTypes.COUNT);
			int ret1 = LibMatrixCountDistinct.estimateDistinctValues(mb, op);
			int ret2 = LibMatrixCountDistinct.estimateDistinctValues(cmb, op);

			String base = this.toString() + "\n";
			if(_cs != null && _cs.lossy) {
				// The number of distinct values should be same or lower in lossy mode.
				// assertTrue(base + "lossy distinct count " +ret2+ "is less than full " + ret1, ret1 >= ret2);

				// above assumption is false, since the distinct count when using multiple different scales becomes
				// larger due to differences in scale.
				assertTrue(base + "lossy distinct count " + ret2 + "is greater than 0", 0 < ret2);
			}
			else {
				assertEquals(base, ret1, ret2);
			}

		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Override
	public void testUnaryOperators(AggType aggType, boolean inCP) {
		AggregateUnaryOperator auop = super.getUnaryOperator(aggType, 1);
		testUnaryOperators(aggType, auop, inCP);
	}

	@Test
	public void testSerialization() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return; // Input was not compressed then just pass test

			// serialize compressed matrix block
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			DataOutputStream fos = new DataOutputStream(bos);
			cmb.write(fos);

			// deserialize compressed matrix block
			ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
			DataInputStream fis = new DataInputStream(bis);
			CompressedMatrixBlock cmb2 = new CompressedMatrixBlock(-1,-1);
			cmb2.readFields(fis);

			// decompress the compressed matrix block
			MatrixBlock tmp = cmb2.decompress();

			compareResultMatrices(mb, tmp, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testCompressionRatio() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || _cs == null ||
				_cs.columnPartitioner.toString().contains("COST"))
				return;
			CompressionStatistics cStat = cmbStats;
			if(cStat != null)
				assertTrue("Compression ration if compressed should be larger than 1", cStat.getRatio() > 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testCompressionEstimationVSCompression() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || _cs == null ||
				_cs.columnPartitioner.toString().contains("COST"))
				return;
			CompressionStatistics cStat = cmbStats;
			if(cStat != null) {
				long colsEstimate = cStat.estimatedSizeCols;
				long actualSize = cStat.size;
				long originalSize = cStat.originalSize;
				int allowedTolerance = 4096;

				if(_cs.samplingRatio < 1.0) {
					allowedTolerance = sampleTolerance;
				}
				if(rows > 50000) {
					allowedTolerance *= 10;
				}

				boolean res = Math.abs(colsEstimate - actualSize) <= originalSize;
				res = res && actualSize - allowedTolerance < colsEstimate;
				if(!res) {
					StringBuilder builder = new StringBuilder();
					builder.append("\n\t" + String.format("%-40s - %12d", "Actual compressed size: ", actualSize));
					builder.append("\n\t" + String.format("%-40s - %12d with tolerance: %5d",
						"<= estimated isolated ColGroups: ", colsEstimate, allowedTolerance));
					builder.append("\n\t" + String.format("%-40s - %12d", "<= Original size: ", originalSize));
					builder.append("\n\tcol groups types: " + cStat.getGroupsTypesString());
					builder.append("\n\tcol groups sizes: " + cStat.getGroupsSizesString());
					builder.append("\n\t" + this.toString());
					assertTrue(builder.toString(), res);
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Ignore
	@Test
	public void testCompressionEstimationVSJolEstimate() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return;
			CompressionStatistics cStat = cmbStats;
			if(cStat != null) {
				long actualSize = cStat.size;
				long originalSize = cStat.originalSize;
				long JolEstimatedSize = getJolSize(((CompressedMatrixBlock) cmb), cmbStats);

				StringBuilder builder = new StringBuilder();
				builder.append("\n\t" + String.format("%-40s - %12d", "Actual compressed size: ", actualSize));
				builder.append("\n\t" + String.format("%-40s - %12d", "<= Original size: ", originalSize));
				builder.append("\n\t" + String.format("%-40s - %12d", "and equal to JOL Size: ", JolEstimatedSize));
				// builder.append("\n\t " + getJolSizeString(cmb));
				builder.append("\n\tcol groups types: " + cStat.getGroupsTypesString());
				builder.append("\n\tcol groups sizes: " + cStat.getGroupsSizesString());
				builder.append("\n\t" + this.toString());

				// NOTE: The Jol estimate is wrong for shared dictionaries because
				// it treats the object hierarchy as a tree and not a graph
				assertTrue(builder.toString(),
					actualSize <= originalSize && (_cs.allowSharedDictionary || actualSize == JolEstimatedSize));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testCompressionScale() {
		// This test is here for a sanity check such that we verify that the compression
		// ratio from our Matrix
		// Compressed Block is not unreasonably good.
		try {
			if(!(cmb instanceof CompressedMatrixBlock))
				return;

			CompressionStatistics cStat = cmbStats;
			if(cStat != null) {
				final double compressRatio = cStat.getRatio();

				StringBuilder builder = new StringBuilder();
				builder.append("Compression Ratio sounds suspiciously good at: " + compressRatio);
				builder.append("\n\tActual compressed size: " + cStat.size);
				builder.append(" original size: " + cStat.originalSize);
				builder.append("\n\tcol groups types: " + cStat.getGroupsTypesString());
				builder.append("\n\tcol groups sizes: " + cStat.getGroupsSizesString());
				builder.append("\n\t" + this.toString());

				assertTrue(builder.toString(), compressRatio < 1000.0);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this.toString() + "\n" + e.getMessage(), e);
		}
	}

	@Test
	public void testMin() {
		if(!(cmb instanceof CompressedMatrixBlock))
			return;
		double ret1 = cmb.min();
		double ret2 = mb.min();
		if(_cs != null && (_cs.lossy || overlappingType == OverLapping.SQUASH))
			assertTrue(this.toString(), TestUtils.compareCellValue(ret2, ret1, lossyTolerance, false));
		else if(OverLapping.effectOnOutput(overlappingType)) {
			double percentDistance = TestUtils.getPercentDistance(ret2, ret1, true);
			assertTrue(this.toString(), percentDistance > .99);
		}
		else
			TestUtils.compareScalarBitsJUnit(ret2, ret1, 0, this.toString()); // Should be exactly same value

	}

	@Test
	public void testMax() {
		if(!(cmb instanceof CompressedMatrixBlock))
			return;
		double ret1 = cmb.max();
		double ret2 = mb.max();
		if(_cs != null && (_cs.lossy || overlappingType == OverLapping.SQUASH))
			assertTrue(this.toString(), TestUtils.compareCellValue(ret2, ret1, lossyTolerance, false));
		else if(OverLapping.effectOnOutput(overlappingType)) {
			double percentDistance = TestUtils.getPercentDistance(ret2, ret1, true);
			assertTrue(this.toString(), percentDistance > .99);
		}
		else
			TestUtils.compareScalarBitsJUnit(ret2, ret1, 0, this.toString()); // Should be exactly same value

	}

	@Test
	public void testSum() {
		if(!(cmb instanceof CompressedMatrixBlock))
			return;
		double ret1 = cmb.sum();
		double ret2 = mb.sum();
		if(_cs != null && (_cs.lossy || overlappingType == OverLapping.SQUASH))
			assertTrue(this.toString(), TestUtils.compareCellValue(ret2, ret1, lossyTolerance, false));
		else if(OverLapping.effectOnOutput(overlappingType)) {
			double percentDistance = TestUtils.getPercentDistance(ret2, ret1, true);
			assertTrue(this.toString(), percentDistance > .99);
		}
		else
			TestUtils.compareScalarBitsJUnit(ret2, ret1, 0, this.toString()); // Should be exactly same value

	}

	@Test
	public void testSumSq() {
		if(!(cmb instanceof CompressedMatrixBlock))
			return;
		double ret1 = cmb.sumSq();
		double ret2 = mb.sumSq();
		if(_cs != null && (_cs.lossy || overlappingType == OverLapping.SQUASH))
			assertTrue(this.toString(), TestUtils.compareCellValue(ret2, ret1, lossyTolerance, false));
		else if(OverLapping.effectOnOutput(overlappingType)) {
			double percentDistance = TestUtils.getPercentDistance(ret2, ret1, true);
			assertTrue(this.toString(), percentDistance > .99);
		}
		else
			TestUtils.compareScalarBitsJUnit(ret2, ret1, 0, this.toString()); // Should be exactly same value

	}

	@Test
	public void testRandOperationsInPlace() {
		if(!(cmb instanceof CompressedMatrixBlock) || rows * cols > 10000)
			return;
		RandomMatrixGenerator rgen = new RandomMatrixGenerator("uniform", rows, cols,
			ConfigurationManager.getBlocksize(), sparsity, min, max);
		Well1024a bigrand = null;
		if(!LibMatrixDatagen.isShortcutRandOperation(min, max, sparsity, RandomMatrixGenerator.PDF.UNIFORM))
			bigrand = LibMatrixDatagen.setupSeedsForRand(seed);
		MatrixBlock ret1 = cmb.randOperationsInPlace(rgen, bigrand, 1342);
		if(!LibMatrixDatagen.isShortcutRandOperation(min, max, sparsity, RandomMatrixGenerator.PDF.UNIFORM))
			bigrand = LibMatrixDatagen.setupSeedsForRand(seed);
		MatrixBlock ret2 = mb.randOperationsInPlace(rgen, bigrand, 1342);

		compareResultMatrices(ret1, ret2, 1);

	}

	@Test
	public void testToString() {
		if(!(cmb instanceof CompressedMatrixBlock) || rows * cols > 10000)
			return;
		String st = cmb.toString();
		assertTrue(st.contains("CompressedMatrixBlock"));
	}

	@Test
	public void testCompressionStatisticsToString() {
		try {
			if(cmbStats != null) {
				String st = cmbStats.toString();
				assertTrue(st.contains("CompressionStatistics"));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException(e);
		}
	}

	@Test
	public void testContainsValue_maybe() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || rows * cols > 10000)
				return;
			boolean ret1 = cmb.containsValue(min);
			boolean ret2 = mb.containsValue(min);
			assertTrue(this.toString(), ret1 == ret2);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException(e);
		}
	}

	@Test
	public void testContainsValue_not() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || rows * cols > 10000)
				return;
			boolean ret1 = cmb.containsValue(min - 1);
			boolean ret2 = mb.containsValue(min - 1);
			assertTrue(this.toString(), ret1 == ret2);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException(e);
		}
	}

	@Test
	public void testReplace() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || rows * cols > 10000)
				return;
			MatrixBlock ret1 = cmb.replaceOperations(new MatrixBlock(), min - 1, 1425);
			MatrixBlock ret2 = mb.replaceOperations(new MatrixBlock(), min - 1, 1425);
			compareResultMatrices(ret2, ret1, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException(e);
		}
	}

	@Test
	public void testCompressedMatrixConstruction() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || rows * cols > 10000)
				return;
			CompressedMatrixBlock cmbC = (CompressedMatrixBlock) cmb;
			CompressedMatrixBlock cmbCopy = new CompressedMatrixBlock(cmbC);

			compareResultMatrices(mb, cmbCopy, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException(e);
		}
	}

	@Test
	public void testCompressedMatrixCopy() {
		try {
			if(!(cmb instanceof CompressedMatrixBlock) || rows * cols > 10000)
				return;
			CompressedMatrixBlock cmbCopy = new CompressedMatrixBlock(cmb.getNumRows(), cmb.getNumColumns());
			cmbCopy.copy(cmb);

			compareResultMatrices(mb, cmbCopy, 1);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException(e);
		}
	}

	@Test(expected = DMLRuntimeException.class)
	public void testCompressedMatrixCopyMatrixBlock_shouldThrowException() {
		CompressedMatrixBlock cmbCopy = new CompressedMatrixBlock(mb.getNumRows(), mb.getNumColumns());
		cmbCopy.copy(mb);
	}

	@Test(expected = RuntimeException.class)
	public void testCompressedMatrixCopyToSelf_shouldThrowException() {
		cmb.copy(cmb);
	}

	private static long getJolSize(CompressedMatrixBlock cmb, CompressionStatistics cStat) {
		Layouter l = new HotSpotLayouter(new X86_64_DataModel());
		long jolEstimate = 0;
		for(Object ob : new Object[] {cmb, cmb.getColGroups()}) {
			jolEstimate += ClassLayout.parseInstance(ob, l).instanceSize();
		}
		for(AColGroup cg : cmb.getColGroups()) {
			jolEstimate += cg.estimateInMemorySize();
		}
		return jolEstimate;
	}

}
