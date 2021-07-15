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

package org.apache.sysds.test.component.compress.colgroup;

import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.CompressionSettingsBuilder;
import org.apache.sysds.runtime.compress.colgroup.AColGroup;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.compress.colgroup.ColGroupFactory;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimatorFactory;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfoColGroup;
import org.apache.sysds.runtime.compress.lib.BitmapEncoder;
import org.apache.sysds.runtime.compress.utils.ABitmap;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public abstract class JolEstimateTest {

	protected static final Log LOG = LogFactory.getLog(JolEstimateTest.class.getName());

	protected static final CompressionType ddc = CompressionType.DDC;
	protected static final CompressionType ole = CompressionType.OLE;
	protected static final CompressionType rle = CompressionType.RLE;
	protected static final CompressionType sdc = CompressionType.SDC;
	protected static final CompressionType unc = CompressionType.UNCOMPRESSED;
	private static final int seed = 7;

	private final int[] colIndexes;
	private final MatrixBlock mbt;

	public abstract CompressionType getCT();

	private final long actualSize;
	private final int actualNumberUnique;
	private final AColGroup cg;

	public JolEstimateTest(MatrixBlock mbt) {
		this.mbt = mbt;
		colIndexes = new int[mbt.getNumRows()];
		for(int x = 0; x < mbt.getNumRows(); x++)
			colIndexes[x] = x;

		try {
			CompressionSettings cs = new CompressionSettingsBuilder().setSamplingRatio(1.0)
				.setValidCompressions(EnumSet.of(getCT())).create();
			cs.transposed = true;
			ABitmap ubm = BitmapEncoder.extractBitmap(colIndexes, mbt, true, 8);
			cg = ColGroupFactory.compress(colIndexes, mbt.getNumColumns(), ubm, getCT(), cs, mbt, 1);
			actualSize = cg.estimateInMemorySize();
			actualNumberUnique = cg.getNumValues();
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DMLRuntimeException("Failed construction : " + this.getClass().getSimpleName());
		}
	}

	@Test
	public void compressedSizeInfoEstimatorExact() {
		compressedSizeInfoEstimatorSample(1.0, 1.0);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_90() {
		compressedSizeInfoEstimatorSample(0.9, 0.9);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_50() {
		compressedSizeInfoEstimatorSample(0.5, 0.8);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_20() {
		compressedSizeInfoEstimatorSample(0.2, 0.7);
	}

	// @Test
	// public void compressedSizeInfoEstimatorSample_10() {
	// compressedSizeInfoEstimatorSample(0.1, 0.6);
	// }

	// @Test
	// public void compressedSizeInfoEstimatorSample_5() {
	// compressedSizeInfoEstimatorSample(0.05, 0.5);
	// }

	// @Test
	// public void compressedSizeInfoEstimatorSample_1() {
	// compressedSizeInfoEstimatorSample(0.01, 0.4);
	// }

	public void compressedSizeInfoEstimatorSample(double ratio, double tolerance) {
		try {

			CompressionSettings cs = new CompressionSettingsBuilder().setSamplingRatio(ratio)
				.setValidCompressions(EnumSet.of(getCT())).setMinimumSampleSize(100).setSeed(seed).create();
			cs.transposed = true;

			final CompressedSizeInfoColGroup cgsi = CompressedSizeEstimatorFactory.getSizeEstimator(mbt, cs, 1)
				.estimateCompressedColGroupSize();

			if(cg.getCompType() != CompressionType.UNCOMPRESSED && actualNumberUnique > 10) {

				final int estimateNUniques = cgsi.getNumVals();
				final double minToleranceNUniques = actualNumberUnique * tolerance;
				final double maxToleranceNUniques = actualNumberUnique / tolerance;
				final String uniqueString = minToleranceNUniques + " < " + estimateNUniques + " < "
					+ maxToleranceNUniques;
				final boolean withinToleranceOnNUniques = minToleranceNUniques <= actualNumberUnique &&
					actualNumberUnique <= maxToleranceNUniques;
				assertTrue("CSI Sampled estimate of number of unique values not in range " + uniqueString + "\n" + cgsi,
					withinToleranceOnNUniques);
			}

			final long estimateCSI = cgsi.getCompressionSize(cg.getCompType());
			final double minTolerance = actualSize * tolerance;
			final double maxTolerance = actualSize / tolerance;
			final String rangeString = minTolerance + " < " + estimateCSI + " < " + maxTolerance;
			final boolean withinToleranceOnSize = minTolerance <= estimateCSI && estimateCSI <= maxTolerance;
			assertTrue("CSI Sampled estimate is not in tolerance range " + rangeString + " Actual number uniques:"
				+ actualNumberUnique + "\n" + cgsi, withinToleranceOnSize);

		}
		catch(Exception e) {
			e.printStackTrace();
			assertTrue("Failed sample test " + getCT() + "", false);
		}
	}
}
