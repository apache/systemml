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

package org.apache.sysds.test.component.compress.estim;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.CompressionSettingsBuilder;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimatorFactory;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

public class SampleEstimatorTest {

	protected static final Log LOG = LogFactory.getLog(SampleEstimatorTest.class.getName());

	private static final int seed = 1512314;

	final MatrixBlock mbt;

	public SampleEstimatorTest() {
		// matrix block 2 columns
		mbt = DataConverter
			.convertToMatrixBlock(TestUtils.round(TestUtils.generateTestMatrix(2, 500000, 0, 299, 1.0, seed + 1)));
	}

	@Test
	public void compressedSizeInfoEstimatorFull() {
		testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(1.0, 1.0);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_90() {
		testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(0.9, 0.9);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_50() {
		testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(0.5, 0.90);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_20() {
		testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(0.2, 0.8);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_10() {
		testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(0.1, 0.75);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_5() {
		testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(0.05, 0.7);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_1() {
		testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(0.01, 0.6);
	}

	@Test
	public void compressedSizeInfoEstimatorSample_p1() {
		testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(0.001, 0.5);
	}

	/**
	 * This test verify that the estimated number or unique values in individual columns is adhered to when analyzing
	 * multi columns.
	 * 
	 * The important part here is not if the number of unique elements is estimated correctly, but that the relation
	 * between observations is preserved.
	 * 
	 * @param ratio     Ratio to sample
	 * @param tolerance A percentage tolerance in number of unique element estimated
	 */
	private void testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(double ratio, double tolerance) {

		final CompressionSettings cs_estimate = new CompressionSettingsBuilder().setMinimumSampleSize(100)
			.setSamplingRatio(ratio).setSeed(seed).create();

		cs_estimate.transposed = true;

		final CompressedSizeEstimator estimate = CompressedSizeEstimatorFactory.getSizeEstimator(mbt, cs_estimate, 1);
		final int estimate_1 = estimate.estimateCompressedColGroupSize(new int[] {0}).getNumVals() + 1;
		final int estimate_2 = estimate.estimateCompressedColGroupSize(new int[] {1}).getNumVals() + 1;

		final int estimate_full = estimate.estimateCompressedColGroupSize(new int[] {0, 1}, estimate_1 * estimate_2)
			.getNumVals();
		assertTrue(
			"Estimate of all columns should be upper bounded by distinct of each column multiplied: " + estimate_full
				+ " * " + tolerance + " <= " + estimate_1 * estimate_2,
			estimate_full * tolerance <= estimate_1 * estimate_2);

	}

}
