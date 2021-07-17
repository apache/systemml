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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.CompressionSettingsBuilder;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimatorFactory;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfoColGroup;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JoinCompressionInfoTest {

	protected static final Log LOG = LogFactory.getLog(SampleEstimatorTest.class.getName());

	private static final int seed = 1512314;

	final MatrixBlock mbt;

	public JoinCompressionInfoTest() {
		// matrix block 2 columns
		MatrixBlock tmp = DataConverter
			.convertToMatrixBlock(TestUtils.round(TestUtils.generateTestMatrix(2, 500000, 1, 10, 1.0, seed + 1)));
		tmp = tmp.append(
			DataConverter
				.convertToMatrixBlock(TestUtils.round(TestUtils.generateTestMatrix(1, 500000, 1, 2, 1.0, seed + 1))),
			new MatrixBlock(), false);
		mbt = tmp;
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

	private void testSampleEstimateIsAtMaxEstimatedElementsInEachColumnsProduct(double ratio, double tolerance) {
		try {

			final CompressionSettings cs_estimate = new CompressionSettingsBuilder().setMinimumSampleSize(100)
				.setSamplingRatio(ratio).setSeed(seed).create();

			cs_estimate.transposed = true;

			final CompressedSizeEstimator es = CompressedSizeEstimatorFactory.getSizeEstimator(mbt, cs_estimate, 1);
			CompressedSizeInfoColGroup g1 = es.estimateCompressedColGroupSize(new int[] {0});
			CompressedSizeInfoColGroup g2 = es.estimateCompressedColGroupSize(new int[] {1});
			g1 = es.estimateJoinCompressedSize(g1, g2);
			g2 = es.estimateCompressedColGroupSize(new int[] {2});

			CompressedSizeInfoColGroup joined_result = es.estimateJoinCompressedSize(g1, g2);
			CompressedSizeInfoColGroup estimate_full = es.estimateCompressedColGroupSize(new int[] {0, 1, 2});

			Assert.assertEquals(joined_result.getNumVals(), estimate_full.getNumVals());
		}
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}

	}
}
