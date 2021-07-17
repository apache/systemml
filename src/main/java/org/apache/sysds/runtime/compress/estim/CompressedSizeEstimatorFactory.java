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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

public class CompressedSizeEstimatorFactory {
	protected static final Log LOG = LogFactory.getLog(CompressedSizeEstimatorFactory.class.getName());

	public static CompressedSizeEstimator getSizeEstimator(MatrixBlock data, CompressionSettings cs, int k) {

		final int nRows = cs.transposed ? data.getNumColumns() : data.getNumRows();
		final int nCols = cs.transposed ? data.getNumRows() : data.getNumColumns();
		final int nnzRows = (int) Math.ceil(data.getNonZeros() / nCols);
		
		final double sampleRatio = cs.samplingRatio;
		final int sampleSize = getSampleSize(sampleRatio, nRows, cs.minimumSampleSize);

		final CompressedSizeEstimator est = (shouldUseExactEstimator(cs, nRows, sampleSize,
			nnzRows)) ? new CompressedSizeEstimatorExact(data,
				cs) : tryToMakeSampleEstimator(data, cs, sampleRatio, sampleSize, nRows, nnzRows, k);

		LOG.debug("Estimating using: " + est);
		return est;
	}

	private static CompressedSizeEstimator tryToMakeSampleEstimator(MatrixBlock data, CompressionSettings cs,
		double sampleRatio, int sampleSize, int nRows, int nnzRows, int k) {
		CompressedSizeEstimatorSample estS = new CompressedSizeEstimatorSample(data, cs, sampleSize, k);
		while(estS.getSample() == null) {
			LOG.warn("Doubling sample size");
			sampleSize = sampleSize * 2;
			if(shouldUseExactEstimator(cs, nRows, sampleSize, nnzRows))
				return new CompressedSizeEstimatorExact(data, cs);
			else
				estS.sampleData(sampleSize);
		}
		return estS;
	}

	private static boolean shouldUseExactEstimator(CompressionSettings cs, int nRows, int sampleSize, int nnzRows) {
		return cs.samplingRatio >= 1.0 || nRows < cs.minimumSampleSize || sampleSize > nnzRows;
	}

	private static int getSampleSize(double sampleRatio, int nRows, int minimumSampleSize) {
		return Math.max((int) Math.ceil(nRows * sampleRatio), minimumSampleSize);
	}
}
