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

package org.apache.sysds.runtime.compress.cocode;

import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.cost.ICostEstimate;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfo;

public class CoCoderFactory {

	/**
	 * The Valid coCoding techniques
	 */
	public enum PartitionerType {
		BIN_PACKING, STATIC, PRIORITY_QUE, GREEDY, AUTO;
	}

	/**
	 * Main entry point of CoCode.
	 * 
	 * This package groups together ColGroups across columns, to improve compression further,
	 * 
	 * @param est           The size estimator used for estimating ColGroups potential sizes and construct compression
	 *                      info objects
	 * @param colInfos      The information already gathered on the individual ColGroups of columns.
	 * @param k             The concurrency degree allowed for this operation.
	 * @param costEstimator The Cost estimator to estimate the cost of the compression
	 * @param cs            The compression settings used in the compression.
	 * @return The estimated (hopefully) best groups of ColGroups.
	 */
	public static CompressedSizeInfo findCoCodesByPartitioning(CompressedSizeEstimator est, CompressedSizeInfo colInfos,
		int k, ICostEstimate costEstimator, CompressionSettings cs) {

		// Use column group partitioner to create partitions of columns
		CompressedSizeInfo bins = createColumnGroupPartitioner(cs.columnPartitioner, est, costEstimator, cs)
			.coCodeColumns(colInfos, k);

		return bins;
	}

	private static AColumnCoCoder createColumnGroupPartitioner(PartitionerType type, CompressedSizeEstimator est,
		ICostEstimate costEstimator, CompressionSettings cs) {
		switch(type) {
			case AUTO:
				// TODO make decision better depending on how much time is allocated for the compression
				// for instance if the compressed object is used for a million instructions, it might be good to
				// search for a really good compression even if it take longer.
				if(est.getNumColumns() > 200)
					return new CoCodePriorityQue(est, costEstimator, cs);
				else
					return new CoCodeGreedy(est, costEstimator, cs);
			case GREEDY:
				return new CoCodeGreedy(est, costEstimator, cs);
			case BIN_PACKING:
				return new CoCodeBinPacking(est, costEstimator, cs);
			case STATIC:
				return new CoCodeStatic(est, costEstimator, cs);
			case PRIORITY_QUE:
				return new CoCodePriorityQue(est, costEstimator, cs);
			default:
				throw new RuntimeException("Unsupported column group partitioner: " + type.toString());
		}
	}
}
