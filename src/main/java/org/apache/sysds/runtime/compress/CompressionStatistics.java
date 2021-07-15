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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.sysds.runtime.compress.colgroup.AColGroup;

/**
 * Compression Statistics contain the main information gathered from the compression, such as sizes of the original
 * matrix, vs the compressed representation at different stages of the compression.
 */
public class CompressionStatistics {

	// sizes while compressing
	public long estimatedSizeCoCoded;
	public long estimatedSizeCols;
	public long compressedInitialSize;

	// sizes before compression
	public long originalSize;
	public long denseSize;

	// compressed size
	public long size;

	private Map<String, int[]> colGroupCounts;

	/**
	 * Set array of counts regarding col group types.
	 * 
	 * The position corresponds with the enum ordinal.
	 * 
	 * @param colGroups list of ColGroups used in compression.
	 */
	protected void setColGroupsCounts(List<AColGroup> colGroups) {
		HashMap<String, int[]> ret = new HashMap<>();
		for(AColGroup c : colGroups) {
			String ct = c.getClass().getSimpleName();
			int colCount = c.getNumCols();
			int[] values;
			if(ret.containsKey(ct)) {
				values = ret.get(ct);
				values[0] += 1;
				values[1] += colCount;
			}
			else {
				values = new int[] {1, colCount};
			}
			ret.put(ct, values);
		}
		this.colGroupCounts = ret;
	}

	public Map<String, int[]> getColGroups() {
		return colGroupCounts;
	}

	public String getGroupsTypesString() {
		StringBuilder sb = new StringBuilder();

		for(String ctKey : colGroupCounts.keySet())
			sb.append(ctKey + ":" + colGroupCounts.get(ctKey)[0] + " ");

		return sb.toString();
	}

	public String getGroupsSizesString() {
		StringBuilder sb = new StringBuilder();

		for(String ctKey : colGroupCounts.keySet())
			sb.append(ctKey + ":" + colGroupCounts.get(ctKey)[1] + " ");

		return sb.toString();
	}

	public double getRatio() {
		return size == 0.0 ? Double.POSITIVE_INFINITY : (double) originalSize / size;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nCompressionStatistics:");
		sb.append("\nDense Size            : " + denseSize);
		sb.append("\nOriginal Size         : " + originalSize);
		sb.append("\nCompressed Size       : " + size);
		sb.append("\nCompressionRatio      : " + getRatio());
		if(colGroupCounts != null){
			sb.append("\nCompressionTypes      : " + getGroupsTypesString());
			sb.append("\nCompressionGroupSizes : " + getGroupsSizesString());
		}
		return sb.toString();
	}

}
