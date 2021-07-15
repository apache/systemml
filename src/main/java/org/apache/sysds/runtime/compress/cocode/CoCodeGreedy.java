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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.sysds.runtime.compress.CompressionSettings;
import org.apache.sysds.runtime.compress.colgroup.AColGroup.CompressionType;
import org.apache.sysds.runtime.compress.cost.ICostEstimate;
import org.apache.sysds.runtime.compress.estim.CompressedSizeEstimator;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfo;
import org.apache.sysds.runtime.compress.estim.CompressedSizeInfoColGroup;
import org.apache.sysds.runtime.compress.utils.Util;


public class CoCodeGreedy extends AColumnCoCoder {


	private final Memorizer mem;

	protected CoCodeGreedy(CompressedSizeEstimator sizeEstimator, ICostEstimate costEstimator,
		CompressionSettings cs) {
		super(sizeEstimator, costEstimator, cs);
		mem = new Memorizer();
	}

	@Override
	protected CompressedSizeInfo coCodeColumns(CompressedSizeInfo colInfos, int k) {
		for(CompressedSizeInfoColGroup g : colInfos.compressionInfo)
			mem.put(g);
		
		colInfos.setInfo(coCodeBruteForce(colInfos.compressionInfo));
		return colInfos;
	}

	private List<CompressedSizeInfoColGroup> coCodeBruteForce(List<CompressedSizeInfoColGroup> inputColumns) {

		List<ColIndexes> workset = new ArrayList<>(inputColumns.size());

		for(int i = 0; i < inputColumns.size(); i++)
			workset.add(new ColIndexes(inputColumns.get(i).getColumns()));

		// process merging iterations until no more change
		while(workset.size() > 1) {
			double changeInSize = 0;
			CompressedSizeInfoColGroup tmp = null;
			ColIndexes selected1 = null, selected2 = null;
			for(int i = 0; i < workset.size(); i++) {
				for(int j = i + 1; j < workset.size(); j++) {
					final ColIndexes c1 = workset.get(i);
					final ColIndexes c2 = workset.get(j);
					final double costC1 = _cest.getCostOfColumnGroup(mem.get(c1));
					final double costC2 = _cest.getCostOfColumnGroup(mem.get(c2));

					mem.incst1();
					// pruning filter : skip dominated candidates
					// Since even if the entire size of one of the column lists is removed,
					// it still does not improve compression
					if(-Math.min(costC1, costC2) > changeInSize)
						continue;

					// Join the two column groups.
					// and Memorize the new join.
					final CompressedSizeInfoColGroup c1c2Inf = mem.getOrCreate(c1, c2);
					final double costC1C2 = _cest.getCostOfColumnGroup(c1c2Inf);

					final double newSizeChangeIfSelected = costC1C2 - costC1 - costC2;
					// Select the best join of either the currently selected
					// or keep the old one.
					if((tmp == null && newSizeChangeIfSelected < changeInSize) || tmp != null &&
						(newSizeChangeIfSelected < changeInSize || newSizeChangeIfSelected == changeInSize &&
							c1c2Inf.getColumns().length < tmp.getColumns().length)) {
						changeInSize = newSizeChangeIfSelected;
						tmp = c1c2Inf;
						selected1 = c1;
						selected2 = c2;
					}
				}
			}

			if(tmp != null) {
				workset.remove(selected1);
				workset.remove(selected2);
				mem.remove(selected1, selected2);
				workset.add(new ColIndexes(tmp.getColumns()));
			}
			else
				break;
		}

		LOG.debug(mem.stats());
		mem.resetStats();

		List<CompressedSizeInfoColGroup> ret = new ArrayList<>(workset.size());

		for(ColIndexes w : workset)
			ret.add(mem.get(w));

		return ret;
	}

	protected class Memorizer {
		private final Map<ColIndexes, CompressedSizeInfoColGroup> mem;
		private int st1 = 0, st2 = 0, st3 = 0, st4 = 0;

		public Memorizer() {
			mem = new HashMap<>();
		}

		public void put(CompressedSizeInfoColGroup g) {
			mem.put(new ColIndexes(g.getColumns()), g);
		}

		// public CompressedSizeInfoColGroup get(CompressedSizeInfoColGroup g) {
		// 	return mem.get(new ColIndexes(g.getColumns()));
		// }

		public CompressedSizeInfoColGroup get(ColIndexes c) {
			return mem.get(c);
		}

		public void remove(ColIndexes c1, ColIndexes c2){
			mem.remove(c1);
			mem.remove(c2);
		}

		public CompressedSizeInfoColGroup getOrCreate(ColIndexes c1, ColIndexes c2) {
			final int[] c = Util.join(c1._indexes, c2._indexes);
			final ColIndexes cI = new ColIndexes(c);
			CompressedSizeInfoColGroup g = mem.get(cI);
			st2++;
			if(g == null) {
				final CompressedSizeInfoColGroup left = mem.get(c1);
				final CompressedSizeInfoColGroup right = mem.get(c2);
				final boolean leftConst = left.getBestCompressionType(_cs) == CompressionType.CONST &&
					left.getNumOffs() == 0;
				final boolean rightConst = right.getBestCompressionType(_cs) == CompressionType.CONST &&
					right.getNumOffs() == 0;
				if(leftConst)
					g = CompressedSizeInfoColGroup.addConstGroup(c, right, _cs.validCompressions);
				else if(rightConst)
					g = CompressedSizeInfoColGroup.addConstGroup(c, left, _cs.validCompressions);
				else {
					st3++;
					g = _sest.estimateJoinCompressedSize(left, right);
				}

				if(leftConst || rightConst)
					st4++;

				mem.put(cI, g);
			}
			return g;
		}

		public void incst1() {
			st1++;
		}

		public String stats() {
			return st1 + " " + st2 + " " + st3 + " " + st4;
		}

		public void resetStats() {
			st1 = 0;
			st2 = 0;
			st3 = 0;
			st4 = 0;
		}

		@Override
		public String toString() {
			return mem.toString();
		}
	}

	private static class ColIndexes {
		final int[] _indexes;
		final int _hash;

		public ColIndexes(int[] indexes) {
			_indexes = indexes;
			_hash = Arrays.hashCode(_indexes);
		}

		@Override
		public int hashCode() {
			return _hash;
		}

		@Override
		public boolean equals(Object that) {
			ColIndexes thatGrp = (ColIndexes) that;
			return Arrays.equals(_indexes, thatGrp._indexes);
		}
	}
}
