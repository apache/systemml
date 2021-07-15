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

package org.apache.sysds.runtime.compress.colgroup.insertionsort;

import org.apache.sysds.runtime.compress.colgroup.mapping.AMapToData;
import org.apache.sysds.runtime.compress.colgroup.mapping.MapToFactory;
import org.apache.sysds.runtime.compress.utils.IntArrayList;

public class MaterializeSortV2 extends AInsertionSorter {

	/** a dense mapToData, that have a value for each row in the input. */
	final private AMapToData md;
	final private int CACHE_BLOCK = 60000;
	final private int[] skip;
	private int off = 0;

	public MaterializeSortV2(int endLength, int numRows, IntArrayList[] offsets, int negativeIndex) {
		super(endLength, numRows, offsets, negativeIndex);

		md = MapToFactory.create(Math.min(_numRows, CACHE_BLOCK), _numLabels);
		skip = new int[offsets.length];

		for(int block = 0; block < _numRows; block += CACHE_BLOCK) {
			md.fill(_numLabels);
			if(_negativeIndex == -1)
				insert(block, Math.min(block + CACHE_BLOCK, _numRows));
			else
				insertWithNegative(block, Math.min(block + CACHE_BLOCK, _numRows));
		}
	}

	private void insert(int rl, int ru) {
		materializeInsert(rl, ru);
		filterInsert(rl, ru);
	}

	private void materializeInsert(int rl, int ru) {
		for(int i = 0; i < _offsets.length; i++) {
			final IntArrayList of = _offsets[i];
			final int size = of.size();
			int k = skip[i];
			while(k < size && of.get(k) < ru)
				md.set(of.get(k++) - rl, i);
			skip[i] = k;
		}
	}

	private void filterInsert(int rl, int ru) {
		for(int i = rl; i < ru; i++) {
			final int idx = md.getIndex(i - rl);
			if(idx != _numLabels)
				set(off++, i, idx);
		}
	}

	private void insertWithNegative(int rl, int ru) {
		for(int i = 0; i < _offsets.length; i++) {
			IntArrayList of = _offsets[i];
			int k = skip[i];
			while(k < of.size() && of.get(k) < ru)
				md.set(of.get(k++) - rl, i);
			skip[i] = k;
		}

		for(int i = rl; i < ru; i++) {
			final int idx = md.getIndex(i - rl);
			if(idx < _negativeIndex)
				set(off++, i, idx);
			else if(idx > _negativeIndex)
				set(off++, i, idx - 1);
		}
	}

}
