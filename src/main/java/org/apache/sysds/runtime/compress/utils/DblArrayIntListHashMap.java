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

package org.apache.sysds.runtime.compress.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DblArrayIntListHashMap {

	protected static final Log LOG = LogFactory.getLog(DblArrayIntListHashMap.class.getName());

	protected static final int INIT_CAPACITY = 8;
	protected static final int RESIZE_FACTOR = 2;
	protected static final float LOAD_FACTOR = 0.5f;
	public static int hashMissCount = 0;

	protected int _size = -1;

	protected DArrayIListEntry[] _data = null;

	public int size() {
		return _size;
	}

	public DblArrayIntListHashMap() {
		_data = new DArrayIListEntry[INIT_CAPACITY];
		_size = 0;
	}

	public DblArrayIntListHashMap(int init_capacity) {
		_data = new DArrayIListEntry[getPow2(init_capacity)];
		_size = 0;
	}

	private int getPow2(int x) {
		x = x - 1;
		x |= x >> 1;
		x |= x >> 2;
		x |= x >> 4;
		x |= x >> 5;
		x |= x >> 6;
		x |= x >> 8;
		x |= x >> 9;
		x |= x >> 10;
		x |= x >> 11;
		x |= x >> 12;
		x |= x >> 13;
		x |= x >> 14;
		x |= x >> 15;
		x |= x >> 16;
		return Math.max(x + 1, 4);
	}

	public IntArrayList get(DblArray key) {
		// probe for early abort
		if(_size == 0)
			return null;
		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// find entry

		while(_data[ix] != null && !_data[ix].keyEquals(key)) {
			hash = Integer.hashCode(hash + 1); // hash of hash
			ix = indexFor(hash, _data.length);
			hashMissCount++;
		}
		DArrayIListEntry e = _data[ix];
		if(e != null)
			return e.value;
		return null;
	}

	private void appendValue(DblArray key, IntArrayList value) {
		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// add new table entry (constant time)
		while(_data[ix] != null && !_data[ix].keyEquals(key)) {
			hash = Integer.hashCode(hash + 1); // hash of hash
			ix = indexFor(hash, _data.length);
			hashMissCount++;
		}
		_data[ix] = new DArrayIListEntry(key, value);
		_size++;
	}

	public void appendValue(DblArray key, int value) {
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		while(_data[ix] != null && !_data[ix].keyEquals(key)) {
			hash = Integer.hashCode(hash + 1); // hash of hash
			ix = indexFor(hash, _data.length);
			hashMissCount++;
		}

		DArrayIListEntry e = _data[ix];
		if(e == null) {
			final IntArrayList lstPtr = new IntArrayList();
			lstPtr.appendValue(value);
			_data[ix] = new DArrayIListEntry(new DblArray(key), lstPtr);
			_size++;
		}
		else {
			final IntArrayList lstPtr = e.value;
			lstPtr.appendValue(value);
		}

		// resize if necessary
		if(_size >= LOAD_FACTOR * _data.length)
			resize();
	}

	public List<DArrayIListEntry> extractValues() {
		List<DArrayIListEntry> ret = new ArrayList<>();

		for(DArrayIListEntry e : _data)
			if(e != null)
				ret.add(e);

		// Collections.sort(ret);
		return ret;
	}

	private void resize() {
		// check for integer overflow on resize
		if(_data.length > Integer.MAX_VALUE / RESIZE_FACTOR)
			return;

		// resize data array and copy existing contents
		DArrayIListEntry[] olddata = _data;
		_data = new DArrayIListEntry[_data.length * RESIZE_FACTOR];
		_size = 0;

		// rehash all entries
		for(DArrayIListEntry e : olddata)
			if(e != null)
				appendValue(e.key, e.value);
	}

	public void reset() {
		Arrays.fill(_data, null);
		_size = 0;
	}

	public void reset(int size) {
		int newSize = getPow2(size);
		if(newSize > _data.length) {
			_data = new DArrayIListEntry[newSize];
		}
		else {
			Arrays.fill(_data, null);
			// only allocate new if the size is smaller than 2x
			if(size < _data.length / 2)
				_data = new DArrayIListEntry[newSize];
		}
		_size = 0;
	}

	protected static int hash(DblArray key) {
		int h = key.hashCode();

		// This function ensures that hashCodes that differ only by
		// constant multiples at each bit position have a bounded
		// number of collisions (approximately 8 at default load factor).
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	protected static int indexFor(int h, int length) {
		return h & (length - 1);
	}

	public class DArrayIListEntry {
		public DblArray key;
		public IntArrayList value;

		public DArrayIListEntry(DblArray ekey, IntArrayList evalue) {
			key = ekey;
			value = evalue;
		}

		@Override
		public String toString() {
			return key + ":" + value;

		}

		public boolean keyEquals(DblArray keyThat) {
			return key.equals(keyThat);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getSimpleName() + this.hashCode());
		sb.append("   " + _size);
		for(int i = 0; i < _data.length; i++) {
			DArrayIListEntry ent = _data[i];
			if(ent != null) {

				sb.append("\n");
				sb.append("id:" + i);
				sb.append("[");
				sb.append(ent);
				sb.append("]");
			}
		}
		return sb.toString();
	}
}
