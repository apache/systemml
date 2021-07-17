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
import java.util.Comparator;

/**
 * This class provides a memory-efficient replacement for {@code HashMap<Double,IntArrayList>} for restricted use cases.
 * 
 */
public class DoubleIntListHashMap {
	protected static final int INIT_CAPACITY = 8;
	protected static final int RESIZE_FACTOR = 2;
	protected static final float LOAD_FACTOR = 0.50f;
	protected int _size = -1;
	private DIListEntry[] _data = null;
	public static int hashMissCount = 0;

	public DoubleIntListHashMap() {
		_data = new DIListEntry[INIT_CAPACITY];
		_size = 0;
	}

	public DoubleIntListHashMap(int init_capacity) {
		_data = new DIListEntry[init_capacity];
		_size = 0;
	}

	public int size() {
		return _size;
	}

	public IntArrayList get(double key) {
		// probe for early abort
		if(_size == 0)
			return null;

		// compute entry index position
		int hash = hash(key);
		return getHash(key, hash);
	}

	private IntArrayList getHash(double key, int hash) {
		int ix = indexFor(hash, _data.length);
		return getHashIndex(key, ix);
	}

	private IntArrayList getHashIndex(double key, int ix) {

		// find entry
		for(DIListEntry e = _data[ix]; e != null; e = e.next) {
			if(e.key == key) {
				return e.value;
			}
		}

		return null;
	}

	public void appendValue(double key, IntArrayList value) {
		// compute entry index position
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);

		// add new table entry (constant time)
		DIListEntry enew = new DIListEntry(key, value);
		enew.next = _data[ix]; // colliding entries / null
		_data[ix] = enew;
		if(enew.next != null && enew.next.key == key) {
			enew.next = enew.next.next;
			_size--;
		}
		_size++;

		// resize if necessary
		if(_size >= LOAD_FACTOR * _data.length)
			resize();
	}

	public void appendValue(double key, int value) {
		int hash = hash(key);
		int ix = indexFor(hash, _data.length);
		IntArrayList lstPtr = null; // The list to add the value to.
		if(_data[ix] == null) {
			lstPtr = new IntArrayList();
			_data[ix] = new DIListEntry(key, lstPtr);
			_size++;
		}
		else {
			for(DIListEntry e = _data[ix]; e != null; e = e.next) {
				if(e.key == key) {
					lstPtr = e.value;
					break;
				}
				else if(e.next == null) {
					lstPtr = new IntArrayList();
					// Swap to place the new value, in front.
					DIListEntry eOld = _data[ix];
					_data[ix] = new DIListEntry(key, lstPtr);
					_data[ix].next = eOld;
					_size++;
					break;
				}
			}
		}
		lstPtr.appendValue(value);
		if(_size >= LOAD_FACTOR * _data.length)
			resize();
	}

	public ArrayList<DIListEntry> extractValues() {
		ArrayList<DIListEntry> ret = new ArrayList<>();
		for(DIListEntry e : _data) {
			if(e != null) {
				while(e.next != null) {
					ret.add(e);
					e = e.next;
				}
				ret.add(e);
			}
		}
		// Collections.sort(ret);

		return ret;
	}

	private void resize() {
		// check for integer overflow on resize
		if(_data.length > Integer.MAX_VALUE / RESIZE_FACTOR)
			return;

		// resize data array and copy existing contents
		DIListEntry[] olddata = _data;
		_data = new DIListEntry[_data.length * RESIZE_FACTOR];
		_size = 0;

		// rehash all entries
		for(DIListEntry e : olddata) {
			if(e != null) {
				while(e.next != null) {
					appendValue(e.key, e.value);
					e = e.next;
				}
				appendValue(e.key, e.value);
			}
		}
	}

	private static int hash(double key) {
		// return (int) key;

		// basic double hash code (w/o object creation)
		long bits = Double.doubleToRawLongBits(key);
		int h = (int) (bits ^ (bits >>> 32));

		// This function ensures that hashCodes that differ only by
		// constant multiples at each bit position have a bounded
		// number of collisions (approximately 8 at default load factor).
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	private static int indexFor(int h, int length) {
		return h & (length - 1);
	}

	public class DIListEntry implements Comparator<DIListEntry>, Comparable<DIListEntry> {
		public double key = Double.MAX_VALUE;
		public IntArrayList value = null;
		public DIListEntry next = null;

		public DIListEntry(double ekey, IntArrayList evalue) {
			key = ekey;
			value = evalue;
			next = null;
		}

		@Override
		public int compareTo(DIListEntry o) {
			return compare(this, o);
		}

		@Override
		public int compare(DIListEntry arg0, DIListEntry arg1) {
			return Double.compare(arg0.key, arg1.key);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("[" + key + ", ");
			sb.append(value + ", ");
			sb.append(next + "]");
			return sb.toString();
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getSimpleName() + this.hashCode());
		for(int i = 0; i < _data.length; i++)
			if(_data[i] != null)
				sb.append(", " + _data[i]);
		return sb.toString();
	}
}
