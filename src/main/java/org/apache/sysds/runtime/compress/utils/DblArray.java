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

import java.util.Arrays;

/**
 * Helper class used for bitmap extraction.
 */
public class DblArray {
	private double[] _arr;
	private int _hash = 0;

	public DblArray() {
		_arr = null;
	}

	public DblArray(double[] arr) {
		_arr = arr;
	}

	private DblArray(double[] arr, int hash) {
		_arr = arr;
		_hash = hash;
	}

	public DblArray(DblArray that) {
		this(Arrays.copyOf(that._arr, that._arr.length), that.hashCode());
	}

	public double[] getData() {
		return _arr;
	}

	public void resetHash() {
		_hash = 0;
	}

	public boolean isEmpty() {
		return _arr == null;
	}

	@Override
	public int hashCode() {
		if(_hash != 0)
			return _hash;
		_hash = _arr == null ? 0 : Arrays.hashCode(_arr);
		return _hash;
	}

	public boolean equals(DblArray that) {
		return this.hashCode() == that.hashCode() && Arrays.equals(this._arr, that._arr);
	}

	@Override
	public boolean equals(Object o) {
		return o instanceof DblArray && this.equals((DblArray) o);
	}

	@Override
	public String toString() {
		return Arrays.toString(_arr);
	}
}
