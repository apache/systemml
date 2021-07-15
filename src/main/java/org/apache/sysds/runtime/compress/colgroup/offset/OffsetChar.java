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
package org.apache.sysds.runtime.compress.colgroup.offset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.sysds.runtime.compress.DMLCompressionException;
import org.apache.sysds.utils.MemoryEstimates;

public class OffsetChar extends AOffset {

	private final static int maxV = (int) Character.MAX_VALUE;

	private final char[] offsets;
	private final int offsetToFirst;

	public OffsetChar(int[] indexes) {
		this(indexes, 0, indexes.length);
	}

	public OffsetChar(int[] indexes, int apos, int alen) {
		int endSize = 0;
		offsetToFirst = indexes[apos];
		int ov = offsetToFirst;
		for(int i = apos+1; i < alen; i++) {
			final int nv = indexes[i];
			endSize += 1 + (nv - ov) / maxV;
			ov = nv;
		}
		offsets = new char[endSize];
		ov = offsetToFirst;
		int p = 0;

		for(int i =  apos+1; i < alen; i++) {
			final int nv = indexes[i];
			final int offsetSize = (nv - ov);
			if(offsetSize == 0)
				throw new DMLCompressionException("Invalid difference between cells :\n" + Arrays.toString(indexes));
			final int div = offsetSize / maxV;
			final int mod = offsetSize % maxV;
			if(mod == 0) {
				p += div - 1; // skip values
				offsets[p++] = (char) maxV;
			}
			else {
				p += div; // skip values
				offsets[p++] = (char) (mod);
			}

			ov = nv;
		}
	}

	private OffsetChar(char[] offsets, int offsetToFirst) {
		this.offsets = offsets;
		this.offsetToFirst = offsetToFirst;
	}

	@Override
	public IterateCharOffset getIterator() {
		return new IterateCharOffset();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(OffsetFactory.OFF_TYPE.CHAR.ordinal());
		out.writeInt(offsetToFirst);
		out.writeInt(offsets.length);
		for(char o : offsets)
			out.writeChar(o);
	}

	@Override
	public long getInMemorySize() {
		return getInMemorySize(offsets.length);
	}

	@Override
	public long getExactSizeOnDisk() {
		return 1 + 4 + 4 + offsets.length * 2;
	}

	@Override
	public int getSize() {
		int size = 1;
		for(char b : offsets) {
			if(b != 0)
				size++;
		}
		return size;
	}

	public static OffsetChar readFields(DataInput in) throws IOException {
		int offsetToFirst = in.readInt();
		int offsetsLength = in.readInt();
		char[] offsets = new char[offsetsLength];
		for(int i = 0; i < offsetsLength; i++) {
			offsets[i] = in.readChar();
		}
		return new OffsetChar(offsets, offsetToFirst);
	}

	public static long getInMemorySize(int length) {
		long size = 16 + 4 + 8; // object header plus int plus reference
		size += MemoryEstimates.charArrayCost(length - 1);
		return size;
	}

	private class IterateCharOffset extends AIterator {

		private IterateCharOffset() {
			super(0, 0, offsetToFirst);
		}

		private IterateCharOffset(int index, int dataIndex, int offset) {
			super(index, dataIndex, offset);
		}

		@Override
		public void next() {
			if(index >= offsets.length) {
				index++;
				dataIndex++;
				return;
			}
			final char v = offsets[index++];
			if(v == 0) {
				offset += maxV;
				next();
			}
			else {
				dataIndex++;
				offset += v;
			}
		}

		@Override
		public boolean hasNext() {
			return index <= offsets.length;
		}

		@Override
		public IterateCharOffset clone() {
			return new IterateCharOffset(index, dataIndex, offset);
		}
	}

}
