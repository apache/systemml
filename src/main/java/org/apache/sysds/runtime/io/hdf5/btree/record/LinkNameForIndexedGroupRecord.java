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


package org.apache.sysds.runtime.io.hdf5.btree.record;

import org.apache.sysds.runtime.io.hdf5.Utils;
import org.apache.sysds.runtime.io.hdf5.exceptions.HdfException;

import java.nio.ByteBuffer;

public class LinkNameForIndexedGroupRecord extends BTreeRecord {

	private final long hash;
	private final ByteBuffer id;

	public LinkNameForIndexedGroupRecord(ByteBuffer bb) {
		if (bb.remaining() != 11) {
			throw new HdfException(
					"Invalid length buffer for LinkNameForIndexedGroupRecord. remaining bytes = " + bb.remaining());
		}

		hash = Utils.readBytesAsUnsignedLong(bb, 4);
		id = Utils.createSubBuffer(bb, 7);
	}

	public long getHash() {
		return hash;
	}

	public ByteBuffer getId() {
		return id;
	}

}
