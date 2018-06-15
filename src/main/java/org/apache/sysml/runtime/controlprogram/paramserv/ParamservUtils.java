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

package org.apache.sysml.runtime.controlprogram.paramserv;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.CacheableData;
import org.apache.sysml.runtime.controlprogram.caching.FrameObject;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.ListObject;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MetaDataFormat;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.OutputInfo;

public class ParamservUtils {

	/**
	 * Deep copy the list object
	 *
	 * @param lo list object
	 * @return a new copied list object
	 */
	public static ListObject copyList(ListObject lo) {
		if (lo.getLength() == 0) {
			return lo;
		}
		List<Data> newData = IntStream.range(0, lo.getLength()).mapToObj(i -> {
			Data oldData = lo.slice(i);
			if (oldData instanceof MatrixObject) {
				MatrixObject mo = (MatrixObject) oldData;
				return sliceMatrix(mo, 1, mo.getNumRows());
			} else if (oldData instanceof ListObject || oldData instanceof FrameObject) {
				throw new DMLRuntimeException("Copy list: does not support list or frame.");
			} else {
				return oldData;
			}
		}).collect(Collectors.toList());
		return new ListObject(newData, lo.getNames());
	}

	public static void cleanupListObject(ExecutionContext ec, String lName) {
		ListObject lo = (ListObject) ec.removeVariable(lName);
		lo.getData().forEach(ParamservUtils::cleanupData);
	}

	public static void cleanupData(Data data) {
		if (!(data instanceof CacheableData))
			return;
		CacheableData<?> cd = (CacheableData<?>) data;
		cd.enableCleanup(true);
		cd.clearData();
	}

	public static MatrixObject newMatrixObject() {
		return new MatrixObject(Expression.ValueType.DOUBLE, OptimizerUtils.getUniqueTempFileName(), new MetaDataFormat(new MatrixCharacteristics(-1, -1, -1, -1), OutputInfo.BinaryBlockOutputInfo, InputInfo.BinaryBlockInputInfo));
	}

	/**
	 * Slice the matrix
	 *
	 * @param mo input matrix
	 * @param rl low boundary
	 * @param rh high boundary
	 * @return new sliced matrix
	 */
	public static MatrixObject sliceMatrix(MatrixObject mo, long rl, long rh) {
		MatrixObject result = newMatrixObject();
		MatrixBlock tmp = mo.acquireRead();
		result.acquireModify(tmp.slice((int) rl - 1, (int) rh - 1));
		mo.release();
		result.release();
		result.enableCleanup(false);
		return result;
	}

	public static MatrixBlock generatePermutation(int numEntries) {
		// Create a sequence and sample w/o replacement
		MatrixBlock seq = MatrixBlock.seqOperations(1, numEntries, 1);
		MatrixBlock sample = MatrixBlock.sampleOperations(numEntries, numEntries, false, -1);

		// Combine the sequence and sample as a table
		MatrixBlock permutation = new MatrixBlock(numEntries, numEntries, true);
		seq.ctableOperations(null, sample, 1.0, permutation);
		return permutation;
	}
}
