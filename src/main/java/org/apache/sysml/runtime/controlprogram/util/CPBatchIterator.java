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

package org.apache.sysml.runtime.controlprogram.util;

import java.util.Iterator;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.sysml.runtime.util.IndexRange;
import org.apache.sysml.utils.Statistics;

public class CPBatchIterator implements Iterator<Data>, Iterable<Data> {
	
	protected MatrixObject X = null;
	protected long batchSize;
	protected long numBatches;
	protected long currentBatchIndex = 0;
	protected long N;
	protected String XName;
	protected String[] iterablePredicateVars;
	
	
	public CPBatchIterator(ExecutionContext ec, String[] iterablePredicateVars, long batchSize) {
		X = (MatrixObject) ec.getVariable( iterablePredicateVars[1] );
		this.iterablePredicateVars = iterablePredicateVars;
		// assumption: known at runtime
		N = X.getNumRows();
		this.batchSize = batchSize;
		numBatches = (long) Math.ceil(  ((double)N) / batchSize);
	}

	@Override
	public Iterator<Data> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return currentBatchIndex < numBatches;
	}

	@Override
	public MatrixObject next() {
		long startTime = DMLScript.STATISTICS ? System.nanoTime() : -1;
		long beg = (currentBatchIndex * batchSize) % N + 1;
		currentBatchIndex++;
		long end = Math.min(N, beg + batchSize - 1);
		IndexRange ixrange = new IndexRange(beg-1, end-1, 0, X.getNumColumns()-1);
		MatrixObject ret = null;
		try {
			// Perform the operation (no prefetching for CP as X.acquireRead() might or might not fit on memory): 
			// # Get next batch
		    // beg = ((i-1) * batch_size) %% N + 1
		    // end = min(N, beg + batch_size - 1)
		    // X_batch = X[beg:end,]
			MatrixBlock matBlock = X.acquireRead();
			MatrixBlock resultBlock = matBlock.sliceOperations(ixrange, new MatrixBlock());
			X.release();
			// Return X_batch as MatrixObject
			MatrixCharacteristics mc = new MatrixCharacteristics(end - beg + 1, 
					X.getNumColumns(), ConfigurationManager.getBlocksize(), ConfigurationManager.getBlocksize());
			ret = new MatrixObject(ValueType.DOUBLE, OptimizerUtils.getUniqueTempFileName(), 
					new MatrixFormatMetaData(mc, OutputInfo.BinaryBlockOutputInfo, InputInfo.BinaryBlockInputInfo));
			ret.acquireModify(resultBlock);
			ret.release();
		} catch (DMLRuntimeException e) {
			throw new RuntimeException("Error while fetching a batch", e);
		}
		
		if(DMLScript.STATISTICS) {
			long endTime = System.nanoTime();
			Statistics.batchFetchingTimeInIndexing += endTime - startTime;
			Statistics.batchFetchingTimeInNext += Statistics.batchFetchingTimeInIndexing; 
		}
		return ret;
	}
}
