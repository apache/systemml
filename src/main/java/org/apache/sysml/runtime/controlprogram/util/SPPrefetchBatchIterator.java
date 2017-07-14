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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.utils.Statistics;

/**
 * Prefetching of batches is done via producer-consumer pattern.
 * The producer (PrefetchingThread) is started asynchronously and puts the batch into  prefetchQueue.
 * The consumer (SPPrefetchBatchIterator) in called by ForProgramBlock via "for( Data iterVar : genericIterator ) { ... }".
 */
public class SPPrefetchBatchIterator extends SPBatchIterator {
	// Create a blocking queue of capacity 1.
	// PrefetchingThread fills this queue and next() fetches from the queue.
	private BlockingQueue<MatrixObject> prefetchQueue = new ArrayBlockingQueue<MatrixObject>(1);
	
	public SPPrefetchBatchIterator(ExecutionContext ec,
			String[] iterablePredicateVars, long batchSize,
			boolean canBatchFitInCP)
			throws DMLRuntimeException {
		super(ec, iterablePredicateVars, batchSize, canBatchFitInCP);
		(new Thread(new PrefetchingThread(prefetchQueue, this, numBatches))).start();
	}

	@Override
	public MatrixObject next() {
		long startTime = DMLScript.STATISTICS ? System.nanoTime() : -1;
		MatrixObject currentBatch = null; 
		long startTime1 = DMLScript.STATISTICS ? System.nanoTime() : -1;
		try {
			// This retrieves and removes the head of this queue, waiting if necessary until an element becomes available.
			currentBatch = prefetchQueue.take();
		} catch (InterruptedException e) {
			throw new RuntimeException("Error while fetching from queue", e);
		}
		long fetchTime = DMLScript.STATISTICS ? (System.nanoTime() - startTime1) : 0;
		Statistics.batchPrefetchWaitTime += fetchTime;
		if(firstBatch) {
			Statistics.batchFirstBatchFetchTime += fetchTime;
			firstBatch = false;
		}
		Statistics.batchFetchingTimeInNext += DMLScript.STATISTICS ? (System.nanoTime() - startTime) : 0;
		return currentBatch;
	}
}
