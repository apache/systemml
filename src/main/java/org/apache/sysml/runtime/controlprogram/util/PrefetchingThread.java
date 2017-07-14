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

import java.util.concurrent.BlockingQueue;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;

/**
 * Prefetching of batches is done via producer-consumer pattern.
 * The producer (PrefetchingThread) is started asynchronously and puts the batch into  prefetchQueue.
 * The consumer (SPPrefetchBatchIterator) in called by ForProgramBlock via "for( Data iterVar : genericIterator ) { ... }".
 */
class PrefetchingThread implements Runnable {
	private BlockingQueue<MatrixObject> prefetchQueue;
	private long numBatches; private SPBatchIterator iter;
	public PrefetchingThread(BlockingQueue<MatrixObject> prefetchQueue, SPBatchIterator iter, long numBatches) {
		this.prefetchQueue = prefetchQueue;
		this.iter = iter;
		this.numBatches = numBatches;
	}
	@Override
	public void run() {
		try {
			for(int i = 0; i < numBatches; i++) {
				// Note: This inserts the specified element into this queue, 
				// waiting if necessary for space to become available.
				prefetchQueue.put(iter.getNextBatch());
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Prefetching thread was interrupted.", e);
		}
	}
}
