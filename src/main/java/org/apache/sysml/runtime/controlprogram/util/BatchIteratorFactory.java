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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;

/**
 * This class decides based on memory budgets and configuration (which iterator to use).
 */
public class BatchIteratorFactory {
	
	private static final Log LOG = LogFactory.getLog(BatchIteratorFactory.class.getName());
	
	public static Iterable<Data> getBatchIterator(ExecutionContext ec, String[] iterablePredicateVars) throws DMLRuntimeException {
		MatrixObject X = (MatrixObject) ec.getVariable( iterablePredicateVars[1] );
		MatrixCharacteristics mcX = ec.getMatrixCharacteristics( iterablePredicateVars[1] );
		long batchSize = 0;
		try {
			batchSize = Long.parseLong(iterablePredicateVars[3]); 
		} catch(NumberFormatException e) {  
			batchSize = ((ScalarObject) ec.getVariable( iterablePredicateVars[3] )).getLongValue();
		}
		MatrixCharacteristics maxMCBatch = new MatrixCharacteristics(batchSize, X.getNumColumns(), ConfigurationManager.getBlocksize(), ConfigurationManager.getBlocksize());
		if(ec instanceof SparkExecutionContext) {
			boolean canBatchFitInCP = canBatchFitInCP(maxMCBatch);
			if(canBatchFitInPrefetchBudget(maxMCBatch)) {
				LOG.info("Prefetching is enabled"); // Setting LOG to warn for experiments. 
				return (Iterable<Data>) (new SPPrefetchBatchIterator(ec, iterablePredicateVars, batchSize, canBatchFitInCP));
			}
			else if(canBatchAndXFitInCP(maxMCBatch, mcX)) {
				warnIfSmallerPrefetchBudget(maxMCBatch);
				return (Iterable<Data>) (new CPBatchIterator(ec, iterablePredicateVars, batchSize));
			}
			else {
				warnIfSmallerPrefetchBudget(maxMCBatch);
				return (Iterable<Data>) (new SPBatchIterator(ec, iterablePredicateVars, batchSize, canBatchFitInCP));
			}
		}
		else {
			if(!canBatchAndXFitInCP(maxMCBatch, mcX)) 
				LOG.warn("The memory budget provided is small. You should consider running with Spark backend.");
			return (Iterable<Data>) (new CPBatchIterator(ec, iterablePredicateVars, batchSize));
		}
	}
	
	private static void warnIfSmallerPrefetchBudget(MatrixCharacteristics maxMCBatch) {
		long prefetchBudget = getPrefetchBudget(maxMCBatch);
		if(prefetchBudget > 0) {
			long MB = 1000000;
			double requiredBudget = OptimizerUtils.estimateSize(maxMCBatch) *2 / MB;
			LOG.warn("Prefetching is disabled as the required budget is less than specified budget. (Hint: To enable prefetching, please set " + DMLConfig.PREFETCH_MEM_BUDGET + " to " + requiredBudget + "mb.");
		}
	}
	
	private static boolean canBatchAndXFitInCP(MatrixCharacteristics maxMCBatch, MatrixCharacteristics mcX) {
		return (OptimizerUtils.estimateSize(maxMCBatch) + OptimizerUtils.estimateSize(mcX)) < OptimizerUtils.getLocalMemBudget()/2;
	}
	
	private static boolean canBatchFitInCP(MatrixCharacteristics maxMCBatch) {
		return OptimizerUtils.estimateSize(maxMCBatch) < OptimizerUtils.getLocalMemBudget()/2;
	}
	
	private static boolean canBatchFitInPrefetchBudget(MatrixCharacteristics maxMCBatch) {
		return OptimizerUtils.estimateSize(maxMCBatch) < getPrefetchBudget(maxMCBatch)/2;
	}
	
	private static long getPrefetchBudget(MatrixCharacteristics maxMCBatch) {
		long MB = 1000000;
		long prefetchMemoryBudget = Math.max(0, (long)(ConfigurationManager.getDMLConfig().getDoubleValue(DMLConfig.PREFETCH_MEM_BUDGET) * MB));
		return prefetchMemoryBudget;
	}
}
