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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sysml.runtime.instructions.spark.data.RDDObject;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.spark.MatrixIndexingSPInstruction;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.sysml.runtime.util.IndexRange;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.utils.Statistics;
import org.apache.sysml.hops.AggBinaryOp.SparkAggType;

public class SPBatchIterator extends CPBatchIterator {
	// Input RDD / MatrixCharacteristics
	protected MatrixCharacteristics mcIn;
	protected JavaPairRDD<MatrixIndexes,MatrixBlock> in1;
	protected SparkExecutionContext sec;
	protected boolean canBatchFitInCP;
	
	public SPBatchIterator(ExecutionContext ec, String[] iterablePredicateVars, long batchSize, boolean canBatchFitInCP) throws DMLRuntimeException {
		super(ec, iterablePredicateVars, batchSize);
		// --------------------------------------------
		// Set the input RDD and matrix characteristics
		sec = ((SparkExecutionContext)ec);
		mcIn = sec.getMatrixCharacteristics(iterablePredicateVars[1]);
		in1 = sec.getBinaryBlockRDDHandleForVariable( iterablePredicateVars[1] );
		// --------------------------------------------
		this.canBatchFitInCP = canBatchFitInCP;
	}
	
	boolean firstBatch = true;
	
	@Override
	public MatrixObject next() {
		long startTime = DMLScript.STATISTICS ? System.nanoTime() : -1;
		MatrixObject currentBatch =  getNextBatch();
		Statistics.batchFetchingTimeInNext += DMLScript.STATISTICS ? (System.nanoTime() - startTime) : 0;
		return currentBatch;
	}
	
	public MatrixObject getNextBatch() {
		long beg = (currentBatchIndex * batchSize) % N + 1;
		currentBatchIndex++;
		long end = Math.min(N, beg + batchSize - 1);
		return getNextBatch(beg, end);
	}
	
	@SuppressWarnings("static-access")
	protected MatrixObject getNextBatch(long beg, long end) {
		long startTime = DMLScript.STATISTICS ? System.nanoTime() : -1;
		IndexRange ixrange = new IndexRange(beg, end, 1, X.getNumColumns());
		MatrixObject ret = null;
		try {
			// Get next batch by calling MatrixIndexingSPInstruction
			MatrixCharacteristics mcOut = new MatrixCharacteristics(end - beg + 1, 
					X.getNumColumns(), mcIn.getRowsPerBlock(), mcIn.getColsPerBlock());
			ret = new MatrixObject(ValueType.DOUBLE, OptimizerUtils.getUniqueTempFileName(), 
					new MatrixFormatMetaData(mcOut, OutputInfo.BinaryBlockOutputInfo, InputInfo.BinaryBlockInputInfo));
			
			if(MatrixIndexingSPInstruction.isSingleBlockLookup(mcIn, ixrange) || MatrixIndexingSPInstruction.isMultiBlockLookup(in1, mcIn, mcOut, ixrange)) {
				ret.acquireModify(MatrixIndexingSPInstruction.inmemoryIndexing(in1, mcIn, mcOut, ixrange));
				ret.release();
			}
			else {
				boolean isBlockAligned = OptimizerUtils.isIndexingRangeBlockAligned(ixrange, mcIn);
				SparkAggType aggType =  isBlockAligned ?  SparkAggType.NONE : SparkAggType.MULTI_BLOCK;
				JavaPairRDD<MatrixIndexes,MatrixBlock> outRDD = MatrixIndexingSPInstruction.generalCaseRightIndexing(in1, mcIn, mcOut, ixrange, aggType);
				if(canBatchFitInCP) {
					// Pulling batch to CP is especially useful during prefetching.
					ret.acquireModify(sec.toMatrixBlock(outRDD, (int)mcOut.getRows(), (int)mcOut.getCols(), (int)mcOut.getRowsPerBlock(), (int)mcOut.getColsPerBlock(), -1));
					ret.release();
				}
				else {
					ret.setRDDHandle(new RDDObject(outRDD, iterablePredicateVars[0]));
				}
			}
		} catch (DMLRuntimeException e) {
			throw new RuntimeException("Error while fetching a batch", e);
		}
		Statistics.batchFetchingTimeInIndexing += DMLScript.STATISTICS ? (System.nanoTime() - startTime) : 0;
		return ret;
	}
	
}
