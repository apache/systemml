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


package org.apache.sysml.runtime.matrix.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.mr.AggregateBinaryInstruction;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.MatrixValue;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.data.TaggedMatrixValue;
import org.apache.sysml.runtime.matrix.data.TripleIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;



public class MMRJMRReducer extends ReduceBase
implements Reducer<TripleIndexes, TaggedMatrixValue, MatrixIndexes, MatrixValue>
{
	
	private Reporter cachedReporter=null;
	private MatrixValue resultblock=null;
	private MatrixIndexes aggIndexes=new MatrixIndexes();
	private TripleIndexes prevIndexes=new TripleIndexes(-1, -1, -1);
	//aggregate binary instruction for the mmrj
	protected AggregateBinaryInstruction[] aggBinInstructions=null;
//	private MatrixIndexes indexBuf=new MatrixIndexes();
	private boolean useNativeBLAS = false; // Since MR is in maintenance mode
	
	@Override
	public void reduce(TripleIndexes triple, Iterator<TaggedMatrixValue> values,
			OutputCollector<MatrixIndexes, MatrixValue> out, Reporter report)
			throws IOException {
		long start=System.currentTimeMillis();
	//	System.out.println("~~~~~ group: "+triple);
		commonSetup(report);
		
		//output previous results if needed
		if(prevIndexes.getFirstIndex()!=triple.getFirstIndex() 
				|| prevIndexes.getSecondIndex()!=triple.getSecondIndex())
		{
		//	System.out.println("cacheValues before processReducerInstructions: \n"+cachedValues);
			//perform mixed operations
			processReducerInstructions();
			
	//		System.out.println("cacheValues before output: \n"+cachedValues);
			//output results
			outputResultsFromCachedValues(report);
			cachedValues.reset();
		}else
		{
			//clear the buffer
			for(AggregateBinaryInstruction aggBinInstruction: aggBinInstructions)
			{
//				System.out.println("cacheValues before remore: \n"+cachedValues);
				cachedValues.remove(aggBinInstruction.input1);
		//		System.out.println("cacheValues after remore: "+aggBinInstruction.input1+"\n"+cachedValues);
				cachedValues.remove(aggBinInstruction.input2);
		//		System.out.println("cacheValues after remore: "+aggBinInstruction.input2+"\n"+cachedValues);
			}
		}
		
		//perform aggregation first
		aggIndexes.setIndexes(triple.getFirstIndex(), triple.getSecondIndex());
		processAggregateInstructions(aggIndexes, values);
		
	//	System.out.println("cacheValues after aggregation: \n"+cachedValues);
		
		//perform aggbinary for this group
		for(AggregateBinaryInstruction aggBinInstruction: aggBinInstructions)
			processAggBinaryPerGroup(aggIndexes, aggBinInstruction);
		
	//	System.out.println("cacheValues after aggbinary: \n"+cachedValues);

		prevIndexes.setIndexes(triple);
		
		report.incrCounter(Counters.COMBINE_OR_REDUCE_TIME, System.currentTimeMillis()-start);
	}
	
	//perform pairwise aggregate binary, and added to the aggregates
	private void processAggBinaryPerGroup(MatrixIndexes indexes, AggregateBinaryInstruction aggBinInstruction) throws IOException
	{
		IndexedMatrixValue left = cachedValues.getFirst(aggBinInstruction.input1);
		IndexedMatrixValue right= cachedValues.getFirst(aggBinInstruction.input2);
	//	System.out.println("left: \n"+left.getValue());
	//	System.out.println("right: \n"+right.getValue());
		if(left!=null && right!=null)
		{
			try {
				resultblock=left.getValue().aggregateBinaryOperations(left.getValue(), right.getValue(), 
						resultblock, (AggregateBinaryOperator) aggBinInstruction.getOperator(), useNativeBLAS);
		//		System.out.println("resultblock: \n"+resultblock);
				IndexedMatrixValue out=cachedValues.getFirst(aggBinInstruction.output);
				if(out==null)
				{
					out=cachedValues.holdPlace(aggBinInstruction.output, valueClass);
					out.getIndexes().setIndexes(indexes);
					OperationsOnMatrixValues.startAggregation(out.getValue(), null, ((AggregateBinaryOperator) aggBinInstruction.getOperator()).aggOp, 
							resultblock.getNumRows(), resultblock.getNumColumns(), resultblock.isInSparseFormat(), false);
				}
				OperationsOnMatrixValues.incrementalAggregation(out.getValue(), null, resultblock, 
						((AggregateBinaryOperator) aggBinInstruction.getOperator()).aggOp, false);

		//		System.out.println("agg: \n"+out.getValue());
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}
	
	public void close() throws IOException
	{
		long start=System.currentTimeMillis();
		
//		System.out.println("cacheValues before processReducerInstructions: \n"+cachedValues);
		//perform mixed operations
		processReducerInstructions();
		
//		System.out.println("cacheValues before output: \n"+cachedValues);
		
		//output results
		outputResultsFromCachedValues(cachedReporter);
		
		if(cachedReporter!=null)
			cachedReporter.incrCounter(Counters.COMBINE_OR_REDUCE_TIME, System.currentTimeMillis()-start);
		super.close();
	}
	

	public void configure(JobConf job)
	{
		super.configure(job);
		try {
			aggBinInstructions = MRJobConfiguration.getAggregateBinaryInstructions(job);
		} catch (DMLRuntimeException e) {
			throw new RuntimeException(e);
		}
	}

}
