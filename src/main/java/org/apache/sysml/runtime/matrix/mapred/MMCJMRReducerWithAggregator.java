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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.lops.MMCJ.MMCJType;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.MatrixValue;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.data.Pair;
import org.apache.sysml.runtime.matrix.data.TaggedFirstSecondIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.util.MapReduceTool;


public class MMCJMRReducerWithAggregator extends MMCJMRCombinerReducerBase 
	implements Reducer<TaggedFirstSecondIndexes, MatrixValue, Writable, Writable>
{	
	
	public static long MIN_CACHE_SIZE = 64*1024*1024; //64MB
	
	boolean useNativeBLAS = false; // Since MR is in maintenance mode
	
	private MMCJMRInputCache cache = null;
	private PartialAggregator aggregator = null;
	
	//variables to keep track of the flow
	private double prevFirstIndex=-1;
	private int prevTag=-1;
	
	//temporary variable
	private MatrixIndexes indexesbuffer=new MatrixIndexes();
	private MatrixValue valueBuffer=null;
	
	private boolean outputDummyRecords = false;
	
	@Override
	public void reduce(TaggedFirstSecondIndexes indexes, Iterator<MatrixValue> values,
			OutputCollector<Writable, Writable> out,
			Reporter report) throws IOException 
	{
		long start=System.currentTimeMillis();
		
		commonSetup(report);
		
		//perform aggregate (if necessary, only for binary cell)		
		MatrixValue aggregateValue=null;
		if( valueClass == MatrixBlock.class ) 
		{
			 //multiple blocks for same indexes impossible
			aggregateValue = values.next();	
		}
		else // MatrixCell.class
		{
			aggregateValue = performAggregateInstructions(indexes, values);	
			if(aggregateValue==null)
				return;
		}	
		
		int tag=indexes.getTag();
		long firstIndex=indexes.getFirstIndex();
		long secondIndex=indexes.getSecondIndex();
		
		//for a different k
		if( prevFirstIndex!=firstIndex ) 
		{
			cache.resetCache(true);
			prevFirstIndex=firstIndex;
		}
		else if(prevTag>tag)
			throw new RuntimeException("tag is not ordered correctly: "+prevTag+" > "+tag);
		
		prevTag=tag;
		
		//perform cross-product binagg
		processJoin(tag, secondIndex, aggregateValue);
		
		report.incrCounter(Counters.COMBINE_OR_REDUCE_TIME, System.currentTimeMillis()-start);
	}

	private void processJoin(int tag, long inIndex, MatrixValue inValue) 
		throws IOException
	{
		try
		{
			if( tag==0 ) //for the cached matrix
			{	
				cache.put(inIndex, inValue);
			}
			else //for the probing matrix
			{
				for(int i=0; i<cache.getCacheSize(); i++)
				{
					Pair<MatrixIndexes, MatrixValue> tmp = cache.get(i);
					
					if(tagForLeft==0) //left cached
					{
						//perform matrix multiplication
						indexesbuffer.setIndexes(tmp.getKey().getRowIndex(), inIndex);
						OperationsOnMatrixValues.performAggregateBinaryIgnoreIndexes(tmp.getValue(), inValue, valueBuffer, 
								                               (AggregateBinaryOperator)aggBinInstruction.getOperator(), useNativeBLAS);
					}
					else //right cached
					{
						//perform matrix multiplication
						indexesbuffer.setIndexes(inIndex, tmp.getKey().getColumnIndex());
						OperationsOnMatrixValues.performAggregateBinaryIgnoreIndexes(inValue, tmp.getValue(), valueBuffer, 
								                               (AggregateBinaryOperator)aggBinInstruction.getOperator(), useNativeBLAS);		
					}
					
					//aggregate block to output buffer or direct output
					if( aggBinInstruction.getMMCJType() == MMCJType.AGG ) {
						aggregator.aggregateToBuffer(indexesbuffer, valueBuffer, tagForLeft==0);
					}
					else { //MMCJType.NO_AGG
						collectFinalMultipleOutputs.collectOutput(indexesbuffer, valueBuffer, 0, cachedReporter);
						resultsNonZeros[0]+=valueBuffer.getNonZeros();
					}
				}
			}
		}
		catch(Exception ex)
		{
			throw new IOException(ex);
		}
	}

	@Override
	public void configure(JobConf job)
	{	
		//basic configure (incl inst parsing)
		super.configure(job);
		
		if(resultIndexes.length>1)
			throw new RuntimeException("MMCJMR only outputs one result");
		
		outputDummyRecords = MapReduceTool.getUniqueKeyPerTask(job, false).equals("0");
		
		try {
			valueBuffer=buffer;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
		
		//determine input and output cache size (cached input and cached aggregated output)
		//(prefer to cache input because accessed more frequently than output)
		long cacheSize = MRJobConfiguration.getMMCJCacheSize(job);
		long memBudget = (long)OptimizerUtils.getLocalMemBudget();
		long inBufferSize, outBufferSize;
		if( (memBudget - cacheSize) > MIN_CACHE_SIZE ){
			inBufferSize = cacheSize;
			outBufferSize = memBudget - cacheSize;
		}
		else{
			inBufferSize = memBudget - 2*MIN_CACHE_SIZE;
			outBufferSize = MIN_CACHE_SIZE;
		}
		
		try 
		{		
			//instantiate cached input
			if( tagForLeft==0 ){ //left cached
				cache = new MMCJMRInputCache(job, inBufferSize, dim1.getRows(), dim1.getCols(), 
				          dim1.getRowsPerBlock(), dim1.getColsPerBlock(), true, valueClass );
			}
			else { //right cached
				cache = new MMCJMRInputCache(job, inBufferSize, dim2.getRows(), dim2.getCols(), 
				          dim2.getRowsPerBlock(), dim2.getColsPerBlock(), false, valueClass );
			}
		
			//instantiate cached output
			if( aggBinInstruction.getMMCJType() == MMCJType.AGG ) {
				aggregator = new PartialAggregator(job, outBufferSize, dim1.getRows(), dim2.getCols(), 
						dim1.getRowsPerBlock(), dim2.getColsPerBlock(), (tagForLeft!=0), 
						(AggregateBinaryOperator) aggBinInstruction.getOperator(), valueClass);
			}
		} 
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		//LOG.info("Memory stats: "+Runtime.getRuntime().totalMemory()+", "+Runtime.getRuntime().freeMemory());
	}
	
	@Override
	public void close() throws IOException
	{
		//output the records in the outCache.
		if(cachedReporter!=null)
		{
			long start=System.currentTimeMillis(); //incl aggregator.close (delete files)
			if( aggBinInstruction.getMMCJType()==MMCJType.AGG )
				resultsNonZeros[0]+=aggregator.outputToHadoop(collectFinalMultipleOutputs, 0, cachedReporter);
			cachedReporter.incrCounter(Counters.COMBINE_OR_REDUCE_TIME, System.currentTimeMillis()-start);
		}
	    //aggregator.close();
		
		//handle empty block output (on first reduce task only)
		if( outputDummyRecords ) //required for rejecting empty blocks in mappers
		{
			long rlen = dim1.getRows();
			long clen = dim2.getCols();
			int brlen = dim1.getRowsPerBlock();
			int bclen = dim2.getColsPerBlock();
			MatrixIndexes tmpIx = new MatrixIndexes();
			MatrixBlock tmpVal = new MatrixBlock();
			for(long i=0, r=1; i<rlen; i+=brlen, r++)
				for(long j=0, c=1; j<clen; j+=bclen, c++)
				{
					int realBrlen=(int)Math.min((long)brlen, rlen-(r-1)*brlen);
					int realBclen=(int)Math.min((long)bclen, clen-(c-1)*bclen);
					tmpIx.setIndexes(r, c);
					//output empty blocks if necessary
					if(    aggBinInstruction.getMMCJType()==MMCJType.NO_AGG 
						|| !aggregator.getBufferMap().containsKey(tmpIx)   )
					{
						tmpVal.reset(realBrlen,realBclen);
						collectFinalMultipleOutputs.collectOutput(tmpIx, tmpVal, 0, cachedReporter);
					}
				}
		}
		
		cache.close();
		
		super.close();
	}
}
