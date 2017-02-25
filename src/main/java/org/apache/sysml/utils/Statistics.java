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

package org.apache.sysml.utils;

import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.util.hash.Hash;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.controlprogram.caching.CacheStatistics;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.MRJobInstruction;
import org.apache.sysml.runtime.instructions.cp.FunctionCallCPInstruction;
import org.apache.sysml.runtime.instructions.spark.SPInstruction;
import org.apache.sysml.runtime.matrix.data.LibMatrixDNN;

/**
 * This class captures all statistics.
 */
public class Statistics 
{
	private static AtomicLong statisticsOverhead = new AtomicLong(0);

	private static long compileStartTime = 0;
	private static long compileEndTime = 0;
	
	private static long execStartTime = 0;
	private static long execEndTime = 0;

	// number of compiled/executed MR jobs
	private static int iNoOfExecutedMRJobs = 0;
	private static int iNoOfCompiledMRJobs = 0;

	// number of compiled/executed SP instructions
	private static int iNoOfExecutedSPInst = 0;
	private static int iNoOfCompiledSPInst = 0;
	
	private static int iNoOfExecutedGPUInst = 0;

	//JVM stats
	private static long jitCompileTime = 0; //in milli sec
	private static long jvmGCTime = 0; //in milli sec
	private static long jvmGCCount = 0; //count
	
	//HOP DAG recompile stats (potentially high update frequency)
	private static AtomicLong hopRecompileTime = new AtomicLong(0); //in nano sec
	private static AtomicLong hopRecompilePred = new AtomicLong(0); //count
	private static AtomicLong hopRecompileSB = new AtomicLong(0);   //count

	//CODEGEN
	private static AtomicLong codegenCompileTime = new AtomicLong(0); //in nano
	private static AtomicLong codegenClassCompileTime = new AtomicLong(0); //in nano
	private static AtomicLong codegenHopCompile = new AtomicLong(0); //count
	private static AtomicLong codegenCPlanCompile = new AtomicLong(0); //count
	private static AtomicLong codegenClassCompile = new AtomicLong(0); //count
	private static AtomicLong codegenPlanCacheHits = new AtomicLong(0); //count
	private static AtomicLong codegenPlanCacheTotal = new AtomicLong(0); //count
	
	//Function recompile stats 
	private static AtomicLong funRecompileTime = new AtomicLong(0); //in nano sec
	private static AtomicLong funRecompiles = new AtomicLong(0); //count
	
	//Spark-specific stats
	private static long sparkCtxCreateTime = 0; 
	private static AtomicLong sparkParallelize = new AtomicLong(0L);
	private static AtomicLong sparkParallelizeCount = new AtomicLong(0L);
	private static AtomicLong sparkCollect = new AtomicLong(0L);
	private static AtomicLong sparkCollectCount = new AtomicLong(0L);
	private static AtomicLong sparkBroadcast = new AtomicLong(0L);
	private static AtomicLong sparkBroadcastCount = new AtomicLong(0L);

	//PARFOR optimization stats 
	private static long parforOptTime = 0; //in milli sec
	private static long parforOptCount = 0; //count
	private static long parforInitTime = 0; //in milli sec
	private static long parforMergeTime = 0; //in milli sec
	
	//heavy hitter counts and times 
	private static HashMap<String,Long> _cpInstTime   =  new HashMap<String, Long>();
	private static HashMap<String,Long> _cpInstCounts =  new HashMap<String, Long>();
	private static HashMap<String, HashMap<String, Long>> _cpInstMiscTime = new HashMap<String, HashMap<String, Long>> ();
	private static HashMap<String, HashMap<String, Long>> _cpInstMiscCount = new HashMap<String, HashMap<String, Long>> ();


	private static AtomicLong lTotalUIPVar = new AtomicLong(0);
	private static AtomicLong lTotalLix = new AtomicLong(0);
	private static AtomicLong lTotalLixUIP = new AtomicLong(0);
	
	public static long cudaInitTime = 0;
	public static long cudaLibrariesInitTime = 0;
	public static AtomicLong cudaSparseToDenseTime = new AtomicLong(0);		// Measures time spent in converting sparse matrix block to dense
	public static AtomicLong cudaSparseToDenseCount = new AtomicLong(0);
	public static AtomicLong cudaDenseToSparseTime = new AtomicLong(0);		// Measures time spent in converting dense matrix block to sparse
	public static AtomicLong cudaDenseToSparseCount = new AtomicLong(0);
	public static AtomicLong cudaSparseConversionTime = new AtomicLong(0);	// Measures time spent in converting between sparse block types
	public static AtomicLong cudaSparseConversionCount = new AtomicLong(0);
	public static AtomicLong cudaAllocTime = new AtomicLong(0);
	public static AtomicLong cudaDeAllocTime = new AtomicLong(0);
	public static AtomicLong cudaToDevTime = new AtomicLong(0);
	public static AtomicLong cudaFromDevTime = new AtomicLong(0);
	public static AtomicLong cudaAllocCount = new AtomicLong(0);
	public static AtomicLong cudaDeAllocCount = new AtomicLong(0);
	public static AtomicLong cudaToDevCount = new AtomicLong(0);
	public static AtomicLong cudaFromDevCount = new AtomicLong(0);
	public static AtomicLong cudaEvictionCount = new AtomicLong(0);
	
	public static synchronized void setNoOfExecutedMRJobs(int iNoOfExecutedMRJobs) {
		Statistics.iNoOfExecutedMRJobs = iNoOfExecutedMRJobs;
	}

	public static synchronized int getNoOfExecutedMRJobs() {
		return iNoOfExecutedMRJobs;
	}
	
	public static synchronized void incrementNoOfExecutedMRJobs() {
		iNoOfExecutedMRJobs ++;
	}
	
	public static synchronized void decrementNoOfExecutedMRJobs() {
		iNoOfExecutedMRJobs --;
	}

	public static synchronized void setNoOfCompiledMRJobs(int numJobs) {
		iNoOfCompiledMRJobs = numJobs;
	}

	public static synchronized int getNoOfCompiledMRJobs() {
		return iNoOfCompiledMRJobs;
	}
	
	public static synchronized void incrementNoOfCompiledMRJobs() {
		iNoOfCompiledMRJobs ++;
	}
	
	
	public static synchronized void setNoOfExecutedGPUInst(int numJobs) {
		iNoOfExecutedGPUInst = numJobs;
	}
	
	public static synchronized void incrementNoOfExecutedGPUInst() {
		iNoOfExecutedGPUInst ++;
	}
	
	public static synchronized int getNoOfExecutedGPUInst() {
		return iNoOfExecutedGPUInst;
	}

	public static synchronized void setNoOfExecutedSPInst(int numJobs) {
		iNoOfExecutedSPInst = numJobs;
	}
	
	public static synchronized int getNoOfExecutedSPInst() {
		return iNoOfExecutedSPInst;
	}
	
	public static synchronized void incrementNoOfExecutedSPInst() {
		iNoOfExecutedSPInst ++;
	}
	
	public static synchronized void decrementNoOfExecutedSPInst() {
		iNoOfExecutedSPInst --;
	}
	
	public static synchronized void setNoOfCompiledSPInst(int numJobs) {
		iNoOfCompiledSPInst = numJobs;
	}

	public static synchronized int getNoOfCompiledSPInst() {
		return iNoOfCompiledSPInst;
	}

	public static synchronized void incrementNoOfCompiledSPInst() {
		iNoOfCompiledSPInst ++;
	}
	
	public static long getTotalUIPVar() {
		return lTotalUIPVar.get();
	}

	public static void incrementTotalUIPVar() {
		lTotalUIPVar.incrementAndGet();
	}

	public static long getTotalLixUIP() {
		return lTotalLixUIP.get();
	}

	public static void incrementTotalLixUIP() {
		lTotalLixUIP.incrementAndGet();
	}

	public static long getTotalLix() {
		return lTotalLix.get();
	}

	public static void incrementTotalLix() {
		lTotalLix.incrementAndGet();
	}

	public static void resetNoOfCompiledJobs( int count )
	{
		//reset both mr/sp for multiple tests within one jvm
		
		if(OptimizerUtils.isSparkExecutionMode()) {
			setNoOfCompiledSPInst(count);
			setNoOfCompiledMRJobs(0);
		}
		else{
			setNoOfCompiledMRJobs(count);
			setNoOfCompiledSPInst(0);
		}
	}

	public static void resetNoOfExecutedJobs( int count )
	{
		//reset both mr/sp for multiple tests within one jvm
		
		if(OptimizerUtils.isSparkExecutionMode()) {
			setNoOfExecutedSPInst(count);
			setNoOfExecutedMRJobs(0);		
		}
		else {
			setNoOfExecutedMRJobs(count);
			setNoOfExecutedSPInst(0);
		}
		
		if( DMLScript.USE_ACCELERATOR )
			setNoOfExecutedGPUInst(0);
	}
	
	public static synchronized void incrementJITCompileTime( long time ) {
		jitCompileTime += time;
	}
	
	public static synchronized void incrementJVMgcTime( long time ) {
		jvmGCTime += time;
	}
	
	public static synchronized void incrementJVMgcCount( long delta ) {
		jvmGCCount += delta;
	}
	
	public static void incrementHOPRecompileTime( long delta ) {
		//note: not synchronized due to use of atomics
		hopRecompileTime.addAndGet(delta);
	}
	
	public static void incrementHOPRecompilePred() {
		//note: not synchronized due to use of atomics
		hopRecompilePred.incrementAndGet();
	}
	
	public static void incrementHOPRecompilePred(long delta) {
		//note: not synchronized due to use of atomics
		hopRecompilePred.addAndGet(delta);
	}
	
	public static void incrementHOPRecompileSB() {
		//note: not synchronized due to use of atomics
		hopRecompileSB.incrementAndGet();
	}
	
	public static void incrementHOPRecompileSB(long delta) {
		//note: not synchronized due to use of atomics
		hopRecompileSB.addAndGet(delta);
	}
	
	public static void incrementCodegenDAGCompile() {
		codegenHopCompile.incrementAndGet();
	}
	
	public static void incrementCodegenCPlanCompile(long delta) {
		codegenCPlanCompile.addAndGet(delta);
	}
	
	public static void incrementCodegenClassCompile() {
		codegenClassCompile.incrementAndGet();
	}
	
	public static void incrementCodegenCompileTime(long delta) {
		codegenCompileTime.addAndGet(delta);
	}
	
	public static void incrementCodegenClassCompileTime(long delta) {
		codegenClassCompileTime.addAndGet(delta);
	}
	
	public static void incrementCodegenPlanCacheHits() {
		codegenPlanCacheHits.incrementAndGet();
	}
	
	public static void incrementCodegenPlanCacheTotal() {
		codegenPlanCacheTotal.incrementAndGet();
	}
	
	public static long getCodegenDAGCompile() {
		return codegenHopCompile.get();
	}
	
	public static long getCodegenCPlanCompile() {
		return codegenCPlanCompile.get();
	}
	
	public static long getCodegenClassCompile() {
		return codegenClassCompile.get();
	}
	
	public static long getCodegenCompileTime() {
		return codegenCompileTime.get();
	}
	
	public static long getCodegenClassCompileTime() {
		return codegenClassCompileTime.get();
	}
	
	public static long getCodegenPlanCacheHits() {
		return codegenPlanCacheHits.get();
	}
	
	public static long getCodegenPlanCacheTotal() {
		return codegenPlanCacheTotal.get();
	}

	public static void incrementFunRecompileTime( long delta ) {
		//note: not synchronized due to use of atomics
		funRecompileTime.addAndGet(delta);
	}
	
	public static void incrementFunRecompiles() {
		//note: not synchronized due to use of atomics
		funRecompiles.incrementAndGet();
	}
	
	public static synchronized void incrementParForOptimCount(){
		parforOptCount ++;
	}
	
	public static synchronized void incrementParForOptimTime( long time ) {
		parforOptTime += time;
	}
	
	public static synchronized void incrementParForInitTime( long time ) {
		parforInitTime += time;
	}
	
	public static synchronized void incrementParForMergeTime( long time ) {
		parforMergeTime += time;
	}

	public static void startCompileTimer() {
		if( DMLScript.STATISTICS )
			compileStartTime = System.nanoTime();
	}

	public static void stopCompileTimer() {
		if( DMLScript.STATISTICS )
			compileEndTime = System.nanoTime();
	}

	public static long getCompileTime() {
		return compileEndTime - compileStartTime;
	}
	
	/**
	 * Starts the timer, should be invoked immediately before invoking
	 * Program.execute()
	 */
	public static void startRunTimer() {
		execStartTime = System.nanoTime();
	}

	/**
	 * Stops the timer, should be invoked immediately after invoking
	 * Program.execute()
	 */
	public static void stopRunTimer() {
		execEndTime = System.nanoTime();
	}

	/**
	 * Returns the total time of run in nanoseconds.
	 * 
	 * @return run time in nanoseconds
	 */
	public static long getRunTime() {
		return execEndTime - execStartTime;
	}
	
	public static void reset()
	{
		hopRecompileTime.set(0);
		hopRecompilePred.set(0);
		hopRecompileSB.set(0);
		
		funRecompiles.set(0);
		funRecompileTime.set(0);
		
		parforOptCount = 0;
		parforOptTime = 0;
		parforInitTime = 0;
		parforMergeTime = 0;
		
		lTotalLix.set(0);
		lTotalLixUIP.set(0);
		lTotalUIPVar.set(0);
		
		resetJITCompileTime();
		resetJVMgcTime();
		resetJVMgcCount();
		resetCPHeavyHitters();
		
		cudaInitTime = 0;
		cudaLibrariesInitTime = 0;
		cudaAllocTime.set(0);
		cudaDeAllocTime.set(0);
		cudaToDevTime.set(0);
		cudaFromDevTime.set(0);
		cudaAllocCount.set(0);
		cudaDeAllocCount.set(0);
		cudaToDevCount.set(0);
		cudaFromDevCount.set(0);
		cudaEvictionCount.set(0);
		LibMatrixDNN.resetStatistics();
	}

	public static void resetJITCompileTime(){
		jitCompileTime = -1 * getJITCompileTime();
	}
	
	public static void resetJVMgcTime(){
		jvmGCTime = -1 * getJVMgcTime();
	}
	
	public static void resetJVMgcCount(){
		jvmGCTime = -1 * getJVMgcCount();
	}

	public static void resetCPHeavyHitters(){
		_cpInstTime.clear();
		_cpInstCounts.clear();
	}

	public static void setSparkCtxCreateTime(long ns) {
		sparkCtxCreateTime = ns;
	}
	
	public static void accSparkParallelizeTime(long t) {
		sparkParallelize.addAndGet(t);
	}

	public static void incSparkParallelizeCount(long c) {
		sparkParallelizeCount.addAndGet(c);
	}

	public static void accSparkCollectTime(long t) {
		sparkCollect.addAndGet(t);
	}

	public static void incSparkCollectCount(long c) {
		sparkCollectCount.addAndGet(c);
	}

	public static void accSparkBroadCastTime(long t) {
		sparkBroadcast.addAndGet(t);
	}

	public static void incSparkBroadcastCount(long c) {
		sparkBroadcastCount.addAndGet(c);
	}
	
	
	public static String getCPHeavyHitterCode( Instruction inst )
	{
		String opcode = null;
		
		if( inst instanceof MRJobInstruction )
		{
			MRJobInstruction mrinst = (MRJobInstruction) inst;
			opcode = "MR-Job_"+mrinst.getJobType();
		}
		else if( inst instanceof SPInstruction )
		{
			opcode = "SP_"+InstructionUtils.getOpCode(inst.toString());
			if( inst instanceof FunctionCallCPInstruction ) {
				FunctionCallCPInstruction extfunct = (FunctionCallCPInstruction)inst;
				opcode = extfunct.getFunctionName();
				//opcode = extfunct.getNamespace()+Program.KEY_DELIM+extfunct.getFunctionName();
			}	
		}
		else //CPInstructions
		{
			opcode = InstructionUtils.getOpCode(inst.toString());
			if( inst instanceof FunctionCallCPInstruction ) {
				FunctionCallCPInstruction extfunct = (FunctionCallCPInstruction)inst;
				opcode = extfunct.getFunctionName();
				//opcode = extfunct.getNamespace()+Program.KEY_DELIM+extfunct.getFunctionName();
			}		
		}
		
		return opcode;
	}

	/**
	 * "Maintains" or adds time to per instruction/op timers, also increments associated count
	 * @param instructionName	name of the instruction/op
	 * @param timeNanos				time in nano seconds
	 */
	public synchronized static void maintainCPHeavyHitters( String instructionName, long timeNanos )
	{
		long t0 = System.nanoTime();

		Long oldVal = _cpInstTime.get(instructionName);
		Long newVal = timeNanos + ((oldVal!=null) ? oldVal : 0);
		_cpInstTime.put(instructionName, newVal);

		Long oldCnt = _cpInstCounts.get(instructionName);
		Long newCnt = 1 + ((oldCnt!=null) ? oldCnt : 0);
		_cpInstCounts.put(instructionName, newCnt);

		statisticsOverhead.addAndGet(System.nanoTime() - t0);
	}

	/**
	 * "Maintains" or adds time to miscellaneous timers per instruction/op, also increments associated count
	 * @param instructionName	name of the instruction/op
	 * @param miscTimer				name of the miscellaneous timer
	 * @param timeNanos				time in nano seconds
	 * @param incrementCount	how much to increment the count of the miscTimer by
	 */
	public synchronized static void maintainCPMiscTimes( String instructionName, String miscTimer, long timeNanos, long incrementCount)
	{
		if (!DMLScript.STATISTICS)
			return;


		long t0 = System.nanoTime();

		HashMap<String, Long> miscTimesMap = _cpInstMiscTime.get(instructionName);
		if (miscTimesMap == null) {
			miscTimesMap = new HashMap<String, Long>();
			_cpInstMiscTime.put(instructionName, miscTimesMap);
		}
		Long oldVal = miscTimesMap.get(miscTimer);
		Long newVal = timeNanos + ((oldVal!=null) ? oldVal : 0);
		miscTimesMap.put(miscTimer, newVal);

		HashMap<String, Long> miscCountMap = _cpInstMiscCount.get(instructionName);
		if (miscCountMap == null){
			miscCountMap = new HashMap<String, Long>();
			_cpInstMiscCount.put(instructionName, miscCountMap);
		}
		Long oldCnt = miscCountMap.get(miscTimer);
		Long newCnt = incrementCount + ((oldCnt!=null) ? oldCnt : 0);
		miscCountMap.put(miscTimer, newCnt);

		statisticsOverhead.addAndGet(System.nanoTime() - t0);
	}

	/**
	 * "Maintains" or adds time to miscellaneous timers per instruction/op, also increments associated count by 1
	 * @param instructionName	name of the instruction/op
	 * @param miscTimer				name of the miscellaneous timer
	 * @param timeNanos				time in nano seconds
	 */
	public synchronized static void maintainCPMiscTimes( String instructionName, String miscTimer, long timeNanos){
		maintainCPMiscTimes(instructionName, miscTimer, timeNanos, 1);
	}


	public static Set<String> getCPHeavyHitterOpCodes() {
		return _cpInstTime.keySet();
	}
	
	public static long getCPHeavyHitterCount(String opcode) {
		return _cpInstCounts.get(opcode);
	}

	@SuppressWarnings("unchecked")
	public static String getHeavyHitters( int num )
	{
		int len = _cpInstTime.size();
		if( num <= 0 || len <= 0 )
			return "-";
		
		//get top k via sort
		Entry<String,Long>[] tmp = _cpInstTime.entrySet().toArray(new Entry[len]);
		Arrays.sort(tmp, new Comparator<Entry<String, Long>>() {
		    public int compare(Entry<String, Long> e1, Entry<String, Long> e2) {
		        return e1.getValue().compareTo(e2.getValue());
		    }
		});
		
		//prepare output string
		StringBuilder sb = new StringBuilder();
		for( int i=0; i<Math.min(num, len); i++ ){
			String key = tmp[len-1-i].getKey();
			sb.append("-- "+(i+1)+") \t");
			sb.append(key);
			sb.append(" \t");
			sb.append(String.format("%.3f", ((double)tmp[len-1-i].getValue())/1000000000));
			sb.append(" sec \t");
			sb.append(_cpInstCounts.get(key));
			sb.append("\t");
			// Add the miscellaneous timer info
			HashMap<String, Long> miscTimerMap =_cpInstMiscTime.get(key);
			if (miscTimerMap != null) {
				List<Entry<String, Long>> sortedList = new ArrayList<Entry<String, Long>>(miscTimerMap.entrySet());
				// Sort the times to display by the most expensive first
				Collections.sort(sortedList, new Comparator<Entry<String, Long>>() {
					@Override
					public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
						return (int)(o1.getValue() - o2.getValue());
					}
				});
				Iterator<Entry<String, Long>> miscTimeIter = sortedList.iterator();
				HashMap<String, Long> miscCountMap = _cpInstMiscCount.get(key);
				while (miscTimeIter.hasNext()) {
					Map.Entry<String, Long> e = miscTimeIter.next();
					String miscTimerName = e.getKey();
					Long miscTimerTime = e.getValue();
					Long miscCount = miscCountMap.get(miscTimerName);
					sb.append(miscTimerName + "[" + String.format("%.3f", (double)miscTimerTime/1000000000.0) + "s," + miscCount + "]");
					if (miscTimeIter.hasNext())
						sb.append(", ");
				}
			}

			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	/**
	 * Returns the total time of asynchronous JIT compilation in milliseconds.
	 * 
	 * @return JIT compile time
	 */
	public static long getJITCompileTime(){
		long ret = -1; //unsupported
		CompilationMXBean cmx = ManagementFactory.getCompilationMXBean();
		if( cmx.isCompilationTimeMonitoringSupported() )
		{
			ret = cmx.getTotalCompilationTime();
			ret += jitCompileTime; //add from remote processes
		}
		return ret;
	}
	
	public static long getJVMgcTime(){
		long ret = 0; 
		
		List<GarbageCollectorMXBean> gcxs = ManagementFactory.getGarbageCollectorMXBeans();
		
		for( GarbageCollectorMXBean gcx : gcxs )
			ret += gcx.getCollectionTime();
		if( ret>0 )
			ret += jvmGCTime;
		
		return ret;
	}
	
	public static long getJVMgcCount(){
		long ret = 0; 
		
		List<GarbageCollectorMXBean> gcxs = ManagementFactory.getGarbageCollectorMXBeans();
		
		for( GarbageCollectorMXBean gcx : gcxs )
			ret += gcx.getCollectionCount();
		if( ret>0 )
			ret += jvmGCCount;
		
		return ret;
	}
	
	public static long getHopRecompileTime(){
		return hopRecompileTime.get();
	}
	
	public static long getHopRecompiledPredDAGs(){
		return hopRecompilePred.get();
	}
	
	public static long getHopRecompiledSBDAGs(){
		return hopRecompileSB.get();
	}
	
	public static long getFunRecompileTime(){
		return funRecompileTime.get();
	}
	
	public static long getFunRecompiles(){
		return funRecompiles.get();
	}
		
	public static long getParforOptCount(){
		return parforOptCount;
	}
	
	public static long getParforOptTime(){
		return parforOptTime;
	}
	
	public static long getParforInitTime(){
		return parforInitTime;
	}
	
	public static long getParforMergeTime(){
		return parforMergeTime;
	}

	/**
	 * Returns statistics of the DML program that was recently completed as a string
	 * @return statistics as a string
	 */
	public static String display() {
		return display(DMLScript.STATISTICS_COUNT);
	}

	/**
	 * Returns statistics as a string
	 * @param maxHeavyHitters The maximum number of heavy hitters that are printed
	 * @return statistics as string
	 */
	public static String display(int maxHeavyHitters)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append("SystemML Statistics:\n");
		if( DMLScript.STATISTICS ) {
			sb.append("Total elapsed time:\t\t" + String.format("%.3f", (getCompileTime()+getRunTime())*1e-9) + " sec.\n"); // nanoSec --> sec
			sb.append("Total compilation time:\t\t" + String.format("%.3f", getCompileTime()*1e-9) + " sec.\n"); // nanoSec --> sec
		}
		sb.append("Total execution time:\t\t" + String.format("%.3f", getRunTime()*1e-9) + " sec.\n"); // nanoSec --> sec
		if( OptimizerUtils.isSparkExecutionMode() ) {
			if( DMLScript.STATISTICS ) //moved into stats on Shiv's request
				sb.append("Number of compiled Spark inst:\t" + getNoOfCompiledSPInst() + ".\n");
			sb.append("Number of executed Spark inst:\t" + getNoOfExecutedSPInst() + ".\n");
		}
		else {
			if( DMLScript.STATISTICS ) //moved into stats on Shiv's request
				sb.append("Number of compiled MR Jobs:\t" + getNoOfCompiledMRJobs() + ".\n");
			sb.append("Number of executed MR Jobs:\t" + getNoOfExecutedMRJobs() + ".\n");	
		}
		
		if( DMLScript.USE_ACCELERATOR && DMLScript.STATISTICS ) {
			sb.append("CUDA/CuLibraries init time:\t" + String.format("%.3f", cudaInitTime*1e-9) + "/"
					+ String.format("%.3f", cudaLibrariesInitTime*1e-9) + " sec.\n");
			sb.append("Number of executed GPU inst:\t" + getNoOfExecutedGPUInst() + ".\n");
			sb.append("GPU mem tx time  (alloc/dealloc/toDev/fromDev):\t"
					+ String.format("%.3f", cudaAllocTime.get()*1e-9) + "/"
					+ String.format("%.3f", cudaDeAllocTime.get()*1e-9) + "/"
					+ String.format("%.3f", cudaToDevTime.get()*1e-9) + "/"
					+ String.format("%.3f", cudaFromDevTime.get()*1e-9)  + " sec.\n");
			sb.append("GPU mem tx count (alloc/dealloc/toDev/fromDev/evict):\t"
					+ cudaAllocCount.get() + "/"
					+ cudaDeAllocCount.get() + "/"
					+ cudaSparseConversionCount.get() + "/"
					+ cudaToDevCount.get() + "/"
					+ cudaFromDevCount.get() + "/"
					+ cudaEvictionCount.get() + ".\n");
			sb.append("GPU conversion time  (sparseConv/sp2dense/dense2sp):\t"
					+ String.format("%.3f", cudaSparseConversionTime.get()*1e-9) + "/"
					+ String.format("%.3f", cudaSparseToDenseTime.get()*1e-9) + "/"
					+ String.format("%.3f", cudaDenseToSparseTime.get()*1e-9) + " sec.\n");
			sb.append("GPU conversion count (sparseConv/sp2dense/dense2sp):\t"
					+ cudaSparseConversionCount.get() + "/"
					+ cudaSparseToDenseCount.get() + "/"
					+ cudaDenseToSparseCount.get() + ".\n");
		}
		
		//show extended caching/compilation statistics
		if( DMLScript.STATISTICS ) 
		{
			sb.append("Cache hits (Mem, WB, FS, HDFS):\t" + CacheStatistics.displayHits() + ".\n");
			sb.append("Cache writes (WB, FS, HDFS):\t" + CacheStatistics.displayWrites() + ".\n");
			sb.append("Cache times (ACQr/m, RLS, EXP):\t" + CacheStatistics.displayTime() + " sec.\n");
			sb.append("HOP DAGs recompiled (PRED, SB):\t" + getHopRecompiledPredDAGs() + "/" + getHopRecompiledSBDAGs() + ".\n");
			sb.append("HOP DAGs recompile time:\t" + String.format("%.3f", ((double)getHopRecompileTime())/1000000000) + " sec.\n");
			if( getFunRecompiles()>0 ) {
				sb.append("Functions recompiled:\t\t" + getFunRecompiles() + ".\n");
				sb.append("Functions recompile time:\t" + String.format("%.3f", ((double)getFunRecompileTime())/1000000000) + " sec.\n");	
			}
			if( ConfigurationManager.getDMLConfig().getBooleanValue(DMLConfig.CODEGEN) ) {
				sb.append("Codegen compile (DAG, CP, JC):\t" + getCodegenDAGCompile() + "/" + getCodegenCPlanCompile() + "/" + getCodegenClassCompile() + ".\n");
				sb.append("Codegen compile times (DAG,JC):\t" + String.format("%.3f", (double)getCodegenCompileTime()/1000000000) + "/" + 
						String.format("%.3f", (double)getCodegenClassCompileTime()/1000000000)  + " sec.\n");
				sb.append("Codegen plan cache hits:\t" + getCodegenPlanCacheHits() + "/" + getCodegenPlanCacheTotal() + ".\n");
			}
			if( OptimizerUtils.isSparkExecutionMode() ){
				String lazy = SparkExecutionContext.isLazySparkContextCreation() ? "(lazy)" : "(eager)";
				sb.append("Spark ctx create time "+lazy+":\t"+
						String.format("%.3f", ((double)sparkCtxCreateTime)*1e-9)  + " sec.\n" ); // nanoSec --> sec
				
				sb.append("Spark trans counts (par,bc,col):" +
						String.format("%d/%d/%d.\n", sparkParallelizeCount.get(), sparkBroadcastCount.get(), sparkCollectCount.get()));
				sb.append("Spark trans times (par,bc,col):\t" +
						String.format("%.3f/%.3f/%.3f secs.\n", 
								 ((double)sparkParallelize.get())*1e-9,
								 ((double)sparkBroadcast.get())*1e-9,
								 ((double)sparkCollect.get())*1e-9));
			}
			if( parforOptCount>0 ){
				sb.append("ParFor loops optimized:\t\t" + getParforOptCount() + ".\n");
				sb.append("ParFor optimize time:\t\t" + String.format("%.3f", ((double)getParforOptTime())/1000) + " sec.\n");	
				sb.append("ParFor initialize time:\t\t" + String.format("%.3f", ((double)getParforInitTime())/1000) + " sec.\n");	
				sb.append("ParFor result merge time:\t" + String.format("%.3f", ((double)getParforMergeTime())/1000) + " sec.\n");	
				sb.append("ParFor total update in-place:\t" + lTotalUIPVar + "/" + lTotalLixUIP + "/" + lTotalLix + "\n");
			}
			sb.append("Total JIT compile time:\t\t" + ((double)getJITCompileTime())/1000 + " sec.\n");
			sb.append("Total JVM GC count:\t\t" + getJVMgcCount() + ".\n");
			sb.append("Total JVM GC time:\t\t" + ((double)getJVMgcTime())/1000 + " sec.\n");
			LibMatrixDNN.appendStatistics(sb);
			sb.append("Heavy hitter instructions (name, time, count):\n" + getHeavyHitters(maxHeavyHitters));
			sb.append("Statistics collection overhead: " + ((double)statisticsOverhead.get()*1e-9) + " sec\n");
		}
		
		return sb.toString();
	}
}
