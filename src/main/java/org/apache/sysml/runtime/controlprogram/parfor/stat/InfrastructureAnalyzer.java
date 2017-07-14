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

package org.apache.sysml.runtime.controlprogram.parfor.stat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.runtime.matrix.mapred.MRConfigurationNames;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * Central place for analyzing and obtaining static infrastructure properties
 * such as memory and number of logical processors.
 * 
 * 
 */
public class InfrastructureAnalyzer 
{
	
	public static final long DEFAULT_JVM_SIZE = 512 * 1024 * 1024;
	
	//static local master node properties
	private static int  _localPar        = -1;
	private static long _localJVMMaxMem  = -1;
	private static boolean _isLtJDK8 = false;
	
	//static hadoop cluster properties
	private static int  _remotePar       = -1;
	private static int  _remoteParMap    = -1;
	private static int  _remoteParReduce = -1;
	private static long _remoteJVMMaxMemMap    = -1;
	private static long _remoteJVMMaxMemReduce = -1;
	private static long _remoteMRSortMem = -1;
	private static boolean _localJT      = false;
	private static long _blocksize       = -1;
	private static boolean _yarnEnabled  = false;
	
	//static initialization, called for each JVM (on each node)
	static 
	{
		//analyze local node properties
		analyzeLocalMachine();
		
		//analyze remote Hadoop cluster properties
		//analyzeHadoopCluster(); //note: due to overhead - analyze on-demand
	}

	public static boolean isJavaVersionLessThanJDK8()
	{
		return _isLtJDK8;
	}
	
	///////
	//methods for obtaining parallelism properties
	
	/**
	 * Gets the number of logical processors of the current node,
	 * including hyper-threading if enabled.
	 * 
	 * @return number of local processors of the current node
	 */
	public static int getLocalParallelism()
	{
		return _localPar;
	}	
	
	/**
	 * Gets the number of cluster nodes (number of tasktrackers). If multiple tasktracker
	 * are started per node, each tasktracker is viewed as individual node.
	 * 
	 * @return number of cluster nodes
	 */
	public static int getRemoteParallelNodes() 
	{
		if( _remotePar == -1 )
			analyzeHadoopCluster();
		
		return _remotePar;
	}	
	
	/**
	 * Gets the number of remote parallel map slots.
	 * 
	 * @return number of remote parallel map tasks
	 */
	public static int getRemoteParallelMapTasks()
	{
		if( _remoteParMap == -1 )
			analyzeHadoopCluster();
		
		return _remoteParMap;
	}

	public static void setRemoteParallelMapTasks(int pmap)
	{
		_remoteParMap = pmap;
	}
	
	/**
	 * Gets the total number of remote parallel reduce slots.
	 * 
	 * @return number of remote parallel reduce tasks
	 */
	public static int getRemoteParallelReduceTasks()
	{
		if( _remoteParReduce == -1 )
			analyzeHadoopCluster();
		
		return _remoteParReduce;
	}

	public static void setRemoteParallelReduceTasks(int preduce)
	{
		_remoteParReduce = preduce;
	}
	
	///////
	//methods for obtaining memory properties
	
	/**
	 * Gets the maximum memory [in bytes] of the current JVM.
	 * 
	 * @return maximum memory of the current JVM
	 */
	public static long getLocalMaxMemory()
	{
		double prefetchMemoryBudgetInMB = ConfigurationManager.getDMLConfig().getDoubleValue(DMLConfig.PREFETCH_MEM_BUDGET);
		if(prefetchMemoryBudgetInMB <= 0) {
			return _localJVMMaxMem;
		}
		else {
			long MBInBytes = 1000000;
			// Ensure that prefetch memory budget is no larger than 20% of available memory
			if(_localJVMMaxMem*0.2 <= prefetchMemoryBudgetInMB*MBInBytes)
				throw new RuntimeException("ERROR: Prefetch memory budget (" + prefetchMemoryBudgetInMB + " mb) cannot be greater than 20% of available memory (" + (((double)_localJVMMaxMem)/MBInBytes) + " mb)");
			return _localJVMMaxMem - ((long)(prefetchMemoryBudgetInMB*MBInBytes));
		}
	}

	public static void setLocalMaxMemory( long localMem )
	{
		_localJVMMaxMem = localMem;
	}
	
	/**
	 * Gets the maximum memory [in bytes] of a hadoop map task JVM.
	 * 
	 * @return maximum memory of remote hadoop map task jvm
	 */
	public static long getRemoteMaxMemoryMap()
	{
		if( _remoteJVMMaxMemMap == -1 )
			analyzeHadoopConfiguration();
		
		return _remoteJVMMaxMemMap;
	}

	public static void setRemoteMaxMemoryMap( long remoteMem )
	{
		_remoteJVMMaxMemMap = remoteMem;
	}
	
	/**
	 * Gets the maximum memory [in bytes] of a hadoop reduce task JVM.
	 * 
	 * @return maximum memory of remote hadoop reduce task jvm
	 */
	public static long getRemoteMaxMemoryReduce()
	{
		if( _remoteJVMMaxMemReduce == -1 )
			analyzeHadoopConfiguration();
		
		return _remoteJVMMaxMemReduce;
	}

	public static void setRemoteMaxMemoryReduce( long remoteMem )
	{
		_remoteJVMMaxMemReduce = remoteMem;
	}

	/**
	 * Gets the maximum sort buffer memory requirement [in bytes] of a hadoop task.
	 * 
	 * @return maximum sort buffer memory of hadoop task
	 */
	public static long getRemoteMaxMemorySortBuffer( )
	{
		if( _remoteMRSortMem == -1 )
			analyzeHadoopConfiguration();
		
		return _remoteMRSortMem;		
	}
	
	public static boolean isLocalMode()
	{
		if( _remoteJVMMaxMemMap == -1 )
			analyzeHadoopConfiguration();
		
		return _localJT;		
	}
	
	public static boolean isLocalMode(JobConf job)
	{
		// Due to a bug in HDP related to fetching the "mode" of execution within mappers,
		// we explicitly probe the relevant properties instead of relying on results from 
		// analyzeHadoopCluster().
		String jobTracker = job.get(MRConfigurationNames.MR_JOBTRACKER_ADDRESS, "local");
		String framework = job.get(MRConfigurationNames.MR_FRAMEWORK_NAME, "local");
		boolean isYarnEnabled = (framework!=null && framework.equals("yarn"));
		
		return ("local".equals(jobTracker) & !isYarnEnabled);
	}
	
	///////
	//methods for obtaining constraints or respective defaults
	
	/**
	 * Gets the maximum local parallelism constraint.
	 * 
	 * @return maximum local parallelism constraint
	 */
	public static int getCkMaxCP() 
	{
		//default value (if not specified)
		return getLocalParallelism();
	}

	/**
	 * Gets the maximum remote parallelism constraint
	 * 
	 * @return maximum remote parallelism constraint
	 */
	public static int getCkMaxMR() 
	{
		if( OptimizerUtils.isSparkExecutionMode() )
			return SparkExecutionContext.getDefaultParallelism(true);
		else
			return getRemoteParallelMapTasks();
	}

	/**
	 * Gets the maximum memory constraint [in bytes].
	 * 
	 * @return maximum memory constraint
	 */
	public static long getCmMax() 
	{
		//default value (if not specified)
		return Math.min( getLocalMaxMemory(), getRemoteMaxMemoryMap() );
	}

	/**
	 * Gets the HDFS blocksize of the used cluster in bytes.
	 * 
	 * @return HDFS block size
	 */
	public static long getHDFSBlockSize()
	{
		if( _blocksize == -1 )
			analyzeHadoopConfiguration();
		
		return _blocksize;		
	}

	public static boolean isYarnEnabled()
	{
		if( _remoteJVMMaxMemMap == -1 )
			analyzeHadoopConfiguration();
		
		return _yarnEnabled;
	}

	public static long extractMaxMemoryOpt(String javaOpts)
	{
		long ret = -1; //mem in bytes
		
		try
		{
			StringTokenizer st = new StringTokenizer( javaOpts, " " );
			while( st.hasMoreTokens() ) {
				String arg = st.nextToken();
				if( !arg.startsWith("-Xmx") ) //search for max mem
					continue;
				
				//cut off "-Xmx" parameter
				arg = arg.substring(4);
				
				//parse number and unit
				ret = UtilFunctions.parseMemorySize(arg); 
			}
			
			if( ret < 0 ) { // no argument found
				ret = DEFAULT_JVM_SIZE;
			}
		}
		catch(Exception ex) {
			//if anything breaks during parsing (e.g., because args not specified correctly)
			ret = DEFAULT_JVM_SIZE;
		}
		
		return ret;
	}

	public static void setMaxMemoryOpt(JobConf job, String key, long bytes)
	{
		String javaOptsOld = job.get( key );
		String javaOptsNew = null;

		//StringTokenizer st = new StringTokenizer( javaOptsOld, " " );
		String[] tokens = javaOptsOld.split(" "); //account also for no ' '
		StringBuilder sb = new StringBuilder();
		for( String arg : tokens )
		{
			if( arg.startsWith("-Xmx") ) //search for max mem
			{
				sb.append("-Xmx");
				sb.append( (bytes/(1024*1024)) );
				sb.append("M");
			}
			else
				sb.append(arg);
			
			sb.append(" ");
		}
		javaOptsNew = sb.toString().trim();		
		job.set(key, javaOptsNew);
	}
	
	/**
	 * Gets the fraction of running map/reduce tasks to existing
	 * map/reduce task slots. 
	 * 
	 * NOTE: on YARN the number of slots is a spurious indicator 
	 * because containers are purely scheduled based on memory. 
	 * 
	 * @param mapOnly if true, only look at map tasks
	 * @return cluster utilization (current / capacity)
	 * @throws IOException if IOException occurs
	 */
	public static double getClusterUtilization(boolean mapOnly) 
		throws IOException
	{
		//in local mode, the cluster utilization is always 0.0 
		
		JobConf job = ConfigurationManager.getCachedJobConf();
		JobClient client = new JobClient(job);
		ClusterStatus stat = client.getClusterStatus();
		
		double ret = 0.0;
		if( stat != null ) //if in cluster mode
		{	
			if( mapOnly )
			{
				int capacity = stat.getMaxMapTasks();
				int current = stat.getMapTasks();
				ret = ((double)current) / capacity;
			}
			else
			{
				int capacity = stat.getMaxMapTasks() + stat.getMaxReduceTasks();
				int current = stat.getMapTasks() + stat.getReduceTasks();
				ret = ((double)current) / capacity;
			}
		}
		
		return ret;
	}
	
	///////
	//internal methods for analysis
		
	/**
	 * Analyzes properties of local machine and JVM.
	 */
	private static void analyzeLocalMachine()
	{
		//step 1: basic parallelism and memory
		_localPar       = Runtime.getRuntime().availableProcessors();
		_localJVMMaxMem = Runtime.getRuntime().maxMemory();
		
		//step 2: analyze if used jdk older than jdk8
		String version = System.getProperty("java.version");
		
		//check for jdk version less than 8 (and raise warning if multi-threaded)
		_isLtJDK8 = (UtilFunctions.compareVersion(version, "1.8") < 0); 
	}
	
	/**
	 * Analyzes properties of hadoop cluster and configuration.
	 */
	private static void analyzeHadoopCluster()
	{
		try 
		{
			JobConf job = ConfigurationManager.getCachedJobConf();
			JobClient client = new JobClient(job);
			ClusterStatus stat = client.getClusterStatus();
			if( stat != null ) //if in cluster mode
			{
				//analyze cluster status
				_remotePar = stat.getTaskTrackers();
				_remoteParMap = stat.getMaxMapTasks(); 
				_remoteParReduce = stat.getMaxReduceTasks(); 
				
				//analyze pure configuration properties
				analyzeHadoopConfiguration();
			}		
		} 
		catch (IOException e) 
		{
			throw new RuntimeException("Unable to analyze infrastructure.",e);
		}
	}
	
	/**
	 * Analyzes only properties of hadoop configuration in order to prevent 
	 * expensive call to cluster status .
	 */
	private static void analyzeHadoopConfiguration()
	{
		JobConf job = ConfigurationManager.getCachedJobConf();
		
		_remoteMRSortMem = (1024*1024) * job.getLong(MRConfigurationNames.MR_TASK_IO_SORT_MB,100); //1MB
			
		//handle jvm max mem (map mem budget is relevant for map-side distcache and parfor)
		//(for robustness we probe both: child and map configuration parameters)
		String javaOpts1 = job.get(MRConfigurationNames.MR_CHILD_JAVA_OPTS); //internally mapred/mapreduce synonym
		String javaOpts2 = job.get(MRConfigurationNames.MR_MAP_JAVA_OPTS, null); //internally mapred/mapreduce synonym
		String javaOpts3 = job.get(MRConfigurationNames.MR_REDUCE_JAVA_OPTS, null); //internally mapred/mapreduce synonym
		if( javaOpts2 != null ) //specific value overrides generic
			_remoteJVMMaxMemMap = extractMaxMemoryOpt(javaOpts2); 
		else
			_remoteJVMMaxMemMap = extractMaxMemoryOpt(javaOpts1);
		if( javaOpts3 != null ) //specific value overrides generic
			_remoteJVMMaxMemReduce = extractMaxMemoryOpt(javaOpts3); 
		else
			_remoteJVMMaxMemReduce = extractMaxMemoryOpt(javaOpts1);
		
		//HDFS blocksize
		String blocksize = job.get(MRConfigurationNames.DFS_BLOCKSIZE, "134217728");
		_blocksize = Long.parseLong(blocksize);
		
		//is yarn enabled
		String framework = job.get(MRConfigurationNames.MR_FRAMEWORK_NAME);
		_yarnEnabled = (framework!=null && framework.equals("yarn"));
		
		//analyze if local mode (internally requires yarn_enabled)
		_localJT = analyzeLocalMode(job);		
	}

	private static boolean analyzeLocalMode(JobConf job)
	{
		//analyze if local mode (if yarn enabled, we always assume cluster mode
		//in order to workaround configuration issues on >=Hadoop 2.6)
		String jobTracker = job.get(MRConfigurationNames.MR_JOBTRACKER_ADDRESS, "local");
		return "local".equals(jobTracker)
			   & !isYarnEnabled();
	}
}
