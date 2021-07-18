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

package org.apache.sysds.runtime.controlprogram.caching;

import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.LazyWriteBuffer.RPolicy;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysds.runtime.controlprogram.parfor.util.IDSequence;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.fed.InitFEDInstruction;
import org.apache.sysds.runtime.instructions.gpu.context.GPUContext;
import org.apache.sysds.runtime.instructions.gpu.context.GPUObject;
import org.apache.sysds.runtime.instructions.spark.data.BroadcastObject;
import org.apache.sysds.runtime.instructions.spark.data.RDDObject;
import org.apache.sysds.runtime.io.FileFormatProperties;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.io.ReaderWriterFederated;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.runtime.util.LocalFileUtils;
import org.apache.sysds.utils.Statistics;


/**
 * Each object of this class is a cache envelope for some large piece of data
 * called "cache block". For example, the body of a matrix can be the cache block.  
 * The term cache block refers strictly to the cacheable portion of the data object, 
 * often excluding metadata and auxiliary parameters, as defined in the subclasses.
 * Under the protection of the envelope, the data blob may be evicted to
 * the file system; then the subclass must set its reference to <code>null</code>
 * to allow Java garbage collection. If other parts of the system continue
 * keep references to the cache block, its eviction will not release any memory.
 */
public abstract class CacheableData<T extends CacheBlock> extends Data
{
	private static final long serialVersionUID = -413810592207212835L;

	/** Global logging instance for all subclasses of CacheableData */
	protected static final Log LOG = LogFactory.getLog(CacheableData.class.getName());
	
	// global constant configuration parameters
	public static final long    CACHING_THRESHOLD = (long)Math.max(4*1024, //obj not s.t. caching
		1e-5 * InfrastructureAnalyzer.getLocalMaxMemory());       //if below threshold [in bytes]
	public static final double CACHING_BUFFER_SIZE = 0.15;
	public static final RPolicy CACHING_BUFFER_POLICY = RPolicy.FIFO;
	public static final boolean CACHING_BUFFER_PAGECACHE = false;
	public static final boolean CACHING_WRITE_CACHE_ON_READ = false;
	public static final String  CACHING_COUNTER_GROUP_NAME = "SystemDS Caching Counters";
	public static final String  CACHING_EVICTION_FILEEXTENSION = ".dat";
	public static final boolean CACHING_ASYNC_FILECLEANUP = true;
	public static final boolean CACHING_ASYNC_SERIALIZE = false;
	
	//NOTE CACHING_ASYNC_SERIALIZE:
	// The serialization of matrices and frames (ultra-sparse matrices or 
	// frames with strings) into buffer pool byte arrays happens outside the 
	// critical region of the global lock in LazyWriteBuffer. However, it still
	// requires thread-local serialization (before returning from release) in 
	// order to guarantee that not too many objects are pinned at the same time 
	// which would violate the memory budget. Therefore, the new asynchronous 
	// serialization (see CACHING_ASYNC_SERIALIZE) should be understood as
	// optimistic with weaker guarantees.
	
	/**
	 * Defines all possible cache status types for a data blob.
	 * An object of class {@link CacheableData} can be in one of the following
	 * five status types:
	 *
	 * <code>EMPTY</code>: Either there is no data blob at all, or the data blob  
	 * resides in a specified import file and has never been downloaded yet.
	 * <code>READ</code>:   The data blob is in main memory; one or more threads are
	 * referencing and reading it (shared "read-only" lock).  This status uses a
	 * counter.  Eviction is NOT allowed.
	 * <code>MODIFY</code>:   The data blob is in main memory; exactly one thread is
	 * referencing and modifying it (exclusive "write" lock).  Eviction is NOT allowed.
	 * <code>CACHED</code>:   The data blob is in main memory, and nobody is using nor referencing it. 
	 * There is always an persistent recovery object for it
	 **/
	public enum CacheStatus {
		EMPTY, 
		READ, 
		MODIFY, 
		CACHED,
		CACHED_NOWRITE,
	}
	
	/** Global flag indicating if caching is enabled (controls eviction) */
	private static volatile boolean _activeFlag = false;
	
	/** Global sequence for generating unique ids. */
	private static IDSequence _seq = null;

	// Global eviction path and prefix (prefix used for isolation purposes)
	public static String cacheEvictionLocalFilePath = null; //set during init
	public static String cacheEvictionLocalFilePrefix = "cache";

	/**
	 * Current state of pinned variables, required for guarded collect.
	 */
	private static ThreadLocal<Long> sizePinned = new ThreadLocal<Long>() {
		@Override protected Long initialValue() { return 0L; }
	};

	//current size of live broadcast objects (because Spark's ContextCleaner maintains 
	//a buffer with references to prevent eager cleanup by GC); note that this is an 
	//overestimate, because we maintain partitioned broadcasts as soft references, which 
	//might be collected by the GC and subsequently cleaned up by Spark's ContextCleaner.
	private static final AtomicLong _refBCs = new AtomicLong(0);

	static {
		_seq = new IDSequence();
	}

	/**
	 * The unique (JVM-wide) ID of a cacheable data object; to ensure unique IDs across JVMs, we
	 * concatenate filenames with a unique prefix (map task ID). 
	 */
	private final long _uniqueID;
	
	/** The cache status of the data blob (whether it can be or is evicted, etc. */
	private CacheStatus _cacheStatus = null;
	
	/** Cache for actual data, evicted by garbage collector. */
	protected SoftReference<T> _cache = null;
	
	/** Container object that holds the actual data. */
	protected T _data = null;

	/**
	 * Object that holds the metadata associated with the matrix, which
	 * includes: 1) Matrix dimensions, if available 2) Number of non-zeros, if
	 * available 3) Block dimensions, if applicable 4) InputInfo -- subsequent
	 * operations that use this Matrix expect it to be in this format.
	 * 
	 * When the matrix is written to HDFS (local file system, as well?), one
	 * must get the OutputInfo that matches with InputInfo stored inside _mtd.
	 */
	protected MetaData _metaData = null;
	
	protected FederationMap _fedMapping = null;
	
	/** The name of HDFS file in which the data is backed up. */
	protected String _hdfsFileName = null; // file name and path
	
	/** 
	 * Flag that indicates whether or not hdfs file exists.It is used 
	 * for improving the performance of "rmvar" instruction. When it has 
	 * value <code>false</code>, one can skip file system existence 
	 * checks which can be expensive.
	 */
	private boolean _hdfsFileExists = false; 

	/** Information relevant to specific external file formats. */
	private FileFormatProperties _formatProps = null;
	
	/**
	 * <code>true</code> if the in-memory or evicted matrix may be different from
	 * the matrix located at {@link #_hdfsFileName}; <code>false</code> if the two
	 * matrices should be the same.
	 */
	private boolean _dirtyFlag = false;
	
	// additional private flags and meta data
	private int     _numReadThreads = 0;   //number of threads for read from HDFS
	private boolean _cleanupFlag = true;   //flag if obj unpinned (cleanup enabled)	
	private String  _cacheFileName = null; //local eviction file name
	private boolean _requiresLocalWrite = false; //flag if local write for read obj
	private boolean _isAcquireFromEmpty = false; //flag if read from status empty 
	
	//spark-specific handles
	//note: we use the abstraction of LineageObjects for two reasons: (1) to keep track of cleanup
	//for lazily evaluated RDDs, and (2) as abstraction for environments that do not necessarily have spark libraries available
	private RDDObject _rddHandle = null; //RDD handle
	private BroadcastObject<T> _bcHandle = null; //Broadcast handle
	protected HashMap<GPUContext, GPUObject> _gpuObjects = null; //Per GPUContext object allocated on GPU

	private LineageItem _lineage = null;
	
	/**
	 * Basic constructor for any cacheable data.
	 * 
	 * @param dt data type
	 * @param vt value type
	 */
	protected CacheableData(DataType dt, ValueType vt) {
		super (dt, vt);
		_uniqueID = _seq.getNextID();
		_cacheStatus = CacheStatus.EMPTY;
		_numReadThreads = 0;
		_gpuObjects = DMLScript.USE_ACCELERATOR ? new HashMap<>() : null;
	}
	
	/**
	 * Copy constructor for cacheable data (of same type).
	 * 
	 * @param that cacheable data object
	 */
	protected CacheableData(CacheableData<T> that) {
		this( that.getDataType(), that.getValueType() );
		_cleanupFlag = that._cleanupFlag;
		_hdfsFileName = that._hdfsFileName;
		_hdfsFileExists = that._hdfsFileExists; 
		_gpuObjects = that._gpuObjects;
		_privacyConstraint = that._privacyConstraint;
		_dirtyFlag = that._dirtyFlag;
	}

	
	/**
	 * Enables or disables the cleanup of the associated 
	 * data object on clearData().
	 * 
	 * @param flag true if cleanup
	 */
	public void enableCleanup(boolean flag) {
		_cleanupFlag = flag;
	}

	/**
	 * Indicates if cleanup of the associated data object 
	 * is enabled on clearData().
	 * 
	 * @return true if cleanup enabled
	 */
	public boolean isCleanupEnabled() {
		return _cleanupFlag;
	}
	
	public CacheStatus getStatus() {
		return _cacheStatus;
	}

	public boolean isHDFSFileExists() {
		return _hdfsFileExists;
	}

	public void setHDFSFileExists( boolean flag )  {
		_hdfsFileExists = flag;
	}

	public String getFileName() {
		return _hdfsFileName;
	}
	
	public long getUniqueID() {
		return _uniqueID;
	}

	public synchronized void setFileName( String file ) {
		if( _hdfsFileName!=null && !_hdfsFileName.equals(file) )
			if( !isEmpty(true) )
				_dirtyFlag = true;
		_hdfsFileName = file;
	}
	
	/**
	 * <code>true</code> if the in-memory or evicted matrix may be different from
	 * the matrix located at {@link #_hdfsFileName}; <code>false</code> if the two
	 * matrices are supposed to be the same.
	 * 
	 * @return true if dirty
	 */
	public boolean isDirty() {
		return _dirtyFlag;
	}

	public void setDirty(boolean flag) {
		_dirtyFlag = flag;
	}

	public FileFormatProperties getFileFormatProperties() {
		return _formatProps;
	}

	public void setFileFormatProperties(FileFormatProperties props) {
		_formatProps = props;
	}
	
	@Override
	public void setMetaData(MetaData md) {
		_metaData = md;
	}
	
	@Override
	public MetaData getMetaData() {
		return _metaData;
	}

	@Override
	public void removeMetaData() {
		_metaData = null;
	}
	
	public DataCharacteristics getDataCharacteristics() {
		return _metaData.getDataCharacteristics();
	}

	public long getNumRows() {
		return getDataCharacteristics().getRows();
	}

	public long getNumColumns() {
		return getDataCharacteristics().getCols();
	}
	
	public long getBlocksize() {
		return getDataCharacteristics().getBlocksize();
	}
	
	public abstract void refreshMetaData();

	public LineageItem getCacheLineage() {
		return _lineage;
	}

	public void setCacheLineage(LineageItem li) {
		_lineage = li;
	}

	public boolean hasValidLineage() {
		return (_lineage != null);
	}

	/**
	 * Check if object is federated.
	 * @return true if federated else false
	 */
	public boolean isFederated() {
		if(_fedMapping == null && _metaData instanceof MetaDataFormat){
			MetaDataFormat mdf = (MetaDataFormat) _metaData;
			if(mdf.getFileFormat() == FileFormat.FEDERATED){
				InitFEDInstruction.federateMatrix(
					this, ReaderWriterFederated.read(_hdfsFileName, mdf.getDataCharacteristics()));
				return true;
			}
		}
		return _fedMapping != null;
	}
	
	public boolean isFederated(FType type) {
		return isFederated() && (type == null || _fedMapping.getType().isType(type)
			|| (_fedMapping.getType() == FType.PART && _fedMapping.getType().getPartType() == type.getPartType()));
	}
	
	/**
	 * Gets the mapping of indices ranges to federated objects.
	 * @return fedMapping mapping
	 */
	public FederationMap getFedMapping() {
		return _fedMapping;
	}
	
	/**
	 * Sets the mapping of indices ranges to federated objects.
	 * @param fedMapping mapping
	 */
	public void setFedMapping(FederationMap fedMapping) {
		_fedMapping = fedMapping;
	}
	
	public RDDObject getRDDHandle() {
		return _rddHandle;
	}

	public void setRDDHandle( RDDObject rdd ) {
		//cleanup potential old back reference
		if( _rddHandle != null )
			_rddHandle.setBackReference(null);
		
		//add new rdd handle
		_rddHandle = rdd;
		if( _rddHandle != null )
			rdd.setBackReference(this);
	}
	
	public BroadcastObject<T> getBroadcastHandle() {
		return _bcHandle;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setBroadcastHandle( BroadcastObject bc ) {
		//cleanup potential old back reference
		if( _bcHandle != null )
			_bcHandle.setBackReference(null);
			
		//add new broadcast handle
		_bcHandle = bc;
		if( _bcHandle != null )
			bc.setBackReference(this);
	}

	public synchronized GPUObject getGPUObject(GPUContext gCtx) {
		if( _gpuObjects == null )
			return null;
		return _gpuObjects.get(gCtx);
	}

	public synchronized void setGPUObject(GPUContext gCtx, GPUObject gObj) {
		if( _gpuObjects == null )
			_gpuObjects = new HashMap<>();
		GPUObject old = _gpuObjects.put(gCtx, gObj);
		if (old != null)
				throw new DMLRuntimeException("GPU : Inconsistent internal state - this CacheableData already has a GPUObject assigned to the current GPUContext (" + gCtx + ")");
	}
	
	public synchronized void removeGPUObject(GPUContext gCtx) {
		_gpuObjects.remove(gCtx);
	}
	
	// *********************************************
	// ***                                       ***
	// ***    HIGH-LEVEL METHODS THAT SPECIFY    ***
	// ***   THE LOCKING AND CACHING INTERFACE   ***
	// ***                                       ***
	// *********************************************

	public T acquireReadAndRelease() {
		T tmp = acquireRead();
		release();
		return tmp;
	}
	
	/**
	 * Acquires a shared "read-only" lock, produces the reference to the cache block,
	 * restores the cache block to main memory, reads from HDFS if needed.
	 * 
	 * Synchronized because there might be parallel threads (parfor local) that
	 * access the same object (in case it was created before the loop).
	 * 
	 * In-Status:  EMPTY, EVICTABLE, EVICTED, READ;
	 * Out-Status: READ(+1).
	 * 
	 * @return cacheable data
	 */
	public T acquireRead() {
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		//core internal acquire (synchronized per object)
		T ret = acquireReadIntern();
		
		//update thread-local status (after pin but outside the
		//critical section of accessing a shared object)
		if( !isBelowCachingThreshold() )
			updateStatusPinned(true);
		
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementAcquireRTime(t1-t0);
		}
		
		return ret;
	}
	
	private synchronized T acquireReadIntern() {
		if ( !isAvailableToRead() )
			throw new DMLRuntimeException("MatrixObject not available to read.");

		//get object from cache
		if( _data == null )
			getCache();
		
		//call acquireHostRead if gpuHandle is set as well as is allocated
		if( DMLScript.USE_ACCELERATOR && _gpuObjects != null ) {
			boolean copiedFromGPU = false;
			for (Map.Entry<GPUContext, GPUObject> kv : _gpuObjects.entrySet()) {
				GPUObject gObj = kv.getValue();
				if (gObj != null && copiedFromGPU && gObj.isDirty())
					throw new DMLRuntimeException("Internal Error : Inconsistent internal state, A copy of this CacheableData was dirty on more than 1 GPU");
				else if (gObj != null) {
					copiedFromGPU = gObj.acquireHostRead(null);
					if( _data == null )
						getCache();
				}
			}
		}
		
		//read data from HDFS/RDD if required
		//(probe data for cache_nowrite / jvm_reuse)
		if( _data==null && ( isEmpty(true) || hasValidLineage() )) {
			try {
				if( hasValidLineage() ) {
					_data = reconstructByLineage(getCacheLineage());
					_requiresLocalWrite = false;
					if( DMLScript.STATISTICS )
						CacheStatistics.incrementLinHits();
				}
				else if( isFederated() ) {
					_data = readBlobFromFederated(_fedMapping);

					//mark for initial local write despite read operation
					_requiresLocalWrite = CACHING_WRITE_CACHE_ON_READ;
				}
				else if( getRDDHandle()==null || getRDDHandle().allowsShortCircuitRead() ) {
					if( DMLScript.STATISTICS )
						CacheStatistics.incrementHDFSHits();
					
					//check filename
					if( _hdfsFileName == null )
						throw new DMLRuntimeException("Cannot read matrix for empty filename.");
					
					//read cacheable data from hdfs
					_data = readBlobFromHDFS( _hdfsFileName );
					
					//mark for initial local write despite read operation
					_requiresLocalWrite = CACHING_WRITE_CACHE_ON_READ;
				}
				else {
					//read matrix from rdd (incl execute pending rdd operations)
					MutableBoolean writeStatus = new MutableBoolean();
					_data = readBlobFromRDD( getRDDHandle(), writeStatus );
					
					//mark for initial local write (prevent repeated execution of rdd operations)
					_requiresLocalWrite = writeStatus.booleanValue() ? 
						CACHING_WRITE_CACHE_ON_READ : true;
				}
				
				setDirty(false);
			}
			catch (IOException e) {
				throw new DMLRuntimeException("Reading of " + _hdfsFileName + " ("+hashCode()+") failed.", e);
			}
			_isAcquireFromEmpty = true;
		}
		else if( _data!=null && DMLScript.STATISTICS ) {
			CacheStatistics.incrementMemHits();
		}
		
		//cache status maintenance
		acquire( false, _data==null );
		return _data;
	}
	
	/**
	 * Acquires the exclusive "write" lock for a thread that wants to throw away the
	 * old cache block data and link up with new cache block data. Abandons the old data
	 * without reading it and sets the new data reference.

	 * In-Status:  EMPTY, EVICTABLE, EVICTED;
	 * Out-Status: MODIFY.
	 * 
	 * @param newData new data
	 * @return cacheable data
	 */
	public T acquireModify(T newData) {
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		//core internal acquire (synchronized per object)
		T ret = acquireModifyIntern(newData);
		
		//update thread-local status (after pin but outside the
		//critical section of accessing a shared object)
		if( !isBelowCachingThreshold() )
			updateStatusPinned(true);
		
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementAcquireMTime(t1-t0);
			if (DMLScript.JMLC_MEM_STATISTICS)
				Statistics.addCPMemObject(System.identityHashCode(this), getDataSize());
		}
		
		return ret;
	}
	
	private synchronized T acquireModifyIntern(T newData) {
		if (! isAvailableToModify ())
			throw new DMLRuntimeException("CacheableData not available to modify.");
		
		//clear old data
		clearData();
		
		//cache status maintenance
		acquire (true, false); //no need to load evicted matrix
		
		setDirty(true);
		_isAcquireFromEmpty = false;
		
		//set references to new data
		if (newData == null)
			throw new DMLRuntimeException("acquireModify with empty cache block.");
		return _data = newData;
	}
	
	/**
	 * Releases the shared ("read-only") or exclusive ("write") lock.  Updates
	 * size information, last-access time, metadata, etc.
	 * 
	 * Synchronized because there might be parallel threads (parfor local) that
	 * access the same object (in case it was created before the loop).
	 * 
	 * In-Status:  READ, MODIFY;
	 * Out-Status: READ(-1), EVICTABLE, EMPTY.
	 * 
	 */
	public void release() {
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		//update thread-local status (before unpin but outside
		//the critical section of accessing a shared object)
		if( !isBelowCachingThreshold() )
			updateStatusPinned(false);
		
		//core internal release (synchronized per object)
		releaseIntern();
		
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementReleaseTime(t1-t0);
		}
	}
	
	private synchronized void releaseIntern() {
		boolean write = false;
		if ( isModify() ) {
			//set flags for write
			write = true;
			setDirty(true);
			
			//update meta data
			refreshMetaData();
			
			//compact empty in-memory block 
			_data.compactEmptyBlock();
		}
		
		//cache status maintenance (pass cacheNoWrite flag)
		release(_isAcquireFromEmpty && !_requiresLocalWrite);
		
		if( isCachingActive() //only if caching is enabled (otherwise keep everything in mem)
			&& isCached(true) //not empty and not read/modify
			&& !isBelowCachingThreshold() ) //min size for caching
		{
			if( ( write && !hasValidLineage() ) || _requiresLocalWrite ) {
				String filePath = getCacheFilePathAndName();
				try {
					LazyWriteBuffer.writeBlock(filePath, _data);
				}
				catch (Exception e) {
					throw new DMLRuntimeException("Eviction to local path " + filePath + " ("+hashCode()+") failed.", e);
				}
				_requiresLocalWrite = false;
			}

			if( DMLScript.STATISTICS && write && hasValidLineage() )
				CacheStatistics.incrementLinWrites();
			
			//create cache
			createCache();
			_data = null;
		}
	}
	
	public void clearData() {
		clearData(-1);
	}

	public void cleanData() {
		createCache();
		_data = null;
	}
	
	/**
	 * Sets the cache block reference to <code>null</code>, abandons the old block.
	 * Makes the "envelope" empty.  Run it to finalize the object (otherwise the
	 * evicted cache block file may remain undeleted).
	 * 
	 * In-Status:  EMPTY, EVICTABLE, EVICTED;
	 * Out-Status: EMPTY.
	 * 
	 * @param tid thread ID
	 * 
	 */
	public synchronized void clearData(long tid) 
	{
		// check if cleanup enabled and possible 
		if( !isCleanupEnabled() ) 
			return; // do nothing
		if( !isAvailableToModify() )
			throw new DMLRuntimeException("CacheableData (" + getDebugName() + ") not available to "
					+ "modify. Status = " + _cacheStatus.name() + ".");
		
		// clear existing WB / FS representation (but prevent unnecessary probes)
		if( !(isEmpty(true)||(_data!=null && isBelowCachingThreshold()) 
			  ||(_data!=null && !isCachingActive()) )) //additional condition for JMLC
			freeEvictedBlob();

		// clear the in-memory data
		_data = null;
		clearCache();
		setCacheLineage(null);
		
		// clear rdd/broadcast back refs
		if( _rddHandle != null )
			_rddHandle.setBackReference(null);
		if( _bcHandle != null )
			_bcHandle.setBackReference(null);
		if( _gpuObjects != null ) {
			for (GPUObject gObj : _gpuObjects.values())
				if (gObj != null) {
					gObj.clearData(null, DMLScript.EAGER_CUDA_FREE);
					if (gObj.isLinCached())
						// set rmVarPending which helps detecting liveness
						gObj.setrmVarPending(true);
				}
		}
		
		//clear federated matrix
		if( _fedMapping != null )
			_fedMapping.execCleanup(tid, _fedMapping.getID());
		
		// change object state EMPTY
		setDirty(false);
		setEmpty();
	}

	public synchronized void exportData() {
		exportData( -1 );
	}
	
	/**
	 * Writes, or flushes, the cache block data to HDFS.
	 * 
	 * In-Status:  EMPTY, EVICTABLE, EVICTED, READ;
	 * Out-Status: EMPTY, EVICTABLE, EVICTED, READ.
	 * 
	 * @param replication ?
	 */
	public synchronized void exportData( int replication ) {
		exportData(_hdfsFileName, null, replication, null);
		_hdfsFileExists = true;
	}

	public synchronized void exportData(String fName, String outputFormat) {
		exportData(fName, outputFormat, -1, null);
	}

	public synchronized void exportData(String fName, String outputFormat, FileFormatProperties formatProperties) {
		exportData(fName, outputFormat, -1, formatProperties);
	}
	
	public synchronized void exportData (String fName, String outputFormat, int replication, FileFormatProperties formatProperties) {
		exportData(fName, outputFormat, replication, formatProperties, null);
	}
	
	/**
	 * Synchronized because there might be parallel threads (parfor local) that
	 * access the same object (in case it was created before the loop).
	 * If all threads export the same data object concurrently it results in errors
	 * because they all write to the same file. Efficiency for loops and parallel threads
	 * is achieved by checking if the in-memory block is dirty.
	 * 
	 * NOTE: MB: we do not use dfs copy from local (evicted) to HDFS because this would ignore
	 * the output format and most importantly would bypass reblocking during write (which effects the
	 * potential degree of parallelism). However, we copy files on HDFS if certain criteria are given.  
	 * 
	 * @param fName file name
	 * @param outputFormat format
	 * @param replication ?
	 * @param formatProperties file format properties
	 * @param opcode instruction opcode if available
	 */
	public synchronized void exportData (String fName, String outputFormat, int replication, FileFormatProperties formatProperties, String opcode) {
		if( LOG.isTraceEnabled() )
			LOG.trace("Export data "+hashCode()+" "+fName);
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		//prevent concurrent modifications
		if ( !isAvailableToRead() )
			throw new DMLRuntimeException("MatrixObject not available to read.");

		LOG.trace("Exporting " + this.getDebugName() + " to " + fName + " in format " + outputFormat);
		
		if( DMLScript.USE_ACCELERATOR && _gpuObjects != null ) {
			boolean copiedFromGPU = false;
			for (Map.Entry<GPUContext, GPUObject> kv : _gpuObjects.entrySet()) {
				GPUObject gObj = kv.getValue();
				if (gObj != null && copiedFromGPU && gObj.isDirty()) {
					throw new DMLRuntimeException("Internal Error : Inconsistent internal state, A copy of this CacheableData was dirty on more than 1 GPU");
				} else if (gObj != null){
					copiedFromGPU = gObj.acquireHostRead(null);
					if( _data == null )
						getCache();
				}
			}
		}
		
		//check for persistent or transient writes
		boolean pWrite = !fName.equals(_hdfsFileName);
		if( !pWrite )
			setHDFSFileExists(true);
		
		//check for common file scheme (otherwise no copy/rename)
		boolean eqScheme = IOUtilFunctions.isSameFileScheme(
			new Path(_hdfsFileName), new Path(fName));
		boolean eqFormat = isEqualOutputFormat(outputFormat);
		boolean eqBlksize = (outputFormat == null || outputFormat.equals("binary"))
			&& ConfigurationManager.getBlocksize() != getBlocksize();
		
		//actual export (note: no direct transfer of local copy in order to ensure blocking (and hence, parallelism))
		if( isDirty() || !eqScheme || isFederated() ||
			(pWrite && (!eqFormat | !eqBlksize)) )
		{
			// CASE 1: dirty in-mem matrix or pWrite w/ different format (write matrix to fname; load into memory if evicted)
			// a) get the matrix
			boolean federatedWrite = (outputFormat != null ) &&  outputFormat.contains("federated");

			if( isEmpty(true) && !federatedWrite)
			{
				//read data from HDFS if required (never read before), this applies only to pWrite w/ different output formats
				//note: for large rdd outputs, we compile dedicated writespinstructions (no need to handle this here) 
				try {
					if( getRDDHandle()==null || getRDDHandle().allowsShortCircuitRead() )
						_data = readBlobFromHDFS( _hdfsFileName );
					else if( getRDDHandle() != null )
						_data = readBlobFromRDD( getRDDHandle(), new MutableBoolean() );
					else if(!federatedWrite)
						_data = readBlobFromFederated( getFedMapping() );
					setDirty(false);
					refreshMetaData(); //e.g., after unknown csv read
				}
				catch (IOException e) {
					throw new DMLRuntimeException("Reading of " + _hdfsFileName + " ("+hashCode()+") failed.", e);
				}
			}
			//get object from cache
			if(!federatedWrite) {
				if( _data == null )
					getCache();
				acquire( false, _data==null ); //incl. read matrix if evicted
			}

			// b) write the matrix 
			try {
				writeMetaData( fName, outputFormat, formatProperties );
				writeBlobToHDFS( fName, outputFormat, replication, formatProperties );
				if ( !pWrite )
					setDirty(false);
			}
			catch (Exception e) {
				throw new DMLRuntimeException("Export to " + fName + " failed.", e);
			}
			finally {
				if(!federatedWrite)
					release();
			}
		}
		else if( pWrite ) // pwrite with same output format
		{
			//CASE 2: matrix already in same format but different file on hdfs (copy matrix to fname)
			try
			{
				HDFSTool.deleteFileIfExistOnHDFS(fName);
				HDFSTool.deleteFileIfExistOnHDFS(fName+".mtd");
				if( getRDDHandle()==null || getRDDHandle().allowsShortCircuitRead() )
					HDFSTool.copyFileOnHDFS( _hdfsFileName, fName );
				else //write might trigger rdd operations and nnz maintenance
					writeBlobFromRDDtoHDFS(getRDDHandle(), fName, outputFormat);
				writeMetaData( fName, outputFormat, formatProperties );
			}
			catch (Exception e) {
				throw new DMLRuntimeException("Export to " + fName + " failed.", e);
			}
		}
		else if( getRDDHandle()!=null && getRDDHandle().isPending()
			&& !getRDDHandle().isHDFSFile() 
			&& !getRDDHandle().allowsShortCircuitRead() )
		{
			//CASE 3: pending rdd operation (other than checkpoints)
			try
			{
				//write matrix or frame
				writeBlobFromRDDtoHDFS(getRDDHandle(), fName, outputFormat);
				writeMetaData( fName, outputFormat, formatProperties );

				//update rdd status
				getRDDHandle().setPending(false);
			}
			catch (Exception e) {
				throw new DMLRuntimeException("Export to " + fName + " failed.", e);
			}
		}
		else 
		{
			//CASE 4: data already in hdfs (do nothing, no need for export)
			LOG.trace(this.getDebugName() + ": Skip export to hdfs since data already exists.");
		}
		  
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementExportTime(t1-t0);
		}
	}
	
	// --------- ABSTRACT LOW-LEVEL CACHE I/O OPERATIONS ----------

	/**
	 * Checks if the data blob reference points to some in-memory object.
	 * This method is called when releasing the (last) lock. Do not call 
	 * this method for a blob that has been evicted.
	 *
	 * @return <code>true</code> if the blob is in main memory and the
	 * reference points to it;
	 * <code>false</code> if the blob reference is <code>null</code>.
	 */
	protected boolean isBlobPresent() {
		return (_data != null);
	}

	/**
	 * Low-level cache I/O method that physically restores the data blob to
	 * main memory. Must be defined by a subclass, never called by users.
	 *
	 */
	protected void restoreBlobIntoMemory() {
		String cacheFilePathAndName = getCacheFilePathAndName();
		long begin = LOG.isTraceEnabled() ? System.currentTimeMillis() : 0;
		
		if( LOG.isTraceEnabled() )
			LOG.trace ("CACHE: Restoring matrix...  " + hashCode() + "  HDFS path: " + 
						(_hdfsFileName == null ? "null" : _hdfsFileName) + ", Restore from path: " + cacheFilePathAndName);
				
		if (_data != null)
			throw new DMLRuntimeException(cacheFilePathAndName + " : Cannot restore on top of existing in-memory data.");

		try {
			_data = readBlobFromCache(cacheFilePathAndName);
		}
		catch (IOException e) {
			throw new DMLRuntimeException(cacheFilePathAndName + " : Restore failed.", e);	
		}
		
		//check for success
		if (_data == null)
			throw new DMLRuntimeException (cacheFilePathAndName + " : Restore failed.");
		
		if( LOG.isTraceEnabled() )
			LOG.trace("Restoring matrix - COMPLETED ... " + (System.currentTimeMillis()-begin) + " msec.");
	}

	protected abstract T readBlobFromCache(String fname)
		throws IOException;
	
	/**
	 * Low-level cache I/O method that deletes the file containing the
	 * evicted data blob, without reading it.
	 * Must be defined by a subclass, never called by users.
	 */
	public final void freeEvictedBlob() {
		String cacheFilePathAndName = getCacheFilePathAndName();
		long begin = LOG.isTraceEnabled() ? System.currentTimeMillis() : 0;
		if( LOG.isTraceEnabled() )
			LOG.trace("CACHE: Freeing evicted matrix...  " + hashCode() + "  HDFS path: " + 
				(_hdfsFileName == null ? "null" : _hdfsFileName) + " Eviction path: " + cacheFilePathAndName);
		
		if(isCachingActive())
			LazyWriteBuffer.deleteBlock(cacheFilePathAndName);
		
		if( LOG.isTraceEnabled() )
			LOG.trace("Freeing evicted matrix - COMPLETED ... " + (System.currentTimeMillis()-begin) + " msec.");
	}

	protected boolean isBelowCachingThreshold() {
		return (_data.getInMemorySize() <= CACHING_THRESHOLD);
	}
	
	public static boolean isBelowCachingThreshold(CacheBlock data) {
		return LazyWriteBuffer.getCacheBlockSize(data) <= CACHING_THRESHOLD;
	}
	
	public long getDataSize() {
		return (_data != null) ?_data.getInMemorySize() : 0;
	}
	
	protected ValueType[] getSchema() {
		return null;
	}

	@Override //Data
	public synchronized String getDebugName() {
		int maxLength = 23;
		String debugNameEnding = (_hdfsFileName == null ? "null" : 
			(_hdfsFileName.length() < maxLength ? _hdfsFileName : "..." + 
				_hdfsFileName.substring (_hdfsFileName.length() - maxLength + 3)));
		return hashCode() + " " + debugNameEnding;
	}

	//HDFS read
	protected T readBlobFromHDFS(String fname) throws IOException {
		MetaDataFormat iimd = (MetaDataFormat) _metaData;
		DataCharacteristics dc = iimd.getDataCharacteristics();
		return readBlobFromHDFS(fname, dc.getDims());
	}

	protected abstract T readBlobFromHDFS(String fname, long[] dims)
		throws IOException;

	//RDD read
	protected abstract T readBlobFromRDD(RDDObject rdd, MutableBoolean status)
		throws IOException;

	// Federated read
	protected T readBlobFromFederated(FederationMap fedMap) throws IOException {
		if( LOG.isDebugEnabled() ) //common if instructions keep federated outputs
			LOG.debug("Pulling data from federated sites");
		MetaDataFormat iimd = (MetaDataFormat) _metaData;
		DataCharacteristics dc = iimd.getDataCharacteristics();
		return readBlobFromFederated(fedMap, dc.getDims());
	}
	
	protected abstract T readBlobFromFederated(FederationMap fedMap, long[] dims)
		throws IOException;

	protected abstract void writeBlobToHDFS(String fname, String ofmt, int rep, FileFormatProperties fprop)
		throws IOException;

	protected abstract void writeBlobFromRDDtoHDFS(RDDObject rdd, String fname, String ofmt)
		throws IOException;

	protected abstract T reconstructByLineage(LineageItem li)
		throws IOException;

	
	protected void writeMetaData (String filePathAndName, String outputFormat, FileFormatProperties formatProperties)
		throws IOException
	{	
		MetaDataFormat iimd = (MetaDataFormat) _metaData;
	
		if (iimd == null)
			throw new DMLRuntimeException("Unexpected error while writing mtd file (" + filePathAndName + ") -- metadata is null.");
		
		// Write the matrix to HDFS in requested format
		FileFormat fmt = (outputFormat != null) ? FileFormat.safeValueOf(outputFormat) : iimd.getFileFormat();
		if ( fmt != FileFormat.MM ) {
			// Get the dimension information from the metadata stored within MatrixObject
			DataCharacteristics dc = iimd.getDataCharacteristics();
			
			// when outputFormat is binaryblock, make sure that matrixCharacteristics has correct blocking dimensions
			// note: this is only required if singlenode (due to binarycell default) 
			if ( fmt == FileFormat.BINARY && DMLScript.getGlobalExecMode() == ExecMode.SINGLE_NODE
				&& dc.getBlocksize() != ConfigurationManager.getBlocksize() )
			{
				dc = new MatrixCharacteristics(dc.getRows(), dc.getCols(), dc.getBlocksize(), dc.getNonZeros());
			}
			
			//write the actual meta data file
			HDFSTool.writeMetaDataFile (filePathAndName + ".mtd", valueType, 
				getSchema(), dataType, dc, fmt, formatProperties, _privacyConstraint);
		}
	}

	protected boolean isEqualOutputFormat(String outputFormat) {
		if( outputFormat != null ) {
			MetaDataFormat iimd = (MetaDataFormat) _metaData;
			return iimd.getFileFormat().toString().equals(outputFormat);
		}
		return true;
	}
	
	// ------------- IMPLEMENTED CACHE LOGIC METHODS --------------
	
	protected String getCacheFilePathAndName () {
		if( _cacheFileName==null ) {
			StringBuilder sb = new StringBuilder();
			sb.append(CacheableData.cacheEvictionLocalFilePath);
			sb.append(CacheableData.cacheEvictionLocalFilePrefix);
			sb.append(String.format ("%09d", _uniqueID));
			sb.append(CacheableData.CACHING_EVICTION_FILEEXTENSION);
			_cacheFileName = sb.toString();
		}
		
		return _cacheFileName;
	}
	
	/**
	 * This method "acquires the lock" to ensure that the data blob is in main memory
	 * (not evicted) while it is being accessed.  When called, the method will try to
	 * restore the blob if it has been evicted.  There are two kinds of locks it may
	 * acquire: a shared "read" lock (if the argument is <code>false</code>) or the 
	 * exclusive "modify" lock (if the argument is <code>true</code>).
	 * The method can fail in three ways:
	 * (1) if there is lock status conflict;
	 * (2) if there is not enough cache memory to restore the blob;
	 * (3) if the restore method returns an error.
	 * The method locks the data blob in memory (which disables eviction) and updates
	 * its last-access timestamp.  For the shared "read" lock, acquiring a new lock
	 * increments the associated count.  The "read" count has to be decremented once
	 * the blob is no longer used, which may re-enable eviction.  This method has to
	 * be called only once per matrix operation and coupled with {@link #release()}, 
	 * because it increments the lock count and the other method decrements this count.
	 * 
	 * @param isModify : <code>true</code> for the exclusive "modify" lock,
	 *     <code>false</code> for a shared "read" lock.
	 * @param restore true if restore
	 */
	protected void acquire (boolean isModify, boolean restore) {
		switch ( _cacheStatus )
		{
			case CACHED:
				if(restore)
					restoreBlobIntoMemory();
			case CACHED_NOWRITE:
			case EMPTY:
				if (isModify)
					setModify();
				else
					addOneRead();
				break;
			case READ:
				if (isModify)
					throw new DMLRuntimeException("READ-MODIFY not allowed.");
				else
					addOneRead();
				break;
			case MODIFY:
				throw new DMLRuntimeException("MODIFY-MODIFY not allowed.");
		}

		if( LOG.isTraceEnabled() )
			LOG.trace("Acquired lock on " + getDebugName() + ", status: " + _cacheStatus.name() );
	}

	
	/**
	 * Call this method to permit eviction for the stored data blob, or to
	 * decrement its "read" count if it is "read"-locked by other threads.
	 * It is expected that you eliminate all external references to the blob
	 * prior to calling this method, because otherwise eviction will
	 * duplicate the blob, but not release memory.  This method has to be
	 * called only once per process and coupled with {@link #acquire(boolean, boolean)},
	 * because it decrements the lock count and the other method increments
	 * the lock count.
	 * 
	 * @param cacheNoWrite ?
	 */
	protected void release(boolean cacheNoWrite)
	{
		switch ( _cacheStatus )
		{
			case EMPTY:
			case CACHED:
			case CACHED_NOWRITE:
				throw new DMLRuntimeException("Redundant release.");
			case READ:
				removeOneRead( isBlobPresent(), cacheNoWrite );
				break;
			case MODIFY:
				if ( isBlobPresent() )
					setCached();
				else
					setEmpty();
				break;
		}
		
		if( LOG.isTraceEnabled() )
			LOG.trace("Released lock on " + getDebugName() + ", status: " + _cacheStatus.name());
		
	}

	
	//  **************************************************
	//  ***                                            ***
	//  ***  CACHE STATUS FIELD - CLASSES AND METHODS  ***
	//  ***                                            ***
	//  **************************************************
	
	public boolean isCached(boolean inclCachedNoWrite) {
		return _cacheStatus == CacheStatus.CACHED
			|| (inclCachedNoWrite && _cacheStatus == CacheStatus.CACHED_NOWRITE);
	}
	
	public void setEmptyStatus() {
		setEmpty();
	}
	
	protected boolean isEmpty(boolean inclCachedNoWrite) {
		return _cacheStatus == CacheStatus.EMPTY
			|| (inclCachedNoWrite && _cacheStatus == CacheStatus.CACHED_NOWRITE);
	}
	
	protected boolean isModify() {
		return (_cacheStatus == CacheStatus.MODIFY);
	}
	
	protected void setEmpty() {
		_cacheStatus = CacheStatus.EMPTY;
	}
	
	protected void setModify() {
		_cacheStatus = CacheStatus.MODIFY;
	}
	
	protected void setCached() {
		_cacheStatus = CacheStatus.CACHED;
	}

	protected void addOneRead() {
		_numReadThreads ++;
		_cacheStatus = CacheStatus.READ;
	}
	
	protected void removeOneRead(boolean doesBlobExist, boolean cacheNoWrite) {
		_numReadThreads --;
		if (_numReadThreads == 0) {
			if( cacheNoWrite )
				_cacheStatus = (doesBlobExist ? 
					CacheStatus.CACHED_NOWRITE : CacheStatus.EMPTY);
			else
				_cacheStatus = (doesBlobExist ? 
					CacheStatus.CACHED : CacheStatus.EMPTY);
		}
	}
	
	protected boolean isAvailableToRead() {
		return (_cacheStatus != CacheStatus.MODIFY);
	}
	
	protected boolean isAvailableToModify() {
		return (   _cacheStatus == CacheStatus.EMPTY 
				|| _cacheStatus == CacheStatus.CACHED
				|| _cacheStatus == CacheStatus.CACHED_NOWRITE);
	}

	// *******************************************
	// ***                                     ***
	// ***      LOW-LEVEL PRIVATE METHODS      ***
	// ***       FOR SOFTREFERENCE CACHE       ***
	// ***                                     ***
	// *******************************************
	
	/**
	 * Creates a new cache soft reference to the currently
	 * referenced cache block.  
	 */
	protected void createCache( ) {
		if( _cache == null || _cache.get() == null )
			_cache = new SoftReference<>( _data );
	}

	/**
	 * Tries to get the cache block from the cache soft reference
	 * and subsequently clears the cache soft reference if existing.
	 */
	protected void getCache() {
		if( _cache != null ) {
			_data = _cache.get();
		}
	}
	
	/** Clears the cache soft reference if existing. */
	protected void clearCache() {
		if( _cache != null ) {
			_cache.clear();
			_cache = null;
		}
	}

	protected void updateStatusPinned(boolean add) {
		if( _data == null || !OptimizerUtils.isHybridExecutionMode() )
			return; //avoid size computation for string frames
		long size = sizePinned.get();
		size += (add ? 1 : -1) * _data.getInMemorySize();
		sizePinned.set( Math.max(size,0) );
	}

	protected static long getPinnedSize() {
		return sizePinned.get();
	}
	
	public static void addBroadcastSize(long size) {
		_refBCs.addAndGet(size);
	}
	
	public static long getBroadcastSize() {
		//scale the total sum of all broadcasts by the current fraction
		//of local memory to equally distribute it across parfor workers
		return (long) (_refBCs.longValue() *
			InfrastructureAnalyzer.getLocalMaxMemoryFraction());
	}
	
	// --------- STATIC CACHE INIT/CLEANUP OPERATIONS ----------

	public synchronized static void cleanupCacheDir() {
		//cleanup remaining cached writes
		LazyWriteBuffer.cleanup();
		
		//delete cache dir and files
		cleanupCacheDir(true);
	}
	
	/**
	 * Deletes the DML-script-specific caching working dir.
	 * 
	 * @param withDir if true, delete directory
	 */
	public synchronized static void cleanupCacheDir(boolean withDir)
	{
		//get directory name
		String dir = cacheEvictionLocalFilePath;
		
		//clean files with cache prefix
		if( dir != null ) //if previous init cache
		{
			File fdir = new File(dir);
			if( fdir.exists()){ //just for robustness
				File[] files = fdir.listFiles();
				for( File f : files )
					if( f.getName().startsWith(cacheEvictionLocalFilePrefix) )
						f.delete();
				if( withDir )
					fdir.delete(); //deletes dir only if empty
			}
		}
		
		_activeFlag = false;
	}
	
	/**
	 * Inits caching with the default uuid of DMLScript
	 * 
	 * @throws IOException if IOException occurs
	 */
	public synchronized static void initCaching() 
		throws IOException
	{
		initCaching(DMLScript.getUUID());
	}
	
	/**
	 * Creates the DML-script-specific caching working dir.
	 * 
	 * Takes the UUID in order to allow for custom uuid, e.g., for remote parfor caching
	 * 
	 * @param uuid ID
	 * @throws IOException if IOException occurs
	 */
	public synchronized static void initCaching( String uuid ) 
		throws IOException
	{
		try
		{
			String dir = LocalFileUtils.getWorkingDir( LocalFileUtils.CATEGORY_CACHE );
			LocalFileUtils.createLocalFileIfNotExist(dir);
			cacheEvictionLocalFilePath = dir;
		}
		catch(DMLRuntimeException e)
		{
			throw new IOException(e);
		}
	
		//init write-ahead buffer
		LazyWriteBuffer.init();
		_refBCs.set(0);
		
		_activeFlag = true; //turn on caching
	}
	
	public static boolean isCachingActive() {
		return _activeFlag;
	}
	
	public static void disableCaching() {
		_activeFlag = false;
	}
	
	public static void enableCaching() {
		_activeFlag = true;
	}

	public synchronized boolean moveData(String fName, String outputFormat) {
		boolean ret = false;
		
		try
		{
			//check for common file scheme (otherwise no copy/rename)
			boolean eqScheme = IOUtilFunctions.isSameFileScheme(
				new Path(_hdfsFileName), new Path(fName));
			
			//export or rename to target file on hdfs
			if( isDirty() || !eqScheme || (!isEqualOutputFormat(outputFormat) && isEmpty(true)) 
				|| (getRDDHandle()!=null && !HDFSTool.existsFileOnHDFS(_hdfsFileName)) )
			{
				exportData(fName, outputFormat);
				ret = true;
			}
			else if( isEqualOutputFormat(outputFormat) )
			{
				HDFSTool.deleteFileIfExistOnHDFS(fName);
				HDFSTool.deleteFileIfExistOnHDFS(fName+".mtd");
				HDFSTool.renameFileOnHDFS( _hdfsFileName, fName );
				writeMetaData( fName, outputFormat, null );
				ret = true;
			}
		}
		catch (Exception e) {
			throw new DMLRuntimeException("Move to " + fName + " failed.", e);
		}
		
		return ret;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(getClass().getSimpleName());
		str.append(": ");
		str.append(_hdfsFileName + ", ");
		try {
			MetaDataFormat md = (MetaDataFormat) _metaData;
			if (md != null) {
				DataCharacteristics dc = _metaData.getDataCharacteristics();
				str.append(dc.toString());
				if (md.getFileFormat() == null)
					str.append("null");
				else {
					str.append(", ");
					str.append(md.getFileFormat().toString());
				}
			} else {
				str.append("null, null");
			}
		} catch (Exception ex) {
			LOG.error(ex);
		}
		str.append(", ");
		str.append(isDirty() ? "dirty" : "not-dirty");

		return str.toString();
	}
}
