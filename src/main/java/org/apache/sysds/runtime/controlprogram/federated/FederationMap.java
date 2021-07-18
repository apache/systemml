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

package org.apache.sysds.runtime.controlprogram.federated;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.cp.VariableCPInstruction;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.CommonThreadPool;
import org.apache.sysds.runtime.util.IndexRange;

public class FederationMap {
	public enum FPartitioning{
		ROW,   //row partitioned, groups of entire rows
		COL,   //column partitioned, groups of entire columns
		MIXED, //arbitrary rectangles
		NONE,  //entire data in a location
	}

	public enum FReplication {
		NONE,    //every data item in a separate location
		FULL,    //every data item at every location
		OVERLAP, //every data item partially at every location, w/ addition as aggregation method
	}

	public enum FType {
		ROW(FPartitioning.ROW, FReplication.NONE),
		COL(FPartitioning.COL, FReplication.NONE),
		FULL(FPartitioning.NONE, FReplication.NONE),
		BROADCAST(FPartitioning.NONE, FReplication.FULL),
		PART(FPartitioning.NONE, FReplication.OVERLAP),
		OTHER(FPartitioning.MIXED, FReplication.NONE);

		private FPartitioning _partType;
		@SuppressWarnings("unused") //not yet
		private final FReplication _repType;

		private FType(FPartitioning ptype, FReplication rtype) {
			_partType = ptype;
			_repType = rtype;
		}

		public boolean isRowPartitioned() {
			return _partType == FPartitioning.ROW
				|| _partType == FPartitioning.NONE;
		}

		public boolean isColPartitioned() {
			return _partType == FPartitioning.COL
				|| _partType == FPartitioning.NONE;
		}

		public FPartitioning getPartType() {
			return this._partType;
		}

		public void setPartType(FPartitioning _partType) {
			this._partType = _partType;
		}

		public boolean isType(FType t) {
			switch(t) {
				case ROW:
					return isRowPartitioned();
				case COL:
					return isColPartitioned();
				case FULL:
				case OTHER:
				default:
					return t == this;
			}
		}
	}

	// Alignment Check Type
	public enum AlignType {
		FULL, // exact matching dimensions of partitions on the same federated worker
		ROW, // matching rows of partitions on the same federated worker
		COL, // matching columns of partitions on the same federated worker
		FULL_T, // matching dimensions with transposed dimensions of partitions on the same federated worker
		ROW_T, // matching rows with columns of partitions on the same federated worker
		COL_T; // matching columns with rows of partitions on the same federated worker

		public boolean isTransposed() {
			return (this == FULL_T || this == ROW_T || this == COL_T);
		}
		public boolean isFullType() {
			return (this == FULL || this == FULL_T);
		}
		public boolean isRowType() {
			return (this == ROW || this == ROW_T);
		}
		public boolean isColType() {
			return (this == COL || this == COL_T);
		}
	}

	private long _ID = -1;
	private final List<Pair<FederatedRange, FederatedData>> _fedMap;
	private FType _type;

	public FederationMap(List<Pair<FederatedRange, FederatedData>> fedMap) {
		this(-1, fedMap);
	}

	public FederationMap(long ID, List<Pair<FederatedRange, FederatedData>> fedMap) {
		this(ID, fedMap, FType.OTHER);
	}

	public FederationMap(long ID, List<Pair<FederatedRange, FederatedData>> fedMap, FType type) {
		_ID = ID;
		_fedMap = fedMap;
		_type = type;
	}

	public long getID() {
		return _ID;
	}

	public FType getType() {
		return _type;
	}

	public boolean isInitialized() {
		return _ID >= 0;
	}

	public void setType(FType type) {
		_type = type;
	}

	public int getSize() {
		return _fedMap.size();
	}

	public FederatedRange[] getFederatedRanges() {
		return _fedMap.stream().map(e -> e.getKey()).toArray(FederatedRange[]::new);
	}

	public FederatedData[] getFederatedData() {
		return _fedMap.stream().map(e -> e.getValue()).toArray(FederatedData[]::new);
	}

	private FederatedData getFederatedData(FederatedRange range) {
		for( Pair<FederatedRange, FederatedData> e : _fedMap )
			if( e.getKey().equals(range) )
				return e.getValue();
		return null;
	}

	private void removeFederatedData(FederatedRange range) {
		Iterator<Pair<FederatedRange, FederatedData>> iter = _fedMap.iterator();
		while( iter.hasNext() )
			if( iter.next().getKey().equals(range) )
				iter.remove();
	}

	public List<Pair<FederatedRange, FederatedData>> getMap() {
		return _fedMap;
	}

	public FederatedRequest broadcast(CacheableData<?> data, long oldId) {
		long id;
		boolean exists = Arrays.stream(_fedMap.stream().map(e -> Triple.of(data.getUniqueID(), FType.BROADCAST, e.getRight().getAddress())).toArray())
			.allMatch(e -> FederationUtils._broadcastMap.containsKey(e));


		if(exists) {
			id = FederationUtils._broadcastMap.get(Triple.of(data.getUniqueID(), FType.BROADCAST,
				((Pair<FederatedRange, FederatedData>) _fedMap.toArray()[0]).getRight().getAddress())).getLeft();
			return new FederatedRequest(RequestType.PUT_VAR, id, data.getUniqueID(), FType.BROADCAST);
		}

		id = FederationUtils.getNextFedDataID();
		CacheBlock cb = data.acquireReadAndRelease();
//		List<Pair<FederatedRange, FederatedData>> newFedMap = new ArrayList<>();

		for(Pair<FederatedRange, FederatedData> e : _fedMap) {
			FederationUtils._broadcastMap.putIfAbsent(Triple.of(data.getUniqueID(), FType.BROADCAST, e.getValue().getAddress()),
				Pair.of(id, false));
			FederationUtils._broadcastMap.putIfAbsent(Triple.of(oldId, _type, e.getValue().getAddress()),
				Pair.of(_ID, false));
//			newFedMap.add(Pair.of(new FederatedRange(new long[] {0, 0}, new long[] {data.getNumRows(), data.getNumColumns()}),
//				new FederatedData(data.getDataType(), e.getRight().getAddress(), data.getFileName())));
		}
//		data.setFedMapping(new FederationMap(id, newFedMap, FType.BROADCAST)); // TODO is really necessary to modify since it can not be reused as normal fed matrix without cutting
		return new FederatedRequest(RequestType.PUT_VAR, id, cb, data.getUniqueID(), FType.BROADCAST);
	}

	public FederatedRequest broadcast(ScalarObject scalar) {
		// prepare single request for all federated data
		long id = FederationUtils.getNextFedDataID();
		return new FederatedRequest(RequestType.PUT_VAR, id, scalar);
	}

	/**
	 * Creates separate slices of an input data object according to the index ranges of federated data. Theses slices
	 * are then wrapped in separate federated requests for broadcasting.
	 *
	 * @param data       input data object (matrix, tensor, frame)
	 * @param transposed false: slice according to federated data, true: slice according to transposed federated data
	 * @param oldId id of the marix object that calls broadcast
	 * @return array of federated requests corresponding to federated data
	 */
	public FederatedRequest[] broadcastSliced(CacheableData<?> data, boolean transposed, long oldId) {
		if( _type == FType.FULL )
			return new FederatedRequest[]{broadcast(data, oldId)};

		long id;
		FederatedRequest[] ret = new FederatedRequest[_fedMap.size()];

		FType type = FType.PART;
		type.setPartType(_type._partType);

		boolean exists = Arrays.stream(_fedMap.stream().map(e -> Triple.of(data.getUniqueID(), type, e.getRight().getAddress())).toArray())
			.allMatch(e -> FederationUtils._broadcastMap.containsKey(e) && ((Triple<Long, FType, InetSocketAddress>) e).getMiddle()._partType == _type._partType
				&& FederationUtils._broadcastMap.get(e).getRight() == transposed);

		if(exists) {
			id = FederationUtils._broadcastMap.get(Triple.of(data.getUniqueID(), FType.PART,
				((Pair<FederatedRange, FederatedData>) _fedMap.toArray()[0]).getRight().getAddress())).getLeft();
			Arrays.fill(ret, new FederatedRequest(RequestType.PUT_VAR, id, data.getUniqueID(), FType.PART));
		} else {
			// prepare broadcast id and pin input
			id = FederationUtils.getNextFedDataID();
			CacheBlock cb = data.acquireReadAndRelease();
			List<Pair<FederatedRange, FederatedData>> newFedMap = new ArrayList<>();

			// prepare indexing ranges
			int[][] ix = new int[_fedMap.size()][];
			int pos = 0;
			for(Pair<FederatedRange, FederatedData> e : _fedMap) {
				int beg = e.getKey().getBeginDimsInt()[(_type == FType.ROW || _type.getPartType() == FPartitioning.ROW ? 0 : 1)];
				int end = e.getKey().getEndDimsInt()[(_type == FType.ROW || _type.getPartType() == FPartitioning.ROW ? 0 : 1)];
				int nr = _type == FType.ROW || _type.getPartType() == FPartitioning.ROW ? cb.getNumRows() : cb.getNumColumns();
				int nc = _type == FType.ROW || _type.getPartType() == FPartitioning.ROW ? cb.getNumColumns() : cb.getNumRows();
				int rl = transposed ? 0 : beg;
				int ru = transposed ? nr - 1 : end - 1;
				int cl = transposed ? beg : 0;
				int cu = transposed ? end - 1 : nc - 1;
				ix[pos++] = _type == FType.ROW || _type.getPartType() == FPartitioning.ROW ? new int[] {rl, ru, cl, cu} : new int[] {cl, cu, rl, ru};

				FederationUtils._broadcastMap.putIfAbsent(Triple.of(data.getUniqueID(), type, e.getValue().getAddress()),
					Pair.of(id, transposed));

				FederationUtils._broadcastMap.putIfAbsent(Triple.of(oldId, type, e.getValue().getAddress()),
					Pair.of(getID(), transposed));

				FederatedRange fr = new FederatedRange(e.getLeft());
				fr.setEndDim(_type == FType.ROW || (_type == FType.PART && _type.getPartType() == FPartitioning.ROW) ? 1 : 0,
					_type == FType.ROW || (_type == FType.PART && _type.getPartType() == FPartitioning.ROW) ?
						data.getNumColumns() : data.getNumRows());
				newFedMap.add(Pair.of(fr,
					new FederatedData(data.getDataType(), e.getRight().getAddress(), data.getFileName())));
			}

			// multi-threaded block slicing and federation request creation
			Arrays.parallelSetAll(ret,i -> new FederatedRequest(RequestType.PUT_VAR, id, cb.slice(ix[i][0], ix[i][1], ix[i][2], ix[i][3],
				new MatrixBlock()), data.getUniqueID(), FType.PART, getID(), oldId));

			if(!data.isFederated()) {
				data.cleanData();

				data.setFedMapping(new FederationMap(id, newFedMap, FType.PART));
				data.getFedMapping()._type.setPartType(type.getPartType());
				data.getDataCharacteristics().setDimension(cb.getNumRows(), cb.getNumColumns());
			}
		}
		return ret;
	}

	public FederatedRequest[] broadcastSliced(CacheableData<?> data, boolean isFrame, int[][] ix, long oldID) {
		if( _type == FType.FULL )
			return new FederatedRequest[]{broadcast(data, oldID)};

		// prepare broadcast id and pin input
		long id = FederationUtils.getNextFedDataID();
		CacheBlock cb = data.acquireReadAndRelease();

		// multi-threaded block slicing and federation request creation
		FederatedRequest[] ret = new FederatedRequest[ix.length];
		Arrays.setAll(ret,
			i -> new FederatedRequest(RequestType.PUT_VAR, id,
				cb.slice(ix[i][0], ix[i][1], ix[i][2], ix[i][3], isFrame ? new FrameBlock() : new MatrixBlock())));
		return ret;
	}


	/**
	 * helper function for checking multiple allowed alignment types
	 * @param that FederationMap to check alignment with
	 * @param alignTypes collection of alignment types which should be checked
	 * @return true if this and that FederationMap are aligned according to at least one alignment type
	 */
	public boolean isAligned(FederationMap that, AlignType... alignTypes) {
		boolean ret = false;
		for(AlignType at : alignTypes) {
			if(at.isFullType())
				ret |= isAligned(that, at.isTransposed());
			else
				ret |= isAligned(that, at.isTransposed(), at.isRowType(), at.isColType());
			if(ret) // early stopping - alignment already found
				break;
		}
		return ret;
	}

	/**
	 * Determines if the two federation maps are aligned row/column partitions
	 * at the same federated sites (which allows for purely federated operation)
	 * @param that FederationMap to check alignment with
	 * @param transposed true if that FederationMap should be transposed before checking alignment
	 * @return true if this and that FederationMap are aligned
	 */
	public boolean isAligned(FederationMap that, boolean transposed) {
		boolean ret = true;
		if (_type == FederationMap.FType.BROADCAST)
			return false;
		for(Pair<FederatedRange, FederatedData> e : _fedMap) {
			FederatedRange range = !transposed ? e.getKey() : new FederatedRange(e.getKey()).transpose();
			FederatedData dat2 = that.getFederatedData(range);
			ret &= e.getValue().equalAddress(dat2);
		}
		return ret;
	}

	/**
	 * determines if the two federated data are aligned row/column partitions (depending on parameters equalRows/equalCols)
	 * at the same federated site (which often allows for purely federated operations)
	 * @param that FederationMap to check alignment with
	 * @param transposed true if that FederationMap should be transposed before checking alignment
	 * @param equalRows true to indicate that the row dimension should be checked for alignment
	 * @param equalCols true to indicate that the col dimension should be checked for alignment
	 * @return true if this and that FederationMap are aligned
	 */
	public boolean isAligned(FederationMap that, boolean transposed, boolean equalRows, boolean equalCols) {
		boolean ret = true;
		final int ROW_IX = transposed ? 1 : 0; // swapping row and col dimension index of "that" if transposed
		final int COL_IX = transposed ? 0 : 1;

		for(Pair<FederatedRange, FederatedData> e : _fedMap) {
			boolean rangeFound = false; // to indicate if at least one matching range has been found
			for(FederatedRange r : that.getFederatedRanges()) {
				long[] rbd = r.getBeginDims();
				long[] red = r.getEndDims();
				long[] ebd = e.getKey().getBeginDims();
				long[] eed = e.getKey().getEndDims();
				// searching for the matching federated range of "that"
				if((!equalRows || (rbd[ROW_IX] == ebd[0] && red[ROW_IX] == eed[0]))
					&& (!equalCols || (rbd[COL_IX] == ebd[1] && red[COL_IX] == eed[1]))) {
					rangeFound = true;
					FederatedData dat2 = that.getFederatedData(r);
					ret &= e.getValue().equalAddress(dat2); // both paritions must be located on the same fed worker
				}
			}
			if(!(ret &= rangeFound)) // setting ret to false if no matching range has been found
				break; // directly returning if not ret to skip further checks
		}
		return ret;
	}

	public Future<FederatedResponse>[] execute(long tid, FederatedRequest... fr) {
		return execute(tid, false, fr);
	}

	public Future<FederatedResponse>[] execute(long tid, boolean wait, FederatedRequest... fr) {
		return execute(tid, wait, null, fr);
	}

	public Future<FederatedResponse>[] execute(long tid, FederatedRequest[] frSlices, FederatedRequest... fr) {
		return execute(tid, false, frSlices, fr);
	}

	@SuppressWarnings("unchecked")
	public Future<FederatedResponse>[] execute(long tid, boolean wait, FederatedRequest[] frSlices,
		FederatedRequest... fr) {
		// executes step1[] - step 2 - ... step4 (only first step federated-data-specific)
		setThreadID(tid, frSlices, fr);
		List<Future<FederatedResponse>> ret = new ArrayList<>();
		int pos = 0;
		for(Pair<FederatedRange, FederatedData> e : _fedMap)
			ret.add(e.getValue().executeFederatedOperation((frSlices != null) ? addAll(frSlices[pos++], fr) : fr));

		// prepare results (future federated responses), with optional wait to ensure the
		// order of requests without data dependencies (e.g., cleanup RPCs)
		if(wait)
			FederationUtils.waitFor(ret);
		return ret.toArray(new Future[0]);
	}

	@SuppressWarnings("unchecked")
	public Future<FederatedResponse>[] execute(long tid, boolean wait, FederatedRange[] fedRange1, FederatedRequest elseFr, FederatedRequest[] frSlices1, FederatedRequest[] frSlices2, FederatedRequest... fr) {
		// executes step1[] - step 2 - ... step4 (only first step federated-data-specific)
		setThreadID(tid, frSlices1, fr);
		setThreadID(tid, frSlices2, fr);
		List<Future<FederatedResponse>> ret = new ArrayList<>();
		int pos = 0;
		for(Pair<FederatedRange, FederatedData> e : _fedMap) {
			if(Arrays.asList(fedRange1).contains(e.getKey())) {
				FederatedRequest[] newFr = (frSlices1 != null) ? ((frSlices2 != null) ? (addAll(frSlices2[pos],
					addAll(frSlices1[pos++], fr))) : addAll(frSlices1[pos++], fr)) : fr;
				ret.add(e.getValue().executeFederatedOperation(newFr));
			}
			else ret.add(e.getValue().executeFederatedOperation(elseFr));
		}

		// prepare results (future federated responses), with optional wait to ensure the
		// order of requests without data dependencies (e.g., cleanup RPCs)
		if( wait )
			FederationUtils.waitFor(ret);
		return ret.toArray(new Future[0]);
	}

	public Future<FederatedResponse>[] execute(long tid, boolean wait, FederatedRequest[] frSlices1, FederatedRequest[] frSlices2, FederatedRequest... fr) {
		return execute(tid, wait,
			_fedMap.stream().map(e->e.getKey()).toArray(FederatedRange[]::new),
			null, frSlices1, frSlices2, fr);
	}

	@SuppressWarnings("unchecked")
	public Future<FederatedResponse>[] executeMultipleSlices(long tid, boolean wait,
		FederatedRequest[][] frSlices, FederatedRequest[] fr) {
		// executes step1[] - ... - stepM[] - stepM+1 - ... stepN (only first step federated-data-specific)
		FederatedRequest[] allSlices = Arrays.stream(frSlices).flatMap(Stream::of).toArray(FederatedRequest[]::new);
		setThreadID(tid, allSlices, fr);
		List<Future<FederatedResponse>> ret = new ArrayList<>();
		int pos = 0;
		for(Pair<FederatedRange, FederatedData> e : _fedMap) {
			FederatedRequest[] fedReq = fr;
			for(FederatedRequest[] slice : frSlices)
				fedReq = addAll(slice[pos], fedReq);
			ret.add(e.getValue().executeFederatedOperation(fedReq));
			pos++;
		}

		// prepare results (future federated responses), with optional wait to ensure the
		// order of requests without data dependencies (e.g., cleanup RPCs)
		if(wait)
			FederationUtils.waitFor(ret);
		return ret.toArray(new Future[0]);
	}

	public List<Pair<FederatedRange, Future<FederatedResponse>>> requestFederatedData() {
		if(!isInitialized())
			throw new DMLRuntimeException("Federated matrix read only supported on initialized FederatedData");

		List<Pair<FederatedRange, Future<FederatedResponse>>> readResponses = new ArrayList<>();
		FederatedRequest request = new FederatedRequest(RequestType.GET_VAR, _ID);
		for(Pair<FederatedRange, FederatedData> e : _fedMap)
			readResponses.add(Pair.of(e.getKey(), e.getValue().executeFederatedOperation(request)));
		return readResponses;
	}

	public FederatedRequest cleanup(long tid, long... id) {
		FederatedRequest request = new FederatedRequest(RequestType.EXEC_INST, -1,
			VariableCPInstruction.prepareRemoveInstruction(id).toString());
		request.setTID(tid);
		return request;
	}

	public void execCleanup(long tid, long... id) {
		FederatedRequest request = new FederatedRequest(RequestType.EXEC_INST, -1,
			VariableCPInstruction.prepareRemoveInstruction(id).toString());
		request.setTID(tid);
		List<Future<FederatedResponse>> tmp = new ArrayList<>();
		for(Pair<FederatedRange, FederatedData> fd : _fedMap)
			tmp.add(fd.getValue().executeFederatedOperation(request));
		// This cleaning is allowed to go in a separate thread, and finish on its own.
		// The benefit is that the program is able to continue working on other things.
		// The downside is that at the end of execution these threads can have executed
		// for some extra time that can in particular be noticeable for shorter federated jobs.

		// To force the cleanup use waitFor -> drastically increasing execution time if
		// communication is slow to federated sites.
		// FederationUtils.waitFor(tmp);
	}

	private static FederatedRequest[] addAll(FederatedRequest a, FederatedRequest[] b) {
		// empty b array
		if( b == null || b.length==0 ) {
			return new FederatedRequest[] {a};
		}
		// concat with b array
		else {
			FederatedRequest[] ret = new FederatedRequest[b.length + 1];
			ret[0] = a;
			System.arraycopy(b, 0, ret, 1, b.length);
			return ret;
		}
	}

	public FederationMap identCopy(long tid, long id) {
		Future<FederatedResponse>[] copyInstr = execute(tid,
			new FederatedRequest(RequestType.EXEC_INST, _ID,
				VariableCPInstruction.prepareCopyInstruction(Long.toString(_ID), Long.toString(id)).toString()));
		for(Future<FederatedResponse> future : copyInstr) {
			try {
				FederatedResponse response = future.get();
				if(!response.isSuccessful())
					response.throwExceptionFromResponse();
			}
			catch(Exception e) {
				throw new DMLRuntimeException(e);
			}
		}
		FederationMap copyFederationMap = copyWithNewID(id);
		copyFederationMap._type = _type;
		return copyFederationMap;
	}

	/**
	 * Copy the federation map with the next available federated ID as reference to the federated data.
	 * This means that the federated map refers to the next federated data object on the workers.
	 * @return copied federation map with next federated ID
	 */
	public FederationMap copyWithNewID() {
		return copyWithNewID(FederationUtils.getNextFedDataID());
	}

	/**
	 * Copy the federation map with the given ID as reference to the federated data.
	 * This means that the federated map refers to the federated data object on the workers with the given ID.
	 * @param id federated data object ID
	 * @return copied federation map with given federated ID
	 */
	public FederationMap copyWithNewID(long id) {
		List<Pair<FederatedRange, FederatedData>> map = new ArrayList<>();
		// TODO handling of file path, but no danger as never written
		for(Entry<FederatedRange, FederatedData> e : _fedMap) {
			if(e.getKey().getSize() != 0)
				map.add(Pair.of(new FederatedRange(e.getKey()), e.getValue().copyWithNewID(id)));
		}
		return new FederationMap(id, map, _type);
	}

	/**
	 * Copy the federation map with the given ID as reference to the federated data
	 * and with given clen as end dimension for the columns in the range.
	 * This means that the federated map refers to the federated data object on the workers with the given ID.
	 * @param id federated data object ID
	 * @param clen column length of data objects on federated workers
	 * @return copied federation map with given federated ID and ranges adapted according to clen
	 */
	public FederationMap copyWithNewID(long id, long clen) {
		List<Pair<FederatedRange, FederatedData>> map = new ArrayList<>();
		// TODO handling of file path, but no danger as never written
		for(Pair<FederatedRange, FederatedData> e : _fedMap)
			map.add(Pair.of(new FederatedRange(e.getKey(), clen), e.getValue().copyWithNewID(id)));
		return new FederationMap(id, map, _type);
	}

	/**
	 * Copy federated mapping while giving the federated data new IDs
	 * and setting the ranges from zero to row and column ends specified.
	 * The overlapping ranges are given an overlap number to separate the ranges when putting to the federated map.
	 * The federation map returned is of type FType.PART.
	 * @param rowRangeEnd end of range for the rows
	 * @param colRangeEnd end of range for the columns
	 * @param outputID ID given to the output
	 * @return new federation map with overlapping ranges with partially aggregated values
	 */
	public FederationMap copyWithNewIDAndRange(long rowRangeEnd, long colRangeEnd, long outputID){
		List<Pair<FederatedRange, FederatedData>> outputMap = new ArrayList<>();
		for(Pair<FederatedRange, FederatedData> e : _fedMap) {
			if(e.getKey().getSize() != 0)
				outputMap.add(Pair.of(
					new FederatedRange(new long[]{0,0}, new long[]{rowRangeEnd, colRangeEnd}),
					e.getValue().copyWithNewID(outputID)));
		}
		return new FederationMap(outputID, outputMap, FType.PART);
	}

	public FederationMap bind(long rOffset, long cOffset, FederationMap that) {
		for(Entry<FederatedRange, FederatedData> e : that._fedMap) {
			_fedMap.add(Pair.of(new FederatedRange(e.getKey()).shift(rOffset, cOffset), e.getValue().copyWithNewID(_ID)));
		}
		return this;
	}

	public FederationMap transpose() {
		List<Pair<FederatedRange, FederatedData>> tmp = new ArrayList<>(_fedMap);
		_fedMap.clear();
		for(Pair<FederatedRange, FederatedData> e : tmp) {
			_fedMap.add(Pair.of(new FederatedRange(e.getKey()).transpose(), e.getValue().copyWithNewID(_ID)));
		}
		// derive output type
		switch(_type) {
			case FULL:
				_type = FType.FULL;
				break;
			case ROW:
				_type = FType.COL;
				break;
			case COL:
				_type = FType.ROW;
				break;
			case PART:
				_type = FType.PART;
			default:
				_type = FType.OTHER;
		}
		return this;
	}

	public long getMaxIndexInRange(int dim) {
		return _fedMap.stream().mapToLong(range -> range.getKey().getEndDims()[dim]).max().orElse(-1L);
	}

	/**
	 * Execute a function for each <code>FederatedRange</code> + <code>FederatedData</code> pair. The function should
	 * not change any data of the pair and instead use <code>mapParallel</code> if that is a necessity. Note that this
	 * operation is parallel and necessary synchronisation has to be performed.
	 *
	 * @param forEachFunction function to execute for each pair
	 */
	public void forEachParallel(BiFunction<FederatedRange, FederatedData, Void> forEachFunction) {
		ExecutorService pool = CommonThreadPool.get(_fedMap.size());

		ArrayList<MappingTask> mappingTasks = new ArrayList<>();
		for(Pair<FederatedRange, FederatedData> fedMap : _fedMap)
			mappingTasks.add(new MappingTask(fedMap.getKey(), fedMap.getValue(), forEachFunction, _ID));
		CommonThreadPool.invokeAndShutdown(pool, mappingTasks);
	}

	/**
	 * Execute a function for each <code>FederatedRange</code> + <code>FederatedData</code> pair mapping the pairs to
	 * their new form by directly changing both <code>FederatedRange</code> and <code>FederatedData</code>. The varIDs
	 * don't have to be changed by the <code>mappingFunction</code> as that will be done by this method. Note that this
	 * operation is parallel and necessary synchronisation has to be performed.
	 *
	 * @param newVarID        the new varID to be used by the new FederationMap
	 * @param mappingFunction the function directly changing ranges and data
	 * @return the new <code>FederationMap</code>
	 */
	public FederationMap mapParallel(long newVarID, BiFunction<FederatedRange, FederatedData, Void> mappingFunction) {
		ExecutorService pool = CommonThreadPool.get(_fedMap.size());

		FederationMap fedMapCopy = copyWithNewID(_ID);
		ArrayList<MappingTask> mappingTasks = new ArrayList<>();
		for(Pair<FederatedRange, FederatedData> fedMap : fedMapCopy._fedMap)
			mappingTasks.add(new MappingTask(fedMap.getKey(), fedMap.getValue(), mappingFunction, newVarID));
		CommonThreadPool.invokeAndShutdown(pool, mappingTasks);
		fedMapCopy._ID = newVarID;
		return fedMapCopy;
	}

	public FederationMap filter(IndexRange ixrange) {
		FederationMap ret = this.clone(); // same ID

		Iterator<Pair<FederatedRange, FederatedData>> iter = ret._fedMap.iterator();
		while(iter.hasNext()) {
			Entry<FederatedRange, FederatedData> e = iter.next();
			FederatedRange range = e.getKey();
			long rs = range.getBeginDims()[0], re = range.getEndDims()[0], cs = range.getBeginDims()[1],
				ce = range.getEndDims()[1];
			boolean overlap = ((ixrange.colStart <= ce) && (ixrange.colEnd >= cs) && (ixrange.rowStart <= re) &&
				(ixrange.rowEnd >= rs));
			if(!overlap)
				iter.remove();
		}
		return ret;
	}

	private static void setThreadID(long tid, FederatedRequest[]... frsets) {
		for(FederatedRequest[] frset : frsets)
			if(frset != null)
				Arrays.stream(frset).forEach(fr -> fr.setTID(tid));
	}

	public void reverseFedMap() {
		// TODO perf
		// TODO: add a check if the map is sorted based on indexes before reversing.
		// TODO: add a setup such that on construction the federated map is already sorted.
		FederatedRange[] fedRanges = getFederatedRanges();

		for(int i = 0; i < Math.floor(fedRanges.length / 2.0); i++) {
			FederatedData data1 = getFederatedData(fedRanges[i]);
			FederatedData data2 = getFederatedData(fedRanges[fedRanges.length-1-i]);

			removeFederatedData(fedRanges[i]);
			removeFederatedData(fedRanges[fedRanges.length-1-i]);

			_fedMap.add(Pair.of(fedRanges[i], data2));
			_fedMap.add(Pair.of(fedRanges[fedRanges.length-1-i], data1));
		}
	}

	private static class MappingTask implements Callable<Void> {
		private final FederatedRange _range;
		private final FederatedData _data;
		private final BiFunction<FederatedRange, FederatedData, Void> _mappingFunction;
		private final long _varID;

		public MappingTask(FederatedRange range, FederatedData data,
			BiFunction<FederatedRange, FederatedData, Void> mappingFunction, long varID) {
			_range = range;
			_data = data;
			_mappingFunction = mappingFunction;
			_varID = varID;
		}

		@Override
		public Void call() throws Exception {
			_mappingFunction.apply(_range, _data);
			_data.setVarID(_varID);
			return null;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Fed Map: " + _type);
		sb.append("\t ID:" + _ID);
		sb.append("\n" + _fedMap);
		return sb.toString();
	}

	@Override
	public FederationMap clone() {
		return copyWithNewID(getID());
	}
}
