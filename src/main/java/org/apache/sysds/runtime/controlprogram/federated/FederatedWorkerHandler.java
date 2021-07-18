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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.parser.DataExpression;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.BasicProgramBlock;
import org.apache.sysds.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.caching.FrameObject;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse.ResponseType;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.instructions.InstructionParser;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.ListObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.lineage.LineageCache;
import org.apache.sysds.runtime.lineage.LineageCacheConfig;
import org.apache.sysds.runtime.lineage.LineageCacheConfig.ReuseCacheType;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.lineage.LineageItemUtils;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataAll;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.privacy.DMLPrivacyException;
import org.apache.sysds.runtime.privacy.PrivacyMonitor;
import org.apache.sysds.utils.Statistics;

public class FederatedWorkerHandler extends ChannelInboundHandlerAdapter {
	protected static Logger log = Logger.getLogger(FederatedWorkerHandler.class);

	private final ExecutionContextMap _ecm;
	private final FederatedWorker _federatedWorker;

	public FederatedWorkerHandler(ExecutionContextMap ecm) {
		this(ecm, null);
	}

	public FederatedWorkerHandler(ExecutionContextMap ecm, FederatedWorker federatedWorker) {
		// Note: federated worker handler created for every command;
		// and concurrent parfor threads at coordinator need separate
		// execution contexts at the federated sites too
		_ecm = ecm;
		_federatedWorker = federatedWorker;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		ctx.writeAndFlush(createResponse(msg)).addListener(new CloseListener());
	}

	public FederatedResponse createResponse(Object msg) {
		if(log.isDebugEnabled()) {
			log.debug("Received: " + msg.getClass().getSimpleName());
		}
		if(!(msg instanceof FederatedRequest[]))
			throw new DMLRuntimeException(
				"FederatedWorkerHandler: Received object no instance of 'FederatedRequest[]'.");
		FederatedRequest[] requests = (FederatedRequest[]) msg;
		FederatedResponse response = null; // last response

		for(int i = 0; i < requests.length; i++) {
			FederatedRequest request = requests[i];
			if(log.isDebugEnabled()) {
				log.debug("Executing command " + (i + 1) + "/" + requests.length + ": " + request.getType().name());
				if(log.isTraceEnabled()) {
					log.trace("full command: " + request.toString());
				}
			}
			PrivacyMonitor.setCheckPrivacy(request.checkPrivacy());
			PrivacyMonitor.clearCheckedConstraints();

			// execute command and handle privacy constraints
			FederatedResponse tmp = executeCommand(request);
			conditionalAddCheckedConstraints(request, tmp);

			// select the response for the entire batch of requests
			if(!tmp.isSuccessful()) {
				log.error("Command " + request.getType() + " failed: " + tmp.getErrorMessage() + "full command: \n"
					+ request.toString());
				response = (response == null || response.isSuccessful()) ? tmp : response; // return first error
			}
			else if(request.getType() == RequestType.GET_VAR) {
				if(response != null && response.isSuccessful())
					log.error("Multiple GET_VAR are not supported in single batch of requests.");
				response = tmp; // return last get result
			}
			else if(response == null && i == requests.length - 1) {
				response = tmp; // return last
			}

			if(DMLScript.STATISTICS && request.getType() == RequestType.CLEAR && Statistics.allowWorkerStatistics) {
				System.out.println("Federated Worker " + Statistics.display());
				Statistics.reset();
			}
		}
		return response;
	}

	private static void conditionalAddCheckedConstraints(FederatedRequest request, FederatedResponse response) {
		if(request.checkPrivacy())
			response.setCheckedConstraints(PrivacyMonitor.getCheckedConstraints());
	}

	private FederatedResponse executeCommand(FederatedRequest request) {
		RequestType method = request.getType();
		try {
			switch(method) {
				case READ_VAR:
					return readData(request); // matrix/frame
				case PUT_VAR:
					return putVariable(request);
				case GET_VAR:
					return getVariable(request);
				case EXEC_INST:
					return execInstruction(request);
				case EXEC_UDF:
					return execUDF(request);
				case CLEAR:
					return execClear();
				default:
					String message = String.format("Method %s is not supported.", method);
					return new FederatedResponse(ResponseType.ERROR, new FederatedWorkerHandlerException(message));
			}
		}
		catch(DMLPrivacyException | FederatedWorkerHandlerException ex) {
			return new FederatedResponse(ResponseType.ERROR, ex);
		}
		catch (Exception ex) {
			String msg = "Exception of type " + ex.getClass() + " thrown when processing request";
			log.error(msg, ex);
			return new FederatedResponse(ResponseType.ERROR,
				new FederatedWorkerHandlerException(msg));
		}
	}

	private FederatedResponse readData(FederatedRequest request) {
		checkNumParams(request.getNumParams(), 2);
		String filename = (String) request.getParam(0);
		DataType dt = DataType.valueOf((String) request.getParam(1));
		return readData(filename, dt, request.getID(), request.getTID());
	}

	private FederatedResponse readData(String filename, Types.DataType dataType, long id, long tid) {
		MatrixCharacteristics mc = new MatrixCharacteristics();
		mc.setBlocksize(ConfigurationManager.getBlocksize());
		CacheableData<?> cd;
		switch(dataType) {
			case MATRIX:
				cd = new MatrixObject(Types.ValueType.FP64, filename);
				break;
			case FRAME:
				cd = new FrameObject(filename);
				break;
			default:
				// should NEVER happen (if we keep request codes in sync with actual behavior)
				return new FederatedResponse(ResponseType.ERROR,
					new FederatedWorkerHandlerException("Could not recognize datatype"));
		}

		FileFormat fmt = null;
		boolean header = false;
		String delim = null;
		FileSystem fs = null;
		MetaDataAll mtd;
		try {
			String mtdname = DataExpression.getMTDFileName(filename);
			Path path = new Path(mtdname);
			fs = IOUtilFunctions.getFileSystem(mtdname);
			try(BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
				mtd = new MetaDataAll(br);
				if(!mtd.mtdExists())
					return new FederatedResponse(ResponseType.ERROR,
						new FederatedWorkerHandlerException("Could not parse metadata file"));
				mc.setRows(mtd.getDim1());
				mc.setCols(mtd.getDim2());
				mc.setNonZeros(mtd.getNnz());
				header = mtd.getHasHeader();
				cd = mtd.parseAndSetPrivacyConstraint(cd);
				fmt = mtd.getFileFormat();
				delim = mtd.getDelim();
			}
		}
		catch (DMLPrivacyException | FederatedWorkerHandlerException ex){
			throw ex;
		}
		catch (Exception ex) {
			String msg = "Exception in reading metadata of: " + filename;
			log.error(msg, ex);
			throw new DMLRuntimeException(msg);
		}
		finally {
			IOUtilFunctions.closeSilently(fs);
		}

		// put meta data object in symbol table, read on first operation
		cd.setMetaData(new MetaDataFormat(mc, fmt));
		if(fmt == FileFormat.CSV)
			cd.setFileFormatProperties(new FileFormatPropertiesCSV(header, delim,
				DataExpression.DEFAULT_DELIM_SPARSE));
		cd.enableCleanup(false); // guard against deletion
		_ecm.get(tid).setVariable(String.valueOf(id), cd);

		if (DMLScript.LINEAGE)
			// create a literal type lineage item with the file name
			_ecm.get(tid).getLineage().set(String.valueOf(id), new LineageItem(filename));

		if(dataType == Types.DataType.FRAME) {
			FrameObject frameObject = (FrameObject) cd;
			frameObject.acquireRead();
			frameObject.refreshMetaData(); // get block schema
			frameObject.release();
			return new FederatedResponse(ResponseType.SUCCESS, new Object[] {id, frameObject.getSchema(), mc});
		}
		return new FederatedResponse(ResponseType.SUCCESS, new Object[] {id, mc});
	}

	private FederatedResponse putVariable(FederatedRequest request) {
		checkNumParams(request.getNumParams(), 1, 2, 3, 5);
		String varname = String.valueOf(request.getID());
		ExecutionContext ec = _ecm.get(request.getTID());

		// check if broadcast already exists, otherwise put
		FederationMap.FType type;
		long dataID;
		if (request.getNumParams() == 2) {
			dataID = (long) request.getParam(0);
			type = (FederationMap.FType) request.getParam(1);
			if(_federatedWorker._broadcastSet.contains(Triple.of(Long.valueOf(varname), type, dataID))) {
				return new FederatedResponse(ResponseType.SUCCESS_EMPTY);
			}
		} else if (request.getNumParams() >= 3) {
			dataID = (long) request.getParam(1);
			type = (FederationMap.FType) request.getParam(2);
			if(_federatedWorker._broadcastSet.contains(Triple.of(Long.valueOf(varname), type, dataID))) {
				return new FederatedResponse(ResponseType.SUCCESS_EMPTY);
			}
			_federatedWorker._broadcastSet.add(Triple.of(Long.valueOf(varname), type, dataID));
			if(request.getNumParams() == 5) {
				long dataID2 = (long) request.getParam(3);
				long dataID3 = (long) request.getParam(4);
				_federatedWorker._broadcastSet.add(Triple.of(dataID2, type, dataID3));
			}
		}

		if(ec.containsVariable(varname)) {
			return new FederatedResponse(ResponseType.ERROR, "Variable " + request.getID() + " already existing.");
		}

		// wrap transferred cache block into cacheable data
		Data data;
		if(request.getParam(0) instanceof CacheBlock)
			data = ExecutionContext.createCacheableData((CacheBlock) request.getParam(0));
		else if(request.getParam(0) instanceof ScalarObject)
			data = (ScalarObject) request.getParam(0);
		else if(request.getParam(0) instanceof ListObject)
			data = (ListObject) request.getParam(0);
		else
			throw new DMLRuntimeException(
				"FederatedWorkerHandler: Unsupported object type, has to be of type CacheBlock or ScalarObject");

		// set variable and construct empty response
		ec.setVariable(varname, data);
		if (DMLScript.LINEAGE)
			ec.getLineage().set(varname, new LineageItem(String.valueOf(request.getChecksum(0))));

		return new FederatedResponse(ResponseType.SUCCESS_EMPTY);
	}

	private FederatedResponse getVariable(FederatedRequest request) {
		checkNumParams(request.getNumParams(), 0);
		ExecutionContext ec = _ecm.get(request.getTID());
		if(!ec.containsVariable(String.valueOf(request.getID()))) {
			return new FederatedResponse(ResponseType.ERROR,
				"Variable " + request.getID() + " does not exist at federated worker.");
		}
		// get variable and construct response
		Data dataObject = ec.getVariable(String.valueOf(request.getID()));
		dataObject = PrivacyMonitor.handlePrivacy(dataObject);
		switch(dataObject.getDataType()) {
			case TENSOR:
			case MATRIX:
			case FRAME:
				return new FederatedResponse(ResponseType.SUCCESS,
					((CacheableData<?>) dataObject).acquireReadAndRelease());
			case LIST:
				return new FederatedResponse(ResponseType.SUCCESS, ((ListObject) dataObject).getData());
			case SCALAR:
				return new FederatedResponse(ResponseType.SUCCESS, dataObject);
			default:
				return new FederatedResponse(ResponseType.ERROR, new FederatedWorkerHandlerException(
					"Unsupported return datatype " + dataObject.getDataType().name()));
		}
	}

	private FederatedResponse execInstruction(FederatedRequest request) {
		ExecutionContext ec = _ecm.get(request.getTID());
		BasicProgramBlock pb = new BasicProgramBlock(null);
		pb.getInstructions().clear();
		Instruction receivedInstruction = InstructionParser.parseSingleInstruction((String) request.getParam(0));
		pb.getInstructions().add(receivedInstruction);


		if(receivedInstruction.getOpcode().equals("rmvar")) {
			long id = Long.parseLong(InstructionUtils.getInstructionParts(receivedInstruction.getInstructionString())[1]);
			if( _federatedWorker == null || (_federatedWorker._broadcastSet
				.stream().anyMatch(e -> e.getLeft() == id)))
				return new FederatedResponse(ResponseType.SUCCESS_EMPTY);
		}


		if (DMLScript.LINEAGE)
			// Compiler assisted optimizations are not applicable for Fed workers.
			// e.g. isMarkedForCaching fails as output operands are saved in the 
			// symbol table only after the instruction execution finishes. 
			// NOTE: In shared JVM, this will disable compiler assistance even for the coordinator 
			LineageCacheConfig.setCompAssRW(false);

		try {
			pb.execute(ec); // execute single instruction
		}
		catch(DMLPrivacyException | FederatedWorkerHandlerException ex){
			throw ex;
		}
		catch(Exception ex) {
			String msg = "Exception of type " + ex.getClass() + " thrown when processing EXEC_INST request";
			log.error(msg, ex);
			return new FederatedResponse(ResponseType.ERROR,
				new FederatedWorkerHandlerException(msg));
		}
		return new FederatedResponse(ResponseType.SUCCESS_EMPTY);
	}

	private FederatedResponse execUDF(FederatedRequest request) {
		checkNumParams(request.getNumParams(), 1);
		ExecutionContext ec = _ecm.get(request.getTID());

		// get function and input parameters
		FederatedUDF udf = (FederatedUDF) request.getParam(0);
		Data[] inputs = Arrays.stream(udf.getInputIDs()).mapToObj(id -> ec.getVariable(String.valueOf(id)))
			.map(PrivacyMonitor::handlePrivacy).toArray(Data[]::new);
		
		// trace lineage
		if (DMLScript.LINEAGE)
			LineageItemUtils.traceFedUDF(ec, udf);
		
		// reuse or execute user-defined function
		try {
			// reuse UDF outputs if available in lineage cache
			FederatedResponse reuse = LineageCache.reuse(udf, ec);
			if (reuse.isSuccessful())
				return reuse;

			// else execute the UDF
			long t0 = !ReuseCacheType.isNone() ? System.nanoTime() : 0;
			FederatedResponse res = udf.execute(ec, inputs);
			long t1 = !ReuseCacheType.isNone() ? System.nanoTime() : 0;
			//cacheUDFOutputs(udf, inputs, t1-t0, ec);
			LineageCache.putValue(udf, ec, t1-t0);
			return res;
		}
		catch(DMLPrivacyException | FederatedWorkerHandlerException ex){
			throw ex;
		}
		catch(Exception ex) {
			String msg = "Exception of type " + ex.getClass() + " thrown when processing EXEC_UDF request";
			log.error(msg, ex);
			return new FederatedResponse(ResponseType.ERROR, new FederatedWorkerHandlerException(msg));
		}
	}
	
	private FederatedResponse execClear() {
		try {
			_ecm.clear();
		}
		catch(DMLPrivacyException | FederatedWorkerHandlerException ex){
			throw ex;
		}
		catch(Exception ex) {
			String msg = "Exception of type " + ex.getClass() + " thrown when processing CLEAR request";
			log.error(msg, ex);
			return new FederatedResponse(ResponseType.ERROR, new FederatedWorkerHandlerException(msg));
		}
		return new FederatedResponse(ResponseType.SUCCESS_EMPTY);
	}

	private static void checkNumParams(int actual, int... expected) {
		if(Arrays.stream(expected).anyMatch(x -> x == actual))
			return;
		throw new DMLRuntimeException("FederatedWorkerHandler: Received wrong amount of params:" 
			+ " expected=" + Arrays.toString(expected) + ", actual=" + actual);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

	private static class CloseListener implements ChannelFutureListener {
		@Override
		public void operationComplete(ChannelFuture channelFuture) throws InterruptedException {
			if(!channelFuture.isSuccess()) {
				log.error("Federated Worker Write failed");
				channelFuture.channel().writeAndFlush(new FederatedResponse(ResponseType.ERROR,
					new FederatedWorkerHandlerException("Error while sending response."))).channel().close().sync();
			}
			else {
				PrivacyMonitor.clearCheckedConstraints();
				channelFuture.channel().close().sync();
			}
		}
	}
}
