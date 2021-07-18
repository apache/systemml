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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import javax.net.ssl.SSLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest.RequestType;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Promise;
import org.apache.sysds.runtime.meta.MetaData;
import org.apache.sysds.runtime.meta.MetaDataAll;

public class FederatedData {
	private static final Log LOG = LogFactory.getLog(FederatedData.class.getName());
	private static final Set<InetSocketAddress> _allFedSites = new HashSet<>();

	/** A Singleton constructed SSL context, that only is assigned if ssl is enabled. */
	private static SslContextMan instance = null;

	private final Types.DataType _dataType;
	private final InetSocketAddress _address;
	private final String _filepath;
	private Types.ReplicationType _replicationType; // replica after broadcast

	/**
	 * The ID of default matrix/tensor on which operations get executed if no other ID is given.
	 */
	private long _varID = -1; // -1 is never valid since varIDs start at 0

	public FederatedData(Types.DataType dataType, InetSocketAddress address, String filepath) {
		_dataType = dataType;
		_address = address;
		_filepath = filepath;
		if(_address != null)
			_allFedSites.add(_address);
		_replicationType = null;
	}

	public FederatedData(Types.DataType dataType, InetSocketAddress address, String filepath, long varID) {
		_dataType = dataType;
		_address = address;
		_filepath = filepath;
		_varID = varID;
		_replicationType = null;
	}

	public FederatedData(Types.DataType dataType, InetSocketAddress address, String filepath, Types.ReplicationType replicationType) {
		this(dataType, address, filepath);
		_replicationType = replicationType;
	}

	public FederatedData(Types.DataType dataType, InetSocketAddress address, String filepath, long varID, Types.ReplicationType replicationType) {
		this(dataType, address, filepath, varID);
		_replicationType = replicationType;
	}

	public InetSocketAddress getAddress() {
		return _address;
	}

	public void setVarID(long varID) {
		_varID = varID;
	}

	public long getVarID() {
		return _varID;
	}

	public String getFilepath() {
		return _filepath;
	}

	public Types.DataType getDataType() {
		return _dataType;
	}

	public Types.ReplicationType getReplicationType() { return _replicationType; }

	public boolean isInitialized() {
		return _varID != -1;
	}

	boolean equalAddress(FederatedData that) {
		return _address != null && that != null && that._address != null && _address.equals(that._address);
	}

	/**
	 * Make a copy of the <code>FederatedData</code> metadata, but use another varID (refer to another object on worker)
	 * 
	 * @param varID the varID of the variable we refer to
	 * @return new <code>FederatedData</code> with different varID set
	 */
	public FederatedData copyWithNewID(long varID) {
		FederatedData copy = new FederatedData(_dataType, _address, _filepath);
		copy.setVarID(varID);
		return copy;
	}

	public synchronized Future<FederatedResponse> initFederatedData(long id) {
		if(isInitialized())
			throw new DMLRuntimeException("Tried to init already initialized data");
		if(!_dataType.isMatrix() && !_dataType.isFrame())
			throw new DMLRuntimeException("Federated datatype \"" + _dataType.toString() + "\" is not supported.");
		_varID = id;
		FederatedRequest request = new FederatedRequest(RequestType.READ_VAR, id);
		request.appendParam(_filepath);
		request.appendParam(_dataType.name());
		return executeFederatedOperation(request);
	}

	public synchronized Future<FederatedResponse> initFederatedData(long id, MetaData mtd) {
		if(isInitialized())
			throw new DMLRuntimeException("Tried to init already initialized data");
		if(!_dataType.isMatrix() && !_dataType.isFrame())
			throw new DMLRuntimeException("Federated datatype \"" + _dataType.toString() + "\" is not supported.");
		_varID = id;
		FederatedRequest request = new FederatedRequest(RequestType.READ_VAR, id, mtd);
		request.appendParam(_filepath);
		request.appendParam(_dataType.name());
		return executeFederatedOperation(request);
	}

	public synchronized Future<FederatedResponse> executeFederatedOperation(FederatedRequest... request) {

		try {
			return executeFederatedOperation(_address, request);
		}
		catch(SSLException e) {
			throw new DMLRuntimeException("Error in SSL Connection", e);
		}
	}

	/**
	 * Executes an federated operation on a federated worker.
	 *
	 * @param address socket address (incl host and port)
	 * @param request the requested operation
	 * @return the response
	 * @throws SSLException Throws an SSL exception if the ssl construction fails.
	 */
	public static Future<FederatedResponse> executeFederatedOperation(InetSocketAddress address,
		FederatedRequest... request) throws SSLException {
		// Careful with the number of threads. Each thread opens connections to multiple files making resulting in
		// java.io.IOException: Too many open files
		EventLoopGroup workerGroup = new NioEventLoopGroup(DMLConfig.DEFAULT_NUMBER_OF_FEDERATED_WORKER_THREADS);

		try {
			Bootstrap b = new Bootstrap();
			final DataRequestHandler handler = new DataRequestHandler(workerGroup);
			// Client Netty
			b.group(workerGroup).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
					ChannelPipeline cp = ch.pipeline();
					if(ConfigurationManager.getDMLConfig().getBooleanValue(DMLConfig.USE_SSL_FEDERATED_COMMUNICATION)) {
						cp.addLast(SslConstructor().context
							.newHandler(ch.alloc(), address.getAddress().getHostAddress(), address.getPort()));
					}

					cp.addLast("ObjectDecoder",
						new ObjectDecoder(Integer.MAX_VALUE,
							ClassResolvers.weakCachingResolver(ClassLoader.getSystemClassLoader())));
					cp.addLast("FederatedOperationHandler", handler);
					cp.addLast("ObjectEncoder", new ObjectEncoder());

				}
			});

			ChannelFuture f = b.connect(address).sync();
			Promise<FederatedResponse> promise = f.channel().eventLoop().newPromise();

			handler.setPromise(promise);
			f.channel().writeAndFlush(request);
			return promise;
		}
		catch(InterruptedException e) {
			throw new DMLRuntimeException("Could not send federated operation.");
		}
		catch(Exception e) {
			throw new DMLRuntimeException(e);
		}
	}

	public static void clearFederatedWorkers() {
		if(_allFedSites.isEmpty())
			return;

		try {
			// create and execute clear request on all workers
			FederatedRequest fr = new FederatedRequest(RequestType.CLEAR);
			List<Future<FederatedResponse>> ret = new ArrayList<>();
			for(InetSocketAddress address : _allFedSites)
				ret.add(executeFederatedOperation(address, fr));

			// wait for successful completion
			FederationUtils.waitFor(ret);
		}
		catch(Exception ex) {
			LOG.warn("Failed to execute CLEAR request on existing federated sites.", ex);
		}
		finally {
			resetFederatedSites();
		}
	}

	public static void resetFederatedSites() {
		_allFedSites.clear();
	}

	private static class DataRequestHandler extends ChannelInboundHandlerAdapter {
		private Promise<FederatedResponse> _prom;
		private EventLoopGroup _workerGroup;

		public DataRequestHandler(EventLoopGroup workerGroup) {
			_workerGroup = workerGroup;
		}

		public void setPromise(Promise<FederatedResponse> prom) {
			_prom = prom;
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			if(_prom == null)
				throw new DMLRuntimeException("Read while no message was sent");
			_prom.setSuccess((FederatedResponse) msg);
			ctx.close();
			_workerGroup.shutdownGracefully();
		}
	}

	private static class SslContextMan {
		protected final SslContext context;

		private SslContextMan() {
			try {
				context = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
			}
			catch(SSLException e) {
				throw new DMLRuntimeException("Static SSL setup failed for client side", e);
			}
		}
	}

	private static SslContextMan SslConstructor() {
		if(instance == null) {
			return new SslContextMan();
		}
		else {
			return instance;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getSimpleName().toString());
		sb.append(" " + _dataType);
		sb.append(" " + _address.toString());
		sb.append(":" + _filepath);
		return sb.toString();
	}
}
