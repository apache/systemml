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

import java.security.cert.CertificateException;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.SSLException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Logger;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.conf.DMLConfig;

public class FederatedWorker {
	protected static Logger log = Logger.getLogger(FederatedWorker.class);

	private int _port;
	private final ExecutionContextMap _ecm;
	protected Set<Triple<Long, FederationMap.FType, Long>> _broadcastSet = new HashSet<>();

	public FederatedWorker(int port) {
		_ecm = new ExecutionContextMap();
		_port = (port == -1) ? DMLConfig.DEFAULT_FEDERATED_PORT : port;
	}

	public void run() throws CertificateException, SSLException {
		log.info("Setting up Federated Worker");
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(1);
		ServerBootstrap b = new ServerBootstrap();
		// TODO add ability to use real ssl files, not self signed certificates.
		SelfSignedCertificate cert = new SelfSignedCertificate();
		final SslContext cont2 = SslContextBuilder.forServer(cert.certificate(), cert.privateKey()).build();

		FederatedWorker federatedWorker = this;

		try {
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) {
						ChannelPipeline cp = ch.pipeline();

						if(ConfigurationManager.getDMLConfig()
							.getBooleanValue(DMLConfig.USE_SSL_FEDERATED_COMMUNICATION)) {
							cp.addLast(cont2.newHandler(ch.alloc()));
						}
						cp.addLast("ObjectDecoder",
							new ObjectDecoder(Integer.MAX_VALUE,
								ClassResolvers.weakCachingResolver(ClassLoader.getSystemClassLoader())));
						cp.addLast("ObjectEncoder", new ObjectEncoder());
						cp.addLast("FederatedWorkerHandler", new FederatedWorkerHandler(_ecm, federatedWorker));
					}
				}).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);
			log.info("Starting Federated Worker server at port: " + _port);
			ChannelFuture f = b.bind(_port).sync();
			log.info("Started Federated Worker at port: " + _port);
			f.channel().closeFuture().sync();
		}
		catch(Exception e) {
			log.info("Federated worker interrupted");
		}
		finally {
			log.info("Federated Worker Shutting down.");
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}
}
