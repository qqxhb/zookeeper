/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * Server configuration storage.
 *
 * We use this instead of Properties as it's typed.
 *
 */
@InterfaceAudience.Public
public class ServerConfig {
	// 客户端连接端口
	protected InetSocketAddress clientPortAddress;
	// 客户端安全连接端口
	protected InetSocketAddress secureClientPortAddress;
	// 数据路径
	protected File dataDir;
	// 日志数据路径
	protected File dataLogDir;
	//# 这个时间是作为 Zookeeper心跳时间单位（如5表示5毫秒，若心跳间隔配置的是10，则表示5*10=50毫秒同步一次心跳）。
	protected int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;//默认3000
	/*
	 * 单个客户端与单台服务器之间的连接数的限制，是ip级别的，默认是60，如果设置为0，那么表明不作任何限制。
	 * 请注意这个限制的使用范围，仅仅是单台客户端机器与单台ZK服务器之间的连接数限制，不是针对指定客户端IP，也不是ZK集群的连接数限制，
	 * 也不是单台ZK对所有客户端的连接数限制。
	 */
	protected int maxClientCnxns;
	/** 最小会话超时时间，默认-1表示永久 */
	protected int minSessionTimeout = -1;
	/** 最大会话超时时间*/
	protected int maxSessionTimeout = -1;

	/**
	 * Parse arguments for server configuration
	 * 
	 * @param args clientPort dataDir and optional tickTime and maxClientCnxns
	 * @return ServerConfig configured wrt arguments
	 * @throws IllegalArgumentException on invalid usage
	 */
	public void parse(String[] args) {
		if (args.length < 2 || args.length > 4) {
			throw new IllegalArgumentException("Invalid number of arguments:" + Arrays.toString(args));
		}

		clientPortAddress = new InetSocketAddress(Integer.parseInt(args[0]));
		dataDir = new File(args[1]);
		dataLogDir = dataDir;
		if (args.length >= 3) {
			tickTime = Integer.parseInt(args[2]);
		}
		if (args.length == 4) {
			maxClientCnxns = Integer.parseInt(args[3]);
		}
	}

	/**
	 * Parse a ZooKeeper configuration file
	 * 
	 * @param path the patch of the configuration file
	 * @return ServerConfig configured wrt arguments
	 * @throws ConfigException error processing configuration
	 */
	public void parse(String path) throws ConfigException {
		QuorumPeerConfig config = new QuorumPeerConfig();
		config.parse(path);

		// let qpconfig parse the file and then pull the stuff we are
		// interested in
		readFrom(config);
	}

	/**
	 * Read attributes from a QuorumPeerConfig.
	 * 
	 * @param config
	 */
	public void readFrom(QuorumPeerConfig config) {
		clientPortAddress = config.getClientPortAddress();
		secureClientPortAddress = config.getSecureClientPortAddress();
		dataDir = config.getDataDir();
		dataLogDir = config.getDataLogDir();
		tickTime = config.getTickTime();
		maxClientCnxns = config.getMaxClientCnxns();
		minSessionTimeout = config.getMinSessionTimeout();
		maxSessionTimeout = config.getMaxSessionTimeout();
	}

	public InetSocketAddress getClientPortAddress() {
		return clientPortAddress;
	}

	public InetSocketAddress getSecureClientPortAddress() {
		return secureClientPortAddress;
	}

	public File getDataDir() {
		return dataDir;
	}

	public File getDataLogDir() {
		return dataLogDir;
	}

	public int getTickTime() {
		return tickTime;
	}

	public int getMaxClientCnxns() {
		return maxClientCnxns;
	}

	/** minimum session timeout in milliseconds, -1 if unset */
	public int getMinSessionTimeout() {
		return minSessionTimeout;
	}

	/** maximum session timeout in milliseconds, -1 if unset */
	public int getMaxSessionTimeout() {
		return maxSessionTimeout;
	}
}
