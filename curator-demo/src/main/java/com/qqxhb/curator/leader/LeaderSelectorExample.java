package com.qqxhb.curator.leader;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class LeaderSelectorExample {
	private static final Logger LOG = LoggerFactory.getLogger(LeaderSelectorListenerAdapter.class);
	// 参与者数量
	private static final int PARTITIONS = 5;
	// 领导者选举节点路径
	private static final String PATH = "/qqxhb/leader";
	// Zookeeper 服务器地址
	private static final String ZK_SERVER = "127.0.0.1:2181";

	public static void main(String[] args) throws Exception {

		List<CuratorFramework> clients = Lists.newArrayList();
		List<LeaderClient> leaderClients = Lists.newArrayList();
		try {
			for (int i = 0; i < PARTITIONS; ++i) {
				CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_SERVER,
						new ExponentialBackoffRetry(2000, 5));
				clients.add(client);
				LeaderClient leaderClient = new LeaderClient(client, PATH, "Client #" + i);
				leaderClients.add(leaderClient);

				client.start();
				leaderClient.start();
			}

			LOG.info("====按回车键结束领导者选举过程。。。。。。。。。");
			new BufferedReader(new InputStreamReader(System.in)).readLine();
		} finally {
			LOG.info("====领导者选举过程结束。。。。。。。。。");

			for (LeaderClient exampleClient : leaderClients) {
				CloseableUtils.closeQuietly(exampleClient);
			}
			for (CuratorFramework client : clients) {
				CloseableUtils.closeQuietly(client);
			}

		}
	}
}
