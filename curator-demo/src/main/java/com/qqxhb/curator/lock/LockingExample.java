package com.qqxhb.curator.lock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

public class LockingExample {

	private static final int CLIENTS = 5;
	// 锁节点路径
	private static final String PATH = "/qqxhb/locks";
	// Zookeeper 服务器地址
	private static final String ZK_SERVER = "127.0.0.1:2181";

	public static void main(String[] args) throws Exception {

		ExecutorService service = Executors.newFixedThreadPool(CLIENTS);
		try {
			String task = "task_test";
			for (int i = 0; i < CLIENTS; ++i) {
				int cli = i;
				service.execute(() -> {
					CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_SERVER,
							new ExponentialBackoffRetry(2000, 5));
					try {
						client.start();
						TaskLocks example = new TaskLocks(client, PATH, task, "Client #" + cli);
						example.doWork(10, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						CloseableUtils.closeQuietly(client);
					}
				});
			}

			service.shutdown();
			service.awaitTermination(10, TimeUnit.MINUTES);
		} finally {
			service.shutdown();
		}
	}
}
