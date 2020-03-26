package com.qqxhb.curator.leader;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 领导者选举客户端
 */
public class LeaderClient extends LeaderSelectorListenerAdapter implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(LeaderSelectorListenerAdapter.class);

	private final String name;
	private final LeaderSelector leaderSelector;
	private final AtomicInteger leaderCount = new AtomicInteger();

	public LeaderClient(CuratorFramework client, String path, String name) {
		this.name = name;
		// 根据路径创建领导者选举实例，所有参与领导者选举的都必须使用同一个路径
		leaderSelector = new LeaderSelector(client, path, this);

		// 放弃领导者之后重新参与领导者选举
		leaderSelector.autoRequeue();
	}

	public void start() throws IOException {
		// 启动领导者选举，并在后台运行
		leaderSelector.start();
	}

	@Override
	public void close() throws IOException {
		leaderSelector.close();
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		// 同一时刻，只有一个Listener会进入takeLeadership()方法，说明它是当前的Leader。
		// 注意：当Listener从takeLeadership()退出时就说明它放弃了“Leader身份”,下面是使用睡眠时间模拟持有领导者的时间

		final int waitSeconds = (int) (5 * Math.random()) + 1;

		LOG.info("当前领导者是：{},已经第 {} 次当选，占据领导者时间 {} 秒后放弃领导者身份。。。。。 ", this.name, leaderCount.getAndIncrement(),
				waitSeconds);
		try {
			Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {

			LOG.info("======= {} 放弃领导权。。。。。", this.name);
		}
	}
}
