package com.qqxhb.curator.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 模拟任务锁
 * 
 */
public class TaskLocks {
	private static final Logger LOG = LoggerFactory.getLogger(TaskLocks.class);
	private final InterProcessMutex lock;
	private final String taskName;
	private final String clientName;

	public TaskLocks(CuratorFramework client, String lockPath, String taskName, String clientName) {
		this.taskName = taskName;
		this.clientName = clientName;
		lock = new InterProcessMutex(client, lockPath);
	}

	public void doWork(long time, TimeUnit unit) throws Exception {
		if (!lock.acquire(time, unit)) {
			throw new IllegalStateException(this.clientName + " could not acquire the lock");
		}
		try {
			LOG.info("======= {} 获取到锁，开始执行任务{}。。。。", this.clientName, this.taskName);
			Thread.sleep((long) (3 * Math.random()));
		} finally {
			LOG.info("======= {}释放任务锁{}。。。。", this.clientName, this.taskName);
			lock.release();
		}
	}
}
