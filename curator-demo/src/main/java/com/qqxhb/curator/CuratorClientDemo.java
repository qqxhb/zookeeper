package com.qqxhb.curator;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Curator 客户端基本操作
 *
 */
public class CuratorClientDemo {
	private static final Logger LOG = LoggerFactory.getLogger(CuratorClientDemo.class);
	// Zookeeper 服务器地址
	private static final String ZK_SERVER = "127.0.0.1:2181";

	private CuratorFramework client;

	public CuratorClientDemo() {
		// 初始化客户端并启动
		this.client = CuratorFrameworkFactory.newClient(ZK_SERVER, new RetryNTimes(5, 6000));
		client.start();
		LOG.info("============curator client started=====");
	}

	/**
	 * 创建新的节点
	 * 
	 * @param path 节点路径
	 * @param data 节点数据
	 * @return 成功与否
	 */
	public boolean createNode(String path, String data) {
		try {
			this.client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("==============创建节点 {} 失败=======", path);
			return false;
		}
	}

	/**
	 * 获取节点数据
	 * 
	 * @param path 节点路径
	 * @return 节点数据
	 */
	public String getData(String path) {
		try {
			byte[] data = this.client.getData().forPath(path);
			return new String(data);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("==============获取节点 {} 数据失败=======", path);
		}
		return null;
	}

	/**
	 * 获取子节点
	 * 
	 * @param path 父节点
	 * @return 子节点路径集合
	 */
	public List<String> getChildren(String path) {
		try {
			return this.client.getChildren().forPath(path);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("==============获取节点 {} 子节点失败=======", path);
		}
		return null;
	}

	/**
	 * 修改节点
	 * 
	 * @param path 节点路径
	 * @param data 节点数据
	 * @return 成功与否
	 */
	public boolean modifyNode(String path, String data) {
		try {
			this.client.setData().forPath(path, data.getBytes());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("==============修改节点 {} 失败=======", path);
			return false;
		}
	}

	/**
	 * 删除节点
	 * 
	 * @param path 节点路径
	 * @return 成功与否
	 */
	public boolean deleteNode(String path) {
		try {
			this.client.delete().forPath(path);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("==============修改节点 {} 失败=======", path);
			return false;
		}
	}

	/**
	 * 关闭连接
	 */
	public void closeConnection() {
		this.client.close();
		LOG.info("============curator client closed=====");
	}

	public static void main(String[] args) throws Exception {
		CuratorClientDemo client = new CuratorClientDemo();
		String path = "/qqxhb";
		client.createNode(path, "test");
		String data = client.getData(path);
		System.out.println(data);
		List<String> children = client.getChildren("/");
		children.forEach(System.out::println);
		client.modifyNode(path, "zookeeper");
		client.deleteNode(path);
		client.closeConnection();
	}

}
