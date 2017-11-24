package cn.sky.zookeepertest;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
/**
 * 对原生zookeeper的api进行简单封装
 * @author lenovo
 *
 */
public class MyZKClient {
	private String connectString = "127.0.0.1:2181";
	private int sessionTimeout_ms;
	private CountDownLatch cdl = new CountDownLatch(1);
	private ZooKeeper zkCli;
	
	private MyZKClient(){}
	
	public void start() {
		try {
			zkCli = new ZooKeeper(connectString, sessionTimeout_ms, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					KeeperState state = event.getState();
					if(state.equals(KeeperState.SyncConnected)) {
						System.out.println("zookeeper client SyncConnected");
						cdl.countDown();
					}
				}
			});
			cdl.await();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void close(){
		if(zkCli!=null) {
			try {
				zkCli.close();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public static MyZKClient newClient(String connectString, int sessionTimeout_ms) {
		MyZKClient client = new MyZKClient();
		client.connectString = connectString;
		client.sessionTimeout_ms = sessionTimeout_ms;
		return client;
	}
	
	public String createPathWithMode(String path,byte[] data,CreateMode createMode) throws Exception {
		if(data==null) {
			data = new byte[0];
		}
		return zkCli.create(path, data, Ids.OPEN_ACL_UNSAFE, createMode);
	}
	
	public void deleteNode(String path) {
		try {
			zkCli.delete(path, -1);
		} catch (InterruptedException | KeeperException e) {
			throw new RuntimeException(e);
		}
	}
	
	public boolean existsNode(String path) throws Exception {
		return zkCli.exists(path, false)!=null;
	}
	
	public List<String> getChildren(String parentPath) throws Exception{
		return zkCli.getChildren(parentPath, null);
	}
	
	/**
	 * 如果注册一次，则只会监听一次
	 * @param path
	 * @param watcher
	 * @throws Exception
	 */
	public void watch(String path,Watcher watcher) throws Exception{
		if(zkCli.exists(path, watcher)==null){
			throw new RuntimeException(new NoNodeException(path));
		}
	}
}
