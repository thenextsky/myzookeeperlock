package cn.sky.zookeepertest;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
/**
 * 分布式锁的实现类
 * @author lenovo
 *
 */
public class MyZKLock {
	private String basePath;
	private static final String lockName = "mylock-";
	private MyZKClient client;
	private String myLockPath;
	private boolean required = false;
	
	public MyZKLock(String basePath,MyZKClient client) {
		this.basePath = basePath;
		this.client = client;
	}
	
	public boolean require(long timeout, TimeUnit unit) throws Exception {
		if(!client.existsNode(basePath)) {
			client.createPathWithMode(basePath, null, CreateMode.PERSISTENT);
		}
		myLockPath = client.createPathWithMode(basePath+"/"+lockName, null, CreateMode.EPHEMERAL_SEQUENTIAL);
		String prePath = null;
		while((prePath=getPreviousPath())!=null) {
			CountDownLatch cdl = new CountDownLatch(1);
			client.watch(prePath, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if(event.getType().equals(EventType.NodeDeleted)){
						cdl.countDown();
					}
				}
			});
			if(unit==null||timeout<0) {
				cdl.await();
			}else {
				if(!cdl.await(timeout, unit)) {
					client.deleteNode(myLockPath);
					required = false;
					return required;
				}
			}
		}
		required = true;
		return required;
	}
	
	public boolean require() throws Exception {
		return require(-1, null);
	}
	
	private String getPreviousPath() throws Exception {
		List<String> children = client.getChildren(basePath);
		if(children==null||children.size()==0) {
			throw new RuntimeException("get lock fail!");
		}
		Collections.sort(children);
		for(int i=0;i<children.size();i++) {
			String path = basePath+"/"+children.get(i);
			if(path.equals(myLockPath)) {
				if(i==0) {
					return null;
				}else {
					return basePath+"/"+children.get(i-1);
				}
			}
		}
		return null;
	}
	
	public void release() {
		if(!required) {
			throw new RuntimeException("the lock is not required!");
		}
		client.deleteNode(myLockPath);
	}

	public boolean isRequired() {
		return required;
	}

}
