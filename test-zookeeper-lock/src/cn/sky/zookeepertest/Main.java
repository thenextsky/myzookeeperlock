package cn.sky.zookeepertest;

import java.util.concurrent.TimeUnit;
/**
 * 测试类
 * @author lenovo
 *
 */
public class Main {
	private static String basePath = "/mylock";
	private static String connectString = "127.0.0.1:2181";
	private static MyZKClient client = MyZKClient.newClient(connectString, 1000);
	
	public static void main(String[] args) throws Exception {
		client.start();
		for(int i=1;i<=3;i++) {
			new Thread(new MyRunnable(i)).start();
		}
		Thread.sleep(9999999);
	}
	
	
	private static class MyRunnable implements Runnable{
		private int i;
		private MyRunnable(int i){
			this.i = i;
		}
		@Override
		public void run() {
			MyZKLock lock = new MyZKLock(basePath, client);
			try {
				if(!lock.require(3,TimeUnit.SECONDS)) {
					System.out.println("获取锁"+i+"失败");
					return;
				}
				{
					System.out.println("begin"+i);
					Thread.sleep(2900);
					System.out.println("end"+i);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
				if(lock.isRequired()) {
					lock.release();
				}
			}
		}
	}
}

