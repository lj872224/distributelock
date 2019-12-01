package com.xp.zookeeper;
/**
 * 分布式锁实现
 * @author liuj
 * @date 2019-12-01
 */

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

public class DistributeLock implements Lock{
	
	private String lockPath;

	private ZkClient zkClient;
	
	private ThreadLocal<String> currentPath = new ThreadLocal<>();
	
	private ThreadLocal<String> beforePath = new ThreadLocal<>();
	
	private ThreadLocal<Integer> reentCount = new ThreadLocal<>();
	
	public DistributeLock(String lockPath){
		super();
		this.lockPath = lockPath;
		zkClient = new ZkClient("localhost:2181");
		zkClient.setZkSerializer(new CustomeZkSerializer());
		if(!this.zkClient.exists(lockPath)){
			try{
				this.zkClient.createPersistent(lockPath);
			}catch(Exception e){
				
			}
		}
		
	}

	@Override
	public void lock() {
		if (!tryLock()) {
			waitForLock();
			lock();
		}
	}
	
	private void waitForLock(){
		CountDownLatch latch = new CountDownLatch(1);
		IZkDataListener listener = new IZkDataListener() {
			@Override
			public void handleDataDeleted(String arg0) throws Exception {
				System.out.println("监听节点被删除");
				latch.countDown();
			}
			@Override
			public void handleDataChange(String arg0, Object arg1) throws Exception {
				
			}
		};
		//注册当前节点上一个节点事件
		this.zkClient.subscribeDataChanges(this.beforePath.get(), listener);
		
		//当前一个节点还存在的情况阻塞自己
		if(this.zkClient.exists(this.beforePath.get())){
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//取消前一个节点的注册监听事件
		this.zkClient.unsubscribeDataChanges(this.beforePath.get(), listener);
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		
	}

	@Override
	public boolean tryLock() {
		//可重入锁逻辑实现
		if(this.reentCount.get() != null){
			//当前线程不是第一获取锁
			int reentCount = this.reentCount.get();
			if(reentCount > 0){
				this.reentCount.set(++reentCount);
				return true;
			}
		}
		if(this.currentPath.get() == null){
			currentPath.set(this.zkClient.createPersistentSequential(lockPath + "/","bbb"));
		}
		//获取当前节点下面所有的子节点
		List<String> childList = zkClient.getChildren(lockPath);
		Collections.sort(childList);
		//判断当前节点是否最小的节点
		if(currentPath.get().equals(lockPath + "/" + childList.get(0))){
			return true;
		}else {
			int currentIndex = childList.indexOf(currentPath.get().substring(lockPath.length() + 1));
			beforePath.set(lockPath + "/" + childList.get(currentIndex - 1));
		}
		return false;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		return false;
	}

	@Override
	public void unlock() {
		if(this.reentCount.get() != null){
			int reentCount = this.reentCount.get();
			if(reentCount > 1){
				this.reentCount.set(--reentCount);
			}else {
				this.reentCount.set(null);
			}
		}
		this.zkClient.delete(this.currentPath.get());
		
	}

	@Override
	public Condition newCondition() {
		return null;
	}
	
	public static void main(String[] args) {
		// 并发数
		int currency = 50;

		// 循环屏障
		CyclicBarrier cb = new CyclicBarrier(currency);

		// 多线程模拟高并发
		for (int i = 0; i < currency; i++) {
			new Thread(new Runnable() {
				public void run() {
					System.out.println(Thread.currentThread().getName() + "---------所有线程准备好---------------");
					// 等待一起出发
					try {
						cb.await();
					} catch (InterruptedException | BrokenBarrierException e) {
						e.printStackTrace();
					}
					DistributeLock lock = new DistributeLock("/distLock");

					try {
						lock.lock();
						System.out.println(Thread.currentThread().getName() + " 获得锁！");
					} finally {
						lock.unlock();
					}
				}
			}).start();

		}
	}
	
	

}
