package com.yangxin.distribute.demo;

import com.yangxin.distribute.lock.DistributeLock;
import com.yangxin.distribute.lock.LockListener;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author leon on 2018/7/29.
 * @version 1.0
 * @Description:
 */
public class LockDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockDemo.class);
    private CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {
        ZooKeeper zooKeeper = new LockDemo().connectServer();
        DistributeLock lock = new DistributeLock(zooKeeper, "/demo", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                new LockListener() {
                    @Override
                    public void lockAcquired() {
                        LOGGER.info("acquire lock");
                    }

                    @Override
                    public void lockReased() {
                        LOGGER.info("rease lock");
                    }
                });
        try {
            lock.lock();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lock.unlock();
        LOGGER.info("end in 10s");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private  ZooKeeper connectServer() {
        ZooKeeper zk =null;
        try {
            zk = new ZooKeeper("192.168.0.167:2181", 5000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }catch (IOException | InterruptedException e){
            LOGGER.error("",e);
        }
        return zk;
    }
}
