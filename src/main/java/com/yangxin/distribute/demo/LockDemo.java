package com.yangxin.distribute.demo;

import com.yangxin.distribute.lock.DistributeLock;
import com.yangxin.distribute.lock.LockListener;
import com.yangxin.distribute.zkmethod.MainZooKeeper;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author leon on 2018/7/29.
 * @version 1.0
 * @Description:
 */
public class LockDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockDemo.class);

    public static void main(String[] args) {
        ZooKeeper zooKeeper = new MainZooKeeper().connectServer();
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
}
