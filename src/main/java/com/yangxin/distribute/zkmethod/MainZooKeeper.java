package com.yangxin.distribute.zkmethod;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author leon on 2018/7/31.
 * @version 1.0
 * @Description:
 */
public class MainZooKeeper implements ZooKeeperMethod {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainZooKeeper.class);
    private CountDownLatch latch;
    private ZooKeeper zk;
    public MainZooKeeper(){
        latch = new CountDownLatch(1);
    }
    @Override
    public ZooKeeper connectServer() {

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
