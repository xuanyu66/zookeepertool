package com.yangxin.distribute.demo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yangxin.distribute.queue.DistributeQueue;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author leon on 2018/7/30.
 * @version 1.0
 * @Description:
 */
public class QueueDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueDemo.class);
    private static final Gson GSON = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
    private static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {
        ZooKeeper zooKeeper = connectServer();
        DistributeQueue queue = new DistributeQueue(zooKeeper, "/queue_demo", null);

    }

    private static byte[] invokeMethods(int field, String method, DistributeQueue queue){
        byte[] data = new byte[0];
        for (int i = 0; i < field; i++) {
            LOGGER.info("{}", i);
            Map<Long,String> map = new HashMap<>();
            map.put((long) i, Integer.toHexString(i));
            data = GSON.toJson(map).getBytes();
            try {
                Method metho = queue.getClass().getMethod(method);
                metho.invoke(queue, null);
            } catch (InterruptedException | KeeperException | NoSuchMethodException |
                    IllegalAccessException | InvocationTargetException e) {
                LOGGER.error("{}", e);
            }
        }
        return data;
    }

    private static ZooKeeper connectServer() {
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
