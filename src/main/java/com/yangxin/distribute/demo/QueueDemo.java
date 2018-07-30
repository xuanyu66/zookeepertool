package com.yangxin.distribute.demo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
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
import java.util.*;
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
        Deque<byte[]> data = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Map<Long,String> map = new HashMap<>();
            map.put((long) i, Integer.toHexString(i));
            LOGGER.debug("第{}个map的gson值 {}", i,GSON.toJson(map).getBytes());
            data.add(GSON.toJson(map).getBytes());
        }
        Deque<byte[]> data2 = new LinkedList<>();
        //invokeMethods(10, "offer", queue, data);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOGGER.error("sleep failed {}", e);
        }
        Map<Integer, byte[]> map = invokeMethods(10, "poll", queue, data2);
        for (int i = 0; i < 10; i++) {
            byte [] temp = map.get(i);
            Map<Long,String> res = GSON.fromJson(new String(temp), new TypeToken<Map<Long,String>>(){}.getType());
            LOGGER.info("得到的第{}个map {}", i, res);
        }
    }

    private static Map<Integer, byte[]> invokeMethods(int field, String method, DistributeQueue queue, Deque<byte[]> data){
        Map<Integer, byte[]> map = new HashMap<>();

        for (int i = 0; i < field; i++) {
            byte [] temp = data.pollFirst();
            LOGGER.info("{}", i);
            try {

                if (temp == null){
                    Method metho = queue.getClass().getMethod(method,  null);
                    temp = (byte[]) metho.invoke(queue, null);
                    map.put(i, temp);
                }else {
                    Method metho = queue.getClass().getMethod(method, temp.getClass());
                    metho.invoke(queue, temp);
                }
            } catch ( NoSuchMethodException |
                    IllegalAccessException | InvocationTargetException e) {
                LOGGER.error("{}", e);
            }
        }
        return map;
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
