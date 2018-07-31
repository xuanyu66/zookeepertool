package com.yangxin.distribute.demo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.yangxin.distribute.queue.DistributeQueue;
import com.yangxin.distribute.zkmethod.MainZooKeeper;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static CountDownLatch latch;
    private static ZooKeeper zooKeeper;
    private static DistributeQueue queue;
    private static  int field;

    public static void main(String[] args) {
        init();
        Deque<byte[]> data = new LinkedList<>();
        for (int i = 0; i < field; i++) {
            Map<Long,String> map = new HashMap<>(field);
            map.put((long) i, Integer.toHexString(i));
            LOGGER.debug("第{}个map的gson值 {}", i,GSON.toJson(map).getBytes());
            data.add(GSON.toJson(map).getBytes());
        }

        invokeMethods(10, "offer", queue, data);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOGGER.error("sleep failed {}", e);
        }

        Deque<byte[]> data2 = new LinkedList<>();
        Map<Integer, byte[]> map = invokeMethods(10, "poll", queue, data2);
        for (int i = 0; i < field; i++) {
            byte [] temp = map.get(i);
            Map<Long,String> res = GSON.fromJson(new String(temp), new TypeToken<Map<Long,String>>(){}.getType());
            LOGGER.info("得到的第{}个map {}", i, res);
        }
    }

    private static void init(){
        latch = new CountDownLatch(1);
        zooKeeper = new MainZooKeeper().connectServer();
        queue = new DistributeQueue(zooKeeper, "/queue_demo", null);
        field = 10;
    }

    private static Map<Integer, byte[]> invokeMethods(int field, String method, DistributeQueue queue, Deque<byte[]> data){
        Map<Integer, byte[]> map = new HashMap<>(field);

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
}
