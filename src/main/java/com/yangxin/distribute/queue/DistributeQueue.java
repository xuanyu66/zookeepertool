package com.yangxin.distribute.queue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author leon on 2018/7/29.
 * @version 1.0
 * @Description:
 */
public class DistributeQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributeQueue.class);

    private final String dir;

    private ZooKeeper zooKeeper;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private final String prefix = "qn-";

    /***
     * @Description: 初始化变量值，如果zooKeeper没有该目录则新建PERSISTENT目录
     * @Param: [zooKeeper, dir, acl]
     */
    public DistributeQueue(ZooKeeper zooKeeper, String dir, List<ACL> acl){
        this.dir = dir;
        if (acl != null){
            this.acl = acl;
        }
        this.zooKeeper = zooKeeper;

        if (zooKeeper != null){
            try {
                Stat stat = zooKeeper.exists(dir, false);
                if (stat == null) {
                    zooKeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
                }
            }catch (InterruptedException | KeeperException e) {
                LOGGER.error("Caught :{}",e);
            }
        }
    }

    /***
    * @Description: 返回一个按id排序的child map
    * @Param: [watcher]
    * @return: java.util.TreeMap<java.lang.Long,java.lang.String>
    */
    private TreeMap<Long,String> orderedChildren(Watcher watcher) throws KeeperException,InterruptedException{
        TreeMap<Long,String> orderedChildren = new TreeMap<>();

        List<String> childNames = null;
        try {
            childNames = zooKeeper.getChildren(dir, watcher);
        } catch (KeeperException.NoNodeException e){
            throw e;
        }

        for (String childName : childNames) {
            try {
                if (!childName.regionMatches(0, prefix,0, prefix.length())){
                    LOGGER.warn("Found child node with improper name: {}", childName);
                    continue;
                }
                String suffix = childName.substring(prefix.length());
                Long childId = Long.valueOf(suffix);
                orderedChildren.put(childId, childName);
            } catch (NumberFormatException e) {
                LOGGER.warn("Found child node with improper format: {}  ", childName);
            }
        }
        return orderedChildren;
    }

    private String smallestChildName() throws KeeperException, InterruptedException {
        long minId = Long.MAX_VALUE;
        String minName = "";

        List<String> childNames = null;

        for (String childName : childNames) {
            try {
                if (!childName.regionMatches(0, prefix,0, prefix.length())){
                    LOGGER.warn("Found child node with improper name: {}", childName);
                    continue;
                }
                String suffix = childName.substring(prefix.length());
                Long childId = Long.parseLong(suffix);
                if (childId < minId) {
                    minId = childId;
                    minName = childName;
                }
            } catch (NumberFormatException e) {
                LOGGER.warn("Found child node with improper format: {}  ", childName);
            }
        }
        if (minId < Long.MAX_VALUE) {
            return minName;
        } else {
            return null;
        }
    }

    /***
    * @Description: 返回队列的头部元素而不修改队列
    * @Param: []
    * @return: byte[]
    */
    public byte[] element() throws NoSuchElementException, KeeperException, InterruptedException {
        TreeMap<Long,String> orderedChildren;

        while (true) {
            try {
                orderedChildren = orderedChildren(null);
            } catch (KeeperException.NoNodeException e) {
                throw new NoSuchElementException();
            }
            if (orderedChildren.size() == 0) {
                throw new NoSuchElementException();
            }

            for (String headNode : orderedChildren.values()) {
                if (headNode != null) {
                    try {
                        return zooKeeper.getData(dir + "/" + headNode, false, null);
                    }catch (KeeperException.NoNodeException e){
                    }
                }
            }
        }
    }

    /*** 
    * @Description: 尝试删除队头元素并返回
    * @Param: [] 
    * @return: byte[]  
    */  
    public byte[] remove() throws NoSuchElementException, KeeperException, InterruptedException{
        TreeMap<Long,String> orderedChildren;

        while (true) {
            try {
                orderedChildren = orderedChildren(null);
            } catch (KeeperException.NoNodeException e) {
                throw new NoSuchElementException();
            }
            if (orderedChildren.size() == 0) {
                throw new NoSuchElementException();
            }

            for (String headNode : orderedChildren.values()) {
                try {
                        byte[] data =  zooKeeper.getData(dir + "/" + headNode, false, null);
                        zooKeeper.delete(dir + "/" + headNode, -1);
                        return data; 
                }catch (KeeperException.NoNodeException e){
                    /***另外一个client先删除，retry*/
                }
            }
        }
    }
    
    private class LatchChlidWatcher implements Watcher {

        CountDownLatch latch;

        public LatchChlidWatcher(){
            latch = new CountDownLatch(1);
        }

        @Override
        public void process(WatchedEvent watchedEvent) {
            LOGGER.debug("Watcher fired on path: {} state: {} type: {}",
                    watchedEvent.getPath(), watchedEvent.getState(),
                    watchedEvent.getType());
            latch.countDown();
        }

        public void await() throws InterruptedException{
            latch.await();
        }
    }

    /***
    * @Description: 阻塞的删除队头的元素并返回
    * @Param: []
    * @return: byte[]
    */
    public byte[] take() throws KeeperException, InterruptedException{
        TreeMap<Long,String> orderedChildren;

        while (true) {
            LatchChlidWatcher chlidWatcher = new LatchChlidWatcher();
            try {
                orderedChildren = orderedChildren(chlidWatcher);
            } catch (KeeperException.NoNodeException e) {
                zooKeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
                continue;
            }
            if (orderedChildren.size() == 0) {
                chlidWatcher.wait();
                continue;
            }

            for (String headNode : orderedChildren.values()) {
                try {
                    byte[] data =  zooKeeper.getData(dir + "/" + headNode, false, null);
                    zooKeeper.delete(dir + "/" + headNode, -1);
                    return data;
                }catch (KeeperException.NoNodeException e){
                }
            }
        }
    }

    /***
    * @Description: 插入元素进队列
    * @Param: [data]
    * @return: boolean
    */
    public boolean offer(byte[] data) throws KeeperException, InterruptedException{
        while (true) {
            try {
                zooKeeper.create(dir + "/" + prefix, data, acl, CreateMode.PERSISTENT_SEQUENTIAL);
                return true;
            } catch (KeeperException.NoNodeException e) {
                zooKeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
            }
        }
    }

    public byte[] peek() throws KeeperException, InterruptedException{
        try {
            return element();
        }catch (NoSuchElementException e){
            return null;
        }
    }

    public byte[] poll() throws KeeperException,InterruptedException{
        try {
            return remove();
        }catch (NoSuchElementException e){
            return null;
        }
    }
}
