package com.yangxin.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @description 基于zookeeper的分布式锁的实现
 * @author leon on 2018/7/27.
 * @version 1.0
 */
public class DistributeLock extends ProtocolSupport{
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributeLock.class);

    private final String dir;
    private ZNodeName idName;
    private String id;
    private String ownId;
    private String lastChildId;
    private byte[] data = {0x12, 0x34};
    private LockListener callback;
    private LockZooKeeperOperation zop;

    /**
     * @Description:
     * @Param: zooKeeper zooKeeper客户端实例
     * @Param: dir 期望用锁同步的parent path
     * @Param: acl 默认权限
     */
    public DistributeLock(ZooKeeper zooKeeper, String dir, List<ACL> acl){
        super(zooKeeper);
        this.dir = dir;
        if (acl != null) {
            setAcl(acl);
        }
        this.zop = new LockZooKeeperOperation();
    }

    /***
     * @Description:
     * @Param: callback LockListener实例
     */
    public DistributeLock(ZooKeeper zooKeeper, String dir, List<ACL> acl,
                          LockListener callback) {
        this(zooKeeper, dir, acl);
        this.callback = callback;
    }

    public LockListener getLockListener() {
        return callback;
    }

    public void setLockListener(LockListener callback) {
        this.callback = callback;
    }

    /***
    * @Description: 删除锁或者相应的znode节点
    * @throws: 当连接不上zookeeper时,抛出一个RuntimeException
    */
    public synchronized void unlock() throws RuntimeException {
        if (!isClosed() && id != null) {
            /***当连接失败的时候不需要重新执行这个操作，因为zookeeper会自动
             * 删除临时节点*/
            try {
                ZookeeperOperation zopdel = new ZookeeperOperation() {
                    @Override
                    public boolean excute() throws KeeperException, InterruptedException {
                        zooKeeper.delete(id, -1);
                        return Boolean.TRUE;
                    }
                };
                zopdel.excute();
            } catch (InterruptedException e){
                LOGGER.error("Caught: {}", e);
                Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e){
            } catch (KeeperException e){
                LOGGER.error("Caught: {}", e);
                throw (RuntimeException) new RuntimeException(e.getMessage()).
                        initCause(e);
            } finally {
                if (callback != null){
                    callback.lockReased();
                }
                id = null;
            }
        }
    }

    private class LockWatcher implements Watcher{
        @Override
        public void process(WatchedEvent watchedEvent) {
            LOGGER.info("Watcher fired on path: {} state: {} type: {}",
                    watchedEvent.getPath(), watchedEvent.getState(),
                    watchedEvent.getType());
            try {
                lock();
            } catch (Exception e){
                LOGGER.error("Failed to acquire lock: {}", e);
            }
        }
    }

    private class LockZooKeeperOperation implements ZookeeperOperation {

        /***
        * @Description: 找到相应的节点，如果没有则新建节点
        * @Param: [prefix, zooKeeper, dir]
        * @return: void
        */
        private void findPrefixInChildern(String prefix, ZooKeeper zooKeeper,
                                          String dir) throws KeeperException,
                                            InterruptedException{
            List<String> names = zooKeeper.getChildren(dir, false);
            for (String name : names) {
                if (name.startsWith(prefix)) {
                    id = dir + "/" + name;
                    LOGGER.debug("Found id created last time: {}", id);
                    break;
                }
            }
            if(id == null) {
                id = zooKeeper.create(dir + "/" + prefix, data, getAcl(),
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                LOGGER.debug("Created id: {}", id);
            }
        }

        @Override
        public boolean excute () throws KeeperException, InterruptedException {
            do {
                if (id == null) {
                    long sessionId = zooKeeper.getSessionId();
                    String prefix = "x-" + sessionId + "-";
                    findPrefixInChildern(prefix, zooKeeper, dir);
                    idName = new ZNodeName(id);
                }
                if (id != null) {
                    List<String> names = zooKeeper.getChildren(dir, false);
                    if (names.isEmpty()){
                        LOGGER.warn("No children in: {} when we've just" +
                                "created one! Lest recreate it ...");
                        id = null;
                    } else {
                        SortedSet<ZNodeName> sortedNames = new TreeSet<>();
                        for (String name : names) {
                            sortedNames.add(new ZNodeName(dir + "/" + name));
                        }
                        ownId = sortedNames.first().getName();
                        SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
                        if (!lessThanMe.isEmpty()) {
                            ZNodeName lastChildName = lessThanMe.last();
                            lastChildId = lastChildName.getName();
                            LOGGER.debug("watching less than me node: {}", lastChildId);
                            Stat stat = zooKeeper.exists(lastChildId, new LockWatcher());
                            if (stat != null) {
                                return Boolean.FALSE;
                            } else {
                                LOGGER.warn("Could not find the stats for less" +
                                        "than me: {}", lastChildName.getName());
                            }
                        } else {
                            if (isOwner()) {
                                if (callback != null) {
                                    callback.lockAcquired();
                                }
                                return Boolean.TRUE;
                            }
                        }
                    }
                }
            } while (id == null);
            return Boolean.TRUE;
        }
    }
    
    /*** 
    * @Description: 尝试获取读锁
    * @Param: [] 
    * @return: boolean  
    */ 
    public synchronized boolean lock() throws KeeperException, InterruptedException{
        if (isClosed()){
            return false;
        }
        ensurePathExists(dir);
        
        return (Boolean) retryOperation(zop);
    }

    public String getDir() {
        return dir;
    }
    
    /*** 
    * @Description: 返回ture如果该节点是锁的拥有者 
    * @Param: [] 
    * @return: boolean  
    */ 
    public boolean isOwner(){
        return id != null && ownId != null && id.equals(ownId);
    }

    public String getId() {
        return id;
    }
}
