package com.yangxin.lock;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
    private byte[] data = {0x12, 0x34};
    private LockListener callback;
    private LockZookeeperOperation zop;

    public DistributeLock(ZooKeeper zooKeeper, String dir, List<ACL> acl){
        super(zooKeeper);
    }

}
