package com.yangxin.distribute.lock;

import org.apache.zookeeper.KeeperException;
/**
 * @description
 * @author leon on 2018/7/27.
 * @version 1.0
 */
public interface ZookeeperOperation {

    public boolean excute() throws KeeperException, InterruptedException;
}
