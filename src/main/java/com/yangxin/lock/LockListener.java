package com.yangxin.lock;

/**
 * @author leon on 2018/7/27.
 * @version 1.0
 * @Description: 回调接口
 */
public interface LockListener {

    /***
    * @Description: 要求锁时回调
    * @Param: []
    * @return: void
    */
    public void lockAcquired();

    /***
    * @Description: 释放锁时回调
    * @Param: []
    * @return: void
    */
    public void lockReased();
}
