package com.yangxin.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author leon on 2018/7/27.
 * @version 1.0
 * @Description: 如果zookeeper连接关闭，提供辅助方法去重连zookeeper
 */
class ProtocolSupport {
    private  static final Logger LOGGER = LoggerFactory.getLogger(ProtocolSupport.class);

    protected final ZooKeeper zooKeeper;
    /**判断连接是否关闭*/
    private AtomicBoolean closed = new AtomicBoolean(false);
    private long retryDelay = 500L;
    private int retryCount = 10;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    public ProtocolSupport(ZooKeeper zooKeeper){
        this.zooKeeper = zooKeeper;
    }

    public void close(){
        if(closed.compareAndSet(false, true)){
            doClose();
        }
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public List<ACL> getAcl() {
        return acl;
    }

    public void setAcl(List<ACL> acl) {
        this.acl = acl;
    }

    public long getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
    }

    /*** 
    * @Description: 允许继承类实现自己的关闭策略去释放资源 
    * @Param: [] 
    * @return: void  
    */ 
    protected void doClose(){
    }

    /*** 
    * @Description: 执行提供的操作，如果连接失败则重试
    * @Param: [operation] 
    * @return: java.lang.Object  
    */  
    protected Object retryOperation(ZookeeperOperation operation)
    throws KeeperException, InterruptedException {
        KeeperException exception = null;
        for (int i = 0; i < retryCount; i++) {
            try{
                return operation.excute();
            }catch (KeeperException.SessionExpiredException e){
                if (exception == null){
                    exception = e;
                }
                LOGGER.debug("Attempt {} failed with connection loss so" +
                "attempting to reconnect: {}", i, e);
                retryDelay(i);
            }
        }
        return exception;
    }

    /*** 
    * @Description: 保证给定的路径存在(路径无数据)
    * @Param: [path] 
    * @return: void  
    */  
    protected void ensurePathExists(String path){
        ensurePathExists(path, null, acl, CreateMode.PERSISTENT);
    }
    
    /*** 
    * @Description: 保证给定的路径存在 
    * @Param: [path, data, acl, flags] 
    * @return: void  
    */ 
    protected void ensurePathExists(final String path, final byte[] data,
                                    final List<ACL> acl, final CreateMode flags){
        try {
            retryOperation(new ZookeeperOperation() {
                @Override
                public boolean excute() throws KeeperException, InterruptedException {
                    Stat stat = zooKeeper.exists(path, false);
                    if (stat != null){
                        return true;
                    }
                    zooKeeper.create(path, data, acl, flags);
                    return true;
                }
            });
        }catch (KeeperException | InterruptedException e){
            LOGGER.error("Caught: {}", e);
        }
    }
    
    /*** 
    * @Description: return ture 如果连接已关闭 
    * @Param: [] 
    * @return: boolean  
    */ 
    protected boolean isClosed(){
        return closed.get();
    }
    
    /*** 
    * @Description:  如果这不是第一次尝试，则执行重试延迟
    * @Param: [attemptCount] 
    * @return: void  
    */ 
    protected void retryDelay(int attemptCount){
        if (attemptCount > 0){
            try {
                Thread.sleep(attemptCount * retryDelay);
            }catch (InterruptedException e){
                LOGGER.error("Falied to sleep：{}", e);
            }
        }
    }
}
