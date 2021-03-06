# zookeepertool
## 锁（Lock）
完全分布式锁是全局同步的，这意味着在任何时刻没有两个客户端会同时认为它们都拥有相同的锁，使用 Zookeeper 可以实现分布式锁，需要首先定义一个锁节点（lock root node）。

需要获得锁的客户端按照以下步骤来获取锁：

1. 保证锁节点（lock root node）这个父根节点的存在，这个节点是每个要获取lock客户端共用的，这个节点是PERSISTENT的。
2. 第一次需要创建本客户端要获取lock的节点，调用 create( )，并设置 节点为EPHEMERAL_SEQUENTIAL类型，表示该节点为临时的和顺序的。如果获取锁的节点挂掉，则该节点自动失效，可以让其他节点获取锁。
3. 在父锁节点（lock root node）上调用 getChildren( ) ，不需要设置监视标志。 (为了避免“羊群效应”).
4. 按照Fair竞争的原则，将步骤3中的子节点（要获取锁的节点）按照节点顺序的大小做排序，取出编号最小的一个节点做为lock的owner，判断自己的节点id是否就为owner id，如果是则返回，lock成功。如果不是则调用 exists( )监听比自己小的前一位的id，关注它锁释放的操作（也就是exist watch）。
6. 如果第4步监听exist的watch被触发，则继续按4中的原则判断自己是否能获取到lock。
   
   释放锁：需要释放锁的客户端只需要删除在第2步中创建的节点即可。

**注意事项**：

一个节点的删除只会导致一个客户端被唤醒，因为每个节点只被一个客户端watch，这避免了“羊群效应”。

# 队列（Queue）
分布式队列是通用的数据结构，为了在 Zookeeper 中实现分布式队列，首先需要指定一个 Znode 节点作为队列节点（queue node）， 各个分布式客户端通过调用 create() 函数向队列中放入数据，调用create()时节点路径名带”qn-”结尾，并设置顺序（sequence）节点标志。 由于设置了节点的顺序标志，新的路径名具有以下字符串模式：”_path-to-queue-node_/qn-X”，X 是唯一自增号。需要从队列中获取数据/移除数据的客户端首先调用 getChildren() 函数，有数据则获取（获取数据后可以删除也可以不删），没有则在队列节点（queue node）上将 watch 设置为 true，等待触发并处理最小序号的节点（即从序号最小的节点中取数据）。

实现步骤基本如下：

前提：需要一个队列root节点dir

入队：使用create()创建节点，将共享数据data放在该节点上，节点类型为PERSISTENT_SEQUENTIAL，永久顺序性的（也可以设置为临时的，看需求）。

出队：因为队列可能为空，2种方式处理：一种如果为空则wait等待，一种返回异常。

等待方式：这里使用了CountDownLatch的等待和Watcher的通知机制，使用了TreeMap的排序获取节点顺序最小的数据（FIFO）。

抛出异常：getChildren()获取队列数据时，如果size==0则抛出异常。
