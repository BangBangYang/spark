## RDD缓存

RDD通过persist方法或cache方法可以将前面的计算结果缓存，默认情况下 persist() 会把数据以序列化的形式缓存在 JVM 的堆空间中。 

但是并不是这两个方法被调用时立即缓存，而是触发后面的action时，该RDD将会被缓存在计算节点的内存中，并供后面重用。

​         ![1568552178744](C:\Users\yangkun\AppData\Roaming\Typora\typora-user-images\1568552178744.png)                                         

通过查看源码发现cache最终也是调用了persist方法，默认的存储级别都是仅在内存存储一份，Spark的存储级别还有好多种，存储级别在object StorageLevel中定义的。

   ![1568552196202](C:\Users\yangkun\AppData\Roaming\Typora\typora-user-images\1568552196202.png)

在存储级别的末尾加上“_2”来把持久化数据存为两份    

![1568552215149](C:\Users\yangkun\AppData\Roaming\Typora\typora-user-images\1568552215149.png)

缓存有可能丢失，或者存储存储于内存的数据由于内存不足而被删除，RDD的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行。通过基于RDD的一系列转换，丢失的数据会被重算，由于RDD的各个Partition是相对独立的，因此只需要计算丢失的部分即可，并不需要重算全部Partition。

```scala
scala> val rdd = sc.makeRDD(Array("atguigu"))
rdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at makeRDD at <console>:24
//将RDD转换为携带当前时间戳不做缓存
scala> val nocache = rdd.map(_.toString+System.currentTimeMillis)
nocache: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[1] at map at <console>:26
//多次打印结果
scala> nocache.collect
res1: Array[String] = Array(atguigu1568552516481)

scala> nocache.collect
res2: Array[String] = Array(atguigu1568552522009)
//将RDD转换为携带当前时间戳并做缓存
scala> val cache =  rdd.map(_.toString+System.currentTimeMillis).cache
cache: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at map at <console>:26
）多次打印做了缓存的结果
scala> cache.collect
res3: Array[String] = Array(atguigu1568552543421)

scala> cache.collect
res4: Array[String] = Array(atguigu1568552543421)

scala> cache.collect
res5: Array[String] = Array(atguigu1568552543421)

scala> cache.collect
res6: Array[String] = Array(atguigu1568552543421)

```

##  RDD CheckPoint

Spark中对于数据的保存除了持久化操作之外，还提供了一种检查点的机制，检查点（本质是通过将RDD写入Disk做检查点）是为了通过lineage做容错的辅助，lineage过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果之后有节点出现问题而丢失分区，从做检查点的RDD开始重做Lineage，就会减少开销。检查点通过将数据写入到HDFS文件系统实现了RDD的检查点功能。

为当前RDD设置检查点。该函数将会创建一个二进制的文件，并存储到checkpoint目录中，该目录是用[Spark](https://www.iteblog.com/archives/tag/spark/)Context.setCheckpointDir()设置的。在checkpoint的过程中，该RDD的所有依赖于父RDD中的信息将全部被移除。对RDD进行checkpoint操作并不会马上被执行，必须执行Action操作才能触发。

```scala
//（1）设置检查点
scala> sc.setCheckpointDir("hdfs://hadoop102:9000/checkpoint")
//（2）创建一个RDD
scala> val rdd = sc.parallelize(Array("atguigu"))
rdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[14] at parallelize at <console>:24
//（3）将RDD转换为携带当前时间戳并做checkpoint
scala> val ch = rdd.map(_+System.currentTimeMillis)
ch: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[16] at map at <console>:26

scala> ch.checkpoint
//（4）多次打印结果
scala> ch.collect
res55: Array[String] = Array(atguigu1538981860336)

scala> ch.collect
res56: Array[String] = Array(atguigu1538981860504)

scala> ch.collect
res57: Array[String] = Array(atguigu1538981860504)

scala> ch.collect
res58: Array[String] = Array(atguigu1538981860504)

```

```scala
package com.atguigu

import org.apache.spark.{SparkConf, SparkContext}

object checkpointTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    val rdd =  sc.makeRDD(Array(1,2,3))
    val maprdd = rdd.map((_,1))
    val result = maprdd.reduceByKey(_+_)
    result.checkpoint()
    result.foreach(println)
    println(result.toDebugString)
    /*
    无 checkpoint 模式的依赖关系
    (8) ShuffledRDD[2] at reduceByKey at checkpointTest.scala:12 []
 +-(8) MapPartitionsRDD[1] at map at checkpointTest.scala:11 []
    |  ParallelCollectionRDD[0] at makeRDD at checkpointTest.scala:10 []
    checkpoint 模式的依赖关系
    (8) ShuffledRDD[2] at reduceByKey at checkpointTest.scala:12 []
 |  ReliableCheckpointRDD[3] at foreach at checkpointTest.scala:14 []
    */
  }
}

```

