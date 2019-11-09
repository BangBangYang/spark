# 第2章 RDD编程

## 2.1 编程模型

在Spark中，RDD被表示为对象，通过对象上的方法调用来对RDD进行转换。经过一系列的transformations定义RDD之后，就可以调用actions触发RDD的计算，action可以是向应用程序返回结果(count, collect等)，或者是向存储系统保存数据(saveAsTextFile等)。在Spark中，只有遇到action，才会执行RDD的计算(即延迟计算)，这样在运行时可以通过管道的方式传输多个转换。

​    要使用Spark，开发者需要编写一个Driver程序，它被提交到集群以调度运行Worker，如下图所示。Driver中定义了一个或多个RDD，并调用RDD上的action，Worker则执行RDD分区计算任务。

​                                                  

   ![1566623566506](C:\Users\yangkun\AppData\Roaming\Typora\typora-user-images\1566623566506.png)

![1566623579783](C:\Users\yangkun\AppData\Roaming\Typora\typora-user-images\1566623579783.png)

## 2.2 RDD创建

### 2.2.1从集合创建

由一个已经存在的Scala集合创建，集合并行化。

```scala
scala> val res = sc.parallelize(Array(1,2,3,4,5))
res: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at <console>:24

scala> res.collect()
res2: Array[Int] = Array(1, 2, 3, 4, 5)

```

```
scala> val res = sc.makeRDD(Array(1,2,3,4,5))
res: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[2] at makeRDD at <console>:24

scala> res.collect()
res3: Array[Int] = Array(1, 2, 3, 4, 5)
```

## 2.3 TransFormation（RDD转化）

### 2.3.1 map(func)

返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成

```
scala> val source = sc.parallelize(1 to 10)
source: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> source.collect()
res0: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

scala> source.map(_+1).collect
res1: Array[Int] = Array(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

scala> source.map((_,1)).collect
res3: Array[(Int, Int)] = Array((1,1), (2,1), (3,1), (4,1), (5,1), (6,1), (7,1), (8,1), (9,1), (10,1))
```

### 2.3.2 mapPartitions(func) 尽量使用mapPartitions

注：

类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] =>Iterator[U]。假设有N个元素，有M个分区，那么map的函数的将被调用N次,而mapPartitions被调用M次,一个函数一次处理所有分区

```
scala> val rdd = sc.parallelize(List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female")))
rdd: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[16] at parallelize at <console>:24

scala> :paste
// Entering paste mode (ctrl-D to finish)
def partitionsFun(iter : Iterator[(String,String)]) : Iterator[String] = {
  var woman = List[String]()
  while (iter.hasNext){
    val next = iter.next()
    next match {
       case (_,"female") => woman = next._1 :: woman
       case _ =>
    }
  }
  woman.iterator
}
// Exiting paste mode, now interpreting.

partitionsFun: (iter: Iterator[(String, String)])Iterator[String]

scala> val result = rdd.mapPartitions(partitionsFun)
result: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[17] at mapPartitions at <console>:28

scala> result.collect()
res13: Array[String] = Array(kpop, lucy)

```

```scala
object mapPati {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    //2、创建sparkcontext
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female")))
    val res = rdd.mapPartitions(partitionsFun)
    println(res.collect())
  }
  def partitionsFun(iter : Iterator[(String,String)]) : Iterator[String] = {
    var woman = List[String]()
    while (iter.hasNext){
      val next = iter.next()
      next match {
        case (_,"female") => woman = next._1 :: woman
        case _ =>
      }
    }
    println(woman.toString())
    woman.iterator
  }
}
```

### 2.3.3 glom

将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]

```scala
scala> val rdd = sc.parallelize(1 to 16,4)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> rdd.collect
res0: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
scala> rdd.glom().collect
res2: Array[Array[Int]] = Array(Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(9, 10, 11, 12), Array(13, 14, 15, 16))
```

### 2.3.4 flatMap(func) map后再扁平化

类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）

```scala
package com.atguigu

import org.apache.spark.{SparkConf, SparkContext}

object flatMp {
  def main(args: Array[String]): Unit = {

      val sc = new SparkContext(new SparkConf().setAppName("map_flatMap_demo").setMaster("local"))
      val arrayRDD =sc.parallelize(Array("a_b","c_d","e_f"))
      arrayRDD.foreach(println) //打印结果1
      /*
        a_b
        c_d
        e_f
      */
      arrayRDD.map(string=>{
        string.split("_")
      }).foreach(x=>{
        println(x.mkString(",")) //打印结果2
      })
      /*
      a,b
      c,d
      e,f
       */
      arrayRDD.flatMap(string=>{
        string.split("_")
      }).foreach(x=>{
        println(x.mkString(","))//打印结果3
      })
      /*
      a
      b
      c
      d
      e
      f
       */
  }

}
```

对比结果2与结果3，很容易得出结论：

map函数后，RDD的值为 Array(Array("a","b"),Array("c","d"),Array("e","f"))

flatMap函数处理后，RDD的值为 Array("a","b","c","d","e","f")

即最终可以认为，flatMap会将其返回的数组全部拆散，然后合成到一个数组中

```scala
scala> val rdd = sc.parallelize(1 to 5)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[2] at parallelize at <console>:24

scala> rdd.map(_ + 1).collect
res3: Array[Int] = Array(2, 3, 4, 5, 6)
////flatMap源码的参数是需要可迭代类型的
rdd.flatMap(_+1).collect
<console>:27: error: type mismatch;
scala> rdd.flatMap(x => Array(x+1)).collect()
res7: Array[Int] = Array(2, 3, 4, 5, 6)
```

注

```scala
 def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))
  }
//源码的参数是需要可迭代类型的
```

### 2.3.5 filter(func)

返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成

```scala
scala> val srdd = sc.parallelize(Array("xiaoming","xiaojiang","liming","haha"))
srdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[5] at parallelize at                       
scala> val rdd = srdd.filter(_.contains("xiao"))
rdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[6] at filter at <console>:26

scala> srdd.collect
res8: Array[String] = Array(xiaoming, xiaojiang, liming, haha)

scala> rdd.collect
res9: Array[String] = Array(xiaoming, xiaojiang)

```

### 2.3.6 mapPartitionsWithIndex(func)

类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U]

```scala
scala> val rdd = sc.parallelize(1 to 5,3)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[9] at parallelize at <console>:24

scala> rdd.mapPartitionsWithIndex((x, y) => Iterator(x+":"+y.mkString("|"))).collect
res12: Array[String] = Array(0:1, 1:2|3, 2:4|5)
```

### 2.3.7 sample(withReplacement, fraction, seed)

以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。例子从RDD中随机且有放回的抽出50%的数据，随机种子值为3（即可能以1 2 3的其中一个起始值）

```scala
scala> val rdd = sc.parallelize(1 to 10)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[20] at parallelize at <console>:24

scala> rdd.collect()
res15: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

scala> var sample1 = rdd.sample(true,0.4,2)
sample1: org.apache.spark.rdd.RDD[Int] = PartitionwiseSampledRDD[21] at sample at <console>:26

scala> sample1.collect()
res16: Array[Int] = Array(1, 2, 2, 7, 7, 8, 9)

scala> var sample2 = rdd.sample(false,0.2,3)
sample2: org.apache.spark.rdd.RDD[Int] = PartitionwiseSampledRDD[22] at sample at <console>:26

scala> sample2.collect()
res17: Array[Int] = Array(1, 9)

```

### 2.3.8 distinct([numTasks]))

对源RDD进行去重后返回一个新的RDD. 默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它。

```
scala> val rdd = sc.parallelize(Array(1,1,2))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[11] at parallelize at <console>:24

scala> rdd.distinct
res13: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[14] at distinct at <console>:27

scala> rdd.distinct.collect
res14: Array[Int] = Array(1, 2)

```

### 2.3.9 partitionBy

对RDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区， 否则会生成ShuffleRDD。

```
scala> val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
rdd: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[18] at parallelize at <console>:24

scala> rdd.partitions.size
res16: Int = 4

scala> var rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
rdd2: org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[19] at partitionBy at <console>:26

scala> rdd2.partitions.size
res17: Int = 2

```

### 2.3.10 coalesce(numPartitions) 

与repartition的区别: repartition(numPartitions:Int):RDD[T]和coalesce(numPartitions:Int，shuffle:Boolean=false):RDD[T] repartition只是coalesce接口中shuffle为true的实现.



缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。

```scala
scala> val rdd = sc.parallelize(1 to 16,4)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[54] at parallelize at <console>:24

scala> rdd.partitions.size
res20: Int = 4

scala> val coalesceRDD = rdd.coalesce(3)
coalesceRDD: org.apache.spark.rdd.RDD[Int] = CoalescedRDD[55] at coalesce at <console>:26

scala> coalesceRDD.partitions.size
res21: Int = 3

```

### 2.3.11 repartition(numPartitions) 

根据分区数，从新通过网络随机洗牌所有数据。

```scala
scala> val rdd = sc.parallelize(1 to 16,4)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[56] at parallelize at <console>:24

scala> rdd.partitions.size
res22: Int = 4

scala> val rerdd = rdd.repartition(2)
rerdd: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[60] at repartition at <console>:26

scala> rerdd.partitions.size
res23: Int = 2

scala> val rerdd = rdd.repartition(4)
rerdd: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[64] at repartition at <console>:26

scala> rerdd.partitions.size
res24: Int = 4

```

### 2.3.12 repartitionAndSortWithinPartitions(partitioner) 

repartitionAndSortWithinPartitions函数是函数的变种，与函数不同的是，在给定的内部进行排序，性能比要高。

### 2.3.13 sortBy(func,[ascending], [numTasks])

用func先对数据进行处理，按照处理后的数据比较结果排序。

```scala
scala> val rdd = sc.parallelize(List(1,2,3,4))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[21] at parallelize at <console>:24

scala> rdd.sortBy(x => x).collect()
res11: Array[Int] = Array(1, 2, 3, 4)

scala> rdd.sortBy(x => x%3).collect()
res12: Array[Int] = Array(3, 4, 1, 2)
```

### 2.3.14 union(otherDataset)(并)

对源RDD和参数RDD求并集后返回一个新的RDD  不去重

```scala
scala> val rdd1 = sc.parallelize(1 to 5)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[23] at parallelize at <console>:24

scala> val rdd2 = sc.parallelize(5 to 10)
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[24] at parallelize at <console>:24

scala> val rdd3 = rdd1.union(rdd2)
rdd3: org.apache.spark.rdd.RDD[Int] = UnionRDD[25] at union at <console>:28

scala> rdd3.collect()
res18: Array[Int] = Array(1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10)

```

### 2.3.15 subtract (otherDataset)(差)

计算差的一种函数，去除两个RDD中相同的元素，不同的RDD将保留下来 

```scala
scala> val rdd = sc.parallelize(3 to 8)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[70] at parallelize at <console>:24

scala> val rdd1 = sc.parallelize(1 to 5)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[71] at parallelize at <console>:24

scala> rdd.subtract(rdd1).collect()
res27: Array[Int] = Array(8, 6, 7)
```

### 2.3.16 intersection(otherDataset)（交）

对源RDD和参数RDD求交集后返回一个新的RDD

```scala
scala> val rdd1 = sc.parallelize(1 to 7)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[26] at parallelize at <console>:24

scala> val rdd2 = sc.parallelize(5 to 10)
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[27] at parallelize at <console>:24

scala> val rdd3 = rdd1.intersection(rdd2)
rdd3: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[33] at intersection at <console>:28

scala> rdd3.collect()
res19: Array[Int] = Array(5, 6, 7)

```

### 2.3.17 cartesian(otherDataset)（笛卡尔积）

笛卡尔积

```scala
scala> val rdd1 = sc.parallelize(1 to 3)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[47] at parallelize at <console>:24

scala> val rdd2 = sc.parallelize(2 to 5)
rdd2: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[48] at parallelize at <console>:24

scala> rdd1.cartesian(rdd2).collect()
res17: Array[(Int, Int)] = Array((1,2), (1,3), (1,4), (1,5), (2,2), (2,3), (2,4), (2,5), (3,2), (3,3), (3,4), (3,5))

```

### 2.3.18 pipe(command, [envVars])

管道，对于每个分区，都执行一个perl或者shell脚本，返回输出的RDD

```scala
scala> val rdd = sc.parallelize(List("hi","how","are","hello"))
rdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[13] at parallelize at <console>:24

scala> rdd.pipe("/opt/module/spark-2.1.1-bin-hadoop2.7/pipe.sh").collect
res4: Array[String] = Array(AA, >>>hi, >>>how, >>>are, >>>hello)

scala> val rdd = sc.parallelize(List("hi","how","are","hello"),2)
rdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[15] at parallelize at <console>:24

scala> rdd.pipe("/opt/module/spark-2.1.1-bin-hadoop2.7/pipe.sh").collect
res5: Array[String] = Array(AA, >>>hi, >>>how, AA, >>>are, >>>hello)

```

### 2.3.19 join(otherDataset, [numTasks])

在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD

```scala
scala> val rdd = sc.parallelize(1 to 10)
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> val rdd1 = sc.parallelize(5 to 15)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at <console>:24

scala> val rdd2 = rdd.map((_,1))
rdd2: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[2] at map at <console>:26

scala> val rdd3 = rdd1.map((_,1))
rdd3: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[3] at map at <console>:26

scala> rdd2.join(rdd3).collect
res0: Array[(Int, (Int, Int))] = Array((6,(1,1)), (7,(1,1)), (9,(1,1)), (8,(1,1)), (10,(1,1)), (5,(1,1)))

scala> val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
rdd: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[32] at parallelize at <console>:24

scala> val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6)))
rdd1: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[33] at parallelize at <console>:24

scala> rdd.join(rdd1).collect()
res13: Array[(Int, (String, Int))] = Array((1,(a,4)), (2,(b,5)), (3,(c,6)))

```

### 2.3.20 cogroup(otherDataset, [numTasks])

在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD

```scala
scala> val rdd = sc.parallelize(Array(0,1,1,3,3)).map((_,1))
rdd: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[8] at map at <console>:24

scala> rdd.collect
res2: Array[(Int, Int)] = Array((0,1), (1,1), (1,1), (3,1), (3,1))

scala> val rdd1 = sc.parallelize(Array(0,2,2,3,4)).map((_,1))
rdd1: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[10] at map at <console>:24

scala> rdd1.collect
res3: Array[(Int, Int)] = Array((0,1), (2,1), (2,1), (3,1), (4,1))

scala> rdd.cogroup(rdd1).collect()
res1: Array[(Int, (Iterable[Int], Iterable[Int]))] = Array((4,(CompactBuffer(),CompactBuffer(1))), (0,(CompactBuffer(1),CompactBuffer(1))), (1,(CompactBuffer(1, 1),CompactBuffer())), (3,(CompactBuffer(1, 1),CompactBuffer(1))), (2,(CompactBuffer(),CompactBuffer(1, 1))))


scala> val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
rdd: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[37] at parallelize at <console>:24

scala> val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6)))
rdd1: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[38] at parallelize at <console>:24

scala> rdd.cogroup(rdd1).collect()
res14: Array[(Int, (Iterable[String], Iterable[Int]))] = Array((1,(CompactBuffer(a),CompactBuffer(4))), (2,(CompactBuffer(b),CompactBuffer(5))), (3,(CompactBuffer(c),CompactBuffer(6))))

scala> val rdd2 = sc.parallelize(Array((4,4),(2,5),(3,6)))
rdd2: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[41] at parallelize at <console>:24

scala> rdd.cogroup(rdd2).collect()
res15: Array[(Int, (Iterable[String], Iterable[Int]))] = Array((4,(CompactBuffer(),CompactBuffer(4))), (1,(CompactBuffer(a),CompactBuffer())), (2,(CompactBuffer(b),CompactBuffer(5))), (3,(CompactBuffer(c),CompactBuffer(6))))

scala> val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
rdd3: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[44] at parallelize at <console>:24

scala> rdd3.cogroup(rdd2).collect()
[Stage 36:>                                                         (0 + 0)                                                                             res16: Array[(Int, (Iterable[String], Iterable[Int]))] = Array((4,(CompactBuffer(),CompactBuffer(4))), (1,(CompactBuffer(d, a),CompactBuffer())), (2,(CompactBuffer(b),CompactBuffer(5))), (3,(CompactBuffer(c),CompactBuffer(6))))

```

### 2.3.21 reduceByKey(func, [numTasks]) 

在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置。

```
scala> val rdd = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))
rdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[15] at parallelize at <console>:24

scala> val reduce = rdd.reduceByKey((x,y) => x+y)
reduce: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[16] at reduceByKey at <console>:26

scala> reduce.collect()
res8: Array[(String, Int)] = Array((male,7), (female,6))

```

### 2.3.22 groupByKey

groupByKey也是对每个key进行操作，但只生成一个sequence。

```scala
scala> val wordPaidsRDD = sc.parallelize(Array("one", "two", "two", "three", "three", "three"))
wordPaidsRDD: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> val wordPairsRDD = sc.parallelize(Array("one", "two", "two", "three", "three", "three")).map((_,1))
wordPairsRDD: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[2] at map at <console>:24

scala> wordPai
wordPaidsRDD   wordPairsRDD

scala> wordPairsRDD.collect
res0: Array[(String, Int)] = Array((one,1), (two,1), (two,1), (three,1), (three,1), (three,1))

scala> val group = wordPairsRDD.groupByKey()
group: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[3] at groupByKey at <console>:26

scala> group.collect
res1: Array[(String, Iterable[Int])] = Array((two,CompactBuffer(1, 1)), (one,CompactBuffer(1)), (three,CompactBuffer(1, 1, 1)))

scala> val res = group.map(t => (t._1, t._2.sum))
res: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[4] at map at <console>:28

scala> res.collect
res2: Array[(String, Int)] = Array((two,2), (one,1), (three,3))
```

### 2.3.23 combineByKey[C]

(  createCombiner: V => C,  mergeValue: (C, V) => C,  mergeCombiners: (C, C) => C) 

对相同K，把V合并成一个集合。

createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就 和之前的某个元素的键相同。如果这是一个新的元素,combineByKey() 会使用一个叫作 createCombiner() 的函数来创建 
 那个键对应的累加器的初始值

mergeValue: 如果这是一个在处理当前分区之前已经遇到的键， 它会使用 mergeValue() 方法将该键的累加器对应的当前值与这个新的值进行合并

mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。

![1567156196697](C:\Users\yangkun\AppData\Roaming\Typora\typora-user-images\1567156196697.png)

![1567181167127](C:\Users\yangkun\AppData\Roaming\Typora\typora-user-images\1567181167127.png)

```scala
//求平均值
scala>  val scores = Array(("Fred", 88), ("Fred", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
scores: Array[(String, Int)] = Array((Fred,88), (Fred,95), (Fred,91), (Wilma,93), (Wilma,95), (Wilma,98))

scala> val input = sc.parallelize(scores)
input: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:26

scala> val combine = input
input   input_file_name

scala> val combine = input.combineByKey(
     | v => (v, 1),
     | (c:(Int,Int), v) => (c._1 + v, c._2+1 ),
     | (c1:(Int,Int), c2:(Int,Int)) => (c1._1+c2._1, c1._2+c2._2))
combine: org.apache.spark.rdd.RDD[(String, (Int, Int))] = ShuffledRDD[1] at combineByKey at <console>:28

scala> combine.collect
res0: Array[(String, (Int, Int))] = Array((Wilma,(286,3)), (Fred,(274,3)))

scala> val res = combine.map((key,value) => (key,value._1/value._2.toDouble))
<console>:30: error: missing parameter type
Note: The expected type requires a one-argument function accepting a 2-Tuple.
      Consider a pattern matching anonymous function, `{ case (key, value) =>  ... }`
       val res = combine.map((key,value) => (key,value._1/value._2.toDouble))
                              ^
<console>:30: error: missing parameter type
       val res = combine.map((key,value) => (key,value._1/value._2.toDouble))
                                  ^

scala> val res = combine.map{ (key,value) => (key,value._1/value._2.toDouble)}
<console>:30: error: missing parameter type
Note: The expected type requires a one-argument function accepting a 2-Tuple.
      Consider a pattern matching anonymous function, `{ case (key, value) =>  ... }`
       val res = combine.map{ (key,value) => (key,value._1/value._2.toDouble)}
                               ^
<console>:30: error: missing parameter type
       val res = combine.map{ (key,value) => (key,value._1/value._2.toDouble)}
                                   ^

scala> val res = combine.map{case (key,value) => (key,value._1/value._2.toDouble)}
res: org.apache.spark.rdd.RDD[(String, Double)] = MapPartitionsRDD[2] at map at <console>:30

scala> res.collect
res1: Array[(String, Double)] = Array((Wilma,95.33333333333333), (Fred,91.33333333333333))

```

### 2.3.24 aggregateByKey

(zeroValue:U,[partitioner: Partitioner]) (seqOp: (U, V) => U,combOp: (U, U) => U)    （这是柯里化函数）

在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算（先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），将key与计算结果作为一个新的kv对输出。

seqOp函数用于在每一个分区中用初始值逐步迭代value，combOp函数用于合并每个分区中的结果。

#### 求平均值

```scala
scala> val input = sc.parallelize( Array(("Fred", 88), ("Fred", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98)))
input: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> val agg = input.aggregate
aggregate   aggregateByKey
//该算子第一个参数是一个元组，别忘了加括号
scala> val agg = input.aggregateByKey(0,0)((u,v)=>(u._1+v,u._2+1),(u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
<console>:26: error: value _1 is not a member of Int
       val agg = input.aggregateByKey(0,0)((u,v)=>(u._1+v,u._2+1),(u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
                                                     ^
<console>:26: error: value _2 is not a member of Int
       val agg = input.aggregateByKey(0,0)((u,v)=>(u._1+v,u._2+1),(u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
                                                            ^
<console>:26: error: value _1 is not a member of Int
       val agg = input.aggregateByKey(0,0)((u,v)=>(u._1+v,u._2+1),(u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
                                                                               ^
<console>:26: error: value _1 is not a member of Int
       val agg = input.aggregateByKey(0,0)((u,v)=>(u._1+v,u._2+1),(u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
                                                                                     ^
<console>:26: error: value _2 is not a member of Int
       val agg = input.aggregateByKey(0,0)((u,v)=>(u._1+v,u._2+1),(u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
                                                                                           ^
<console>:26: error: value _2 is not a member of Int
       val agg = input.aggregateByKey(0,0)((u,v)=>(u._1+v,u._2+1),(u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
                                                                                                 ^

scala> val agg = input.aggregateByKey((0,0))((u,v)=>(u._1+v,u._2+1),(u1,u2)=>(u1._1+u2._1,u1._2+u2._2))
agg: org.apache.spark.rdd.RDD[(String, (Int, Int))] = ShuffledRDD[1] at aggregateByKey at <console>:26

scala> agg.collect
res0: Array[(String, (Int, Int))] = Array((Wilma,(286,3)), (Fred,(274,3)))

scala> val res = agg.map{case (key,value) => (key,value._1/value._2.toDouble)}
res: org.apache.spark.rdd.RDD[(String, Double)] = MapPartitionsRDD[2] at map at <console>:28

scala> res.collect
res1: Array[(String, Double)] = Array((Wilma,95.33333333333333), (Fred,91.33333333333333))

```

#### 分区按key取最大值，不同区同key的最大值相加

```scala
scala> val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
rdd: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[12] at parallelize at <console>:24

scala> val agg = rdd.aggregateByKey(0)(math.max(_,_),_+_)
agg: org.apache.spark.rdd.RDD[(Int, Int)] = ShuffledRDD[13] at aggregateByKey at <console>:26

scala> agg.collect()
res7: Array[(Int, Int)] = Array((3,8), (1,7), (2,3))

scala> agg.partitions.size
res8: Int = 3

scala> val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),1)
rdd: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[10] at parallelize at <console>:24

scala> val agg = rdd.aggregateByKey(0)(math.max(_,_),_+_).collect()
agg: Array[(Int, Int)] = Array((1,4), (3,8), (2,3))

```

### 2.3.25 foldByKey

(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] 

aggregateByKey的简化操作，seqop和combop相同

```scala
scala> val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
rdd: org.apache.spark.rdd.RDD[(Int, Int)] = ParallelCollectionRDD[3] at parallelize at <console>:24
scala> val res = rdd.foldByKey(0)(_+_)
res: org.apache.spark.rdd.RDD[(Int, Int)] = ShuffledRDD[4] at foldByKey at <console>:26
scala> res.collect
res2: Array[(Int, Int)] = Array((3,14), (1,9), (2,3))
```

### 2.3.26 sortByKey([ascending], [numTasks]) 

在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD

```scala
scala> val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
rdd: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[5] at parallelize at <console>:24

scala> val res = rdd.sortByKey(true).collect
res: Array[(Int, String)] = Array((1,dd), (2,bb), (3,aa), (6,cc))

scala> val res = rdd.sortByKey(true).collect
res: Array[(Int, String)] = Array((1,dd), (2,bb), (3,aa), (6,cc))

scala> val res = rdd.sortByKey(false).collect
res: Array[(Int, String)] = Array((6,cc), (3,aa), (2,bb), (1,dd))

```

### 2.3.27 mapValues

针对于(K,V)形式的类型只对V进行操作 

```scala
scala> val rdd = sc.parallelize(Array(1,2,1))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> rdd.map((_,1)).mapValues(_*2)
res0: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[2] at mapValues at <console>:27

scala> rdd.map((_,1)).mapValues(_*2).collect
res1: Array[(Int, Int)] = Array((1,2), (2,2), (1,2))

scala> rdd.map((_,1)).map(x=>(x._1,x._2*2))
res2: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[6] at map at <console>:27

scala> rdd.map((_,1)).map(x=>(x._1,x._2*2)).collect
res3: Array[(Int, Int)] = Array((1,2), (2,2), (1,2))

```

