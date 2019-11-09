# 第5章 RDD编程进阶

## 5.1 累加器

累加器用来对信息进行聚合，通常在向 Spark 传递函数时，比如使用 map() 函数或者用 filter() 传条件时，可以使用驱 动器程序中定义的变量，但是集群中运行的每个任务都会得到这些变量的一份新的副本， 更新这些副本的值也不会影响驱动器中的对应变量。 如果我们想实现所有分片处理时更新共享变量的功能，那么累加器可以实现我们想要的效果。

### 5.1.1 系统累加器

```
package com.atguigu

import org.apache.spark.{SparkConf, SparkContext}

object AccuTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("cp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val arr =  Array(1,2,3,4,5)
    val rdd = sc.makeRDD(arr)
    var sum = sc.accumulator(0)
    rdd.map{x=>
      sum += x
      x
    }.collect
    println(sum.value)
  }
}

```

累加器的用法如下所示。

通过在驱动器中调用SparkContext.accumulator(initialValue)方法，创建出存有初始值的累加器。返回值为 org.apache.spark.Accumulator[T] 对象，其中 T 是初始值 initialValue 的类型。Spark闭包里的执行器代码可以使用累加器的 += 方法(在Java中是 add)增加累加器的值。 驱动器程序可以调用累加器的value属性(在Java中使用value()或setValue())来访问累加器的值。 

注意：工作节点上的任务不能访问累加器的值。从这些任务的角度来看，累加器是一个只写变量。

对于要在行动操作中使用的累加器，Spark只会把每个任务对各累加器的修改应用一次。因此，如果想要一个无论在失败还是重复计 算时都绝对可靠的累加器，我们必须把它放在 foreach() 这样的行动操作中。转化操作中累加器可 能会发生不止一次更新

### 自定义累加器

自定义累加器类型的功能在1.X版本中就已经提供了，但是使用起来比较麻烦，在2.0版本后，累加器的易用性有了较大的改进，而且官方还提供了一个新的抽象类：AccumulatorV2来提供更加友好的自定义类型累加器的实现方式。实现自定义类型累加器需要继承AccumulatorV2并至少覆写下例中出现的方法，下面这个累加器可以用于在程序运行过程中收集一些文本类信息，最终以Set[String]的形式返回。

```
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

class LogAccumulator extends org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]] {
  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]]): Unit = {
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }

  }

  override def value: java.util.Set[String] = {
    java.util.Collections.unmodifiableSet(_logArray)
  }

  override def copy():org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }
}

// 过滤掉带字母的
object LogAccumulator {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("LogAccumulator").setMaster("local[*]")
    val sc=new SparkContext(conf)

    val accum = new LogAccumulator
    sc.register(accum, "logAccum")
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    for (v <- accum.value) print(v + "")
    println()
    sc.stop()
  }
}

```

自己写的

```
package com.atguigu

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

class logAccumulatorTest extends AccumulatorV2[String,java.util.Set[String]]{

  private val _logArr: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = _logArr.isEmpty

  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newlog = new  logAccumulatorTest
    newlog._logArr.addAll(_logArr)
    newlog
  }

  override def reset(): Unit = _logArr.clear()

  override def add(v: String): Unit = _logArr.add(v)

  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    _logArr.addAll(other.value)
  }

  override def value: util.Set[String] = _logArr
}
object logAcc{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("LogAccumulator").setMaster("local[*]")
    val sc=new SparkContext(conf)

    val logAcc = new logAccumulatorTest
    sc.register(logAcc, "logAccumlator")

    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter( line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if(!flag){
        logAcc.add(line)
      }
      flag
    }).map(_.toInt).reduce(_+_)

    println("sum:"+sum)
    for(v <- logAcc.value) println(v)
  }
}

```

遇到的错误

```
Scala编程中常见错误：Error:(24, 29) value foreach is not a member of java.util.Set[String]
问题:
在Scala编程开发中, 经常会出现类似如下的错误,
Error:(24, 29) value foreach is not a member of java.util.Set[String]
    for (key <- reducedList.keySet) {
                            ^
或
Error:(21, 22) value filter is not a member of java.util.ArrayList[myakka.messages.Word]
    for (wc: Word <- dataList) {
                     ^
解决方法:
因为reducedList是java.util.HashMap, 没有foreach方法, 所以需要将其转换为Scala的集合类型, 
因此需要在代码中加入如下内容(Scala支持与Java的隐式转换),
import scala.collection.JavaConversions._  
```

