### RDD中的函数传递（序列化问题）

在实际开发中我们往往需要自己定义一些对于RDD的操作，那么此时需要主要的是，初始化工作是在Driver端进行的，而实际运行程序是在Executor端进行的，这就涉及到了跨进程通信，是需要序列化的。下面我们看几个例子：

### 2.5.1 传递一个方法

```
package com.atguigu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class Search(qurey: String) extends java.io.Serializable{ // 必须加extends //java.io.Serializable 因为类初始化在Driver端  而在executor端确需要调用类中的方法
//因此涉及到了网络通信需要序列化
  def isMatch(s:String):Boolean={
      s.contains(qurey)
  }
  def getMatch1(rdd:RDD[String])={
    rdd.filter(isMatch)
  }
}
object serializeable {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
      val conf = new SparkConf().setMaster("local[*]").setAppName("serializeable")
      val sc = new SparkContext(conf)
    //2.创建一个RDD
      val rdd:RDD[String] = sc.parallelize(Array("hadoop", "spark","hive"))
    //3.创建一个Search对象
      val search = new Search("h")
    //4.运用第一个过滤函数并打印结果
      search.getMatch1(rdd).collect().foreach(println)
  }

}
```

注：在这个方法中所调用的方法isMatch()是定义在Search这个类中的，实际上调用的是this. isMatch()，this表示Search这个类的对象，程序在运行过程中需要将Search对象序列化以后传递到Executor端。

### 2.5.2 传递一个属性

```
package com.atguigu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
class Search(qurey: String) extends {
//class Search(qurey: String) extends java.io.Serializable{
  def isMatch(s:String):Boolean={
      s.contains(qurey)
  }
  def getMatch1(rdd:RDD[String])={
    rdd.filter(isMatch)
  }
  def getMatch2(rdd:RDD[String])={
    val q = qurey
    rdd.filter(x => x.contains(q))
  }
}
object serializeable {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
      val conf = new SparkConf().setMaster("local[*]").setAppName("serializeable")
      val sc = new SparkContext(conf)
    //2.创建一个RDD
      val rdd:RDD[String] = sc.parallelize(Array("hadoop", "spark","hive"))
    //3.创建一个Search对象
      val search = new Search("h")
    //4.运用第一个过滤函数并打印结果
      search.getMatch2(rdd).collect().foreach(println)
  }

}


```

注：在这个方法中所调用的方法query是定义在Search这个类中的字段，实际上调用的是this. query，this表示Search这个类的对象，程序在运行过程中需要将Search对象序列化以后传递到Executor端。

解决方案

1）使类继承scala.Serializable即可。

class Search() extends Serializable{...}

2）将类变量query赋值给局部变量