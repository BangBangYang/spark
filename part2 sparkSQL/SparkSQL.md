# SparkSQL



## DataFrame

### 创建

在Spark SQL中SparkSession是创建DataFrame和执行SQL的入口，创建DataFrame有三种方式：通过Spark的数据源进行创建；从一个存在的RDD进行转换；还可以从Hive Table进行查询返回。

1）从Spark数据源进行创建  

（1）查看Spark数据源进行创建的文件格式

```
scala> spark.read.
csv   format   jdbc   json   load   option   options   orc   parquet   schema   table   text   textFile
```

（2）读取json文件创建DataFrame

      ```
people.json文件

{"name":"yangkun","age":23}
{"name":"amy"}
{"name":"mike","age":20}
{"name":"nike","age":22}
      ```

```
scala> val df = spark.read.json("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
```

（3）展示结果

```
scala> df.show
+----+-------+
| age|   name|
+----+-------+
|  23|yangkun|
|null|    amy|
|  20|   mike|
|  22|   nike|
+----+-------+
```

2）从RDD进行转换

pass

3）从Hive Table进行查询返回

pass

###  SQL风格语法(主要)  

1）创建一个DataFrame

```
scala> val df = spark.read.json("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
```

2）对DataFrame创建一个临时表

```
scala> df.createOrReplaceTempView("people")
```

3）通过SQL语句实现查询全表

 ```
scala> df.df.createOrReplaceTempView("people")

scala> spark.sql("select *from people").show
+----+-------+
| age|   name|
+----+-------+
|  23|yangkun|
|null|    amy|
|  20|   mike|
|  22|   nike|
+----+-------+
 ```

注意：临时表是Session范围内的，Session退出后，表就失效了。如果想应用范围内有效，可以使用全局表。注意使用全局表时需要全路径访问，如：global_temp.people

5）对于DataFrame创建一个全局表

```
scala> val df = spark.read.json("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]

scala> df.create
createGlobalTempView   createOrReplaceTempView   createTempView

scala> df.createGlobalTempView("people")

scala> spark.sql("SELECT * FROM global_temp.people").show()
+----+-------+
| age|   name|
+----+-------+
|  23|yangkun|
|null|    amy|
|  20|   mike|
|  22|   nike|
+----+-------+

```

SparkSession的临时表分为两种

全局临时表：作用于某个Spark应用程序的所有SparkSession会话 局部临时表：
作用于某个特定的SparkSession会话
如果同一个应用中不同的session需要重用一个临时表，那么不妨将该临时表注册为全局临时表，可以避免多余的IO，提高系统的执行效率，但是如果只是在某个session中使用，只需要注册局部临时表，可以避免不必要的内存占用

###   DSL风格语法(次要)  

1）创建一个DataFrame

```
scala> val df = spark.read.json("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
```

2）查看DataFrame的Schema信息

```
scala> df.printSchema
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
```

3）只查看”name”列数据

```
scala> df.select("name").show
+-------+
|   name|
+-------+
|yangkun|
|    amy|
|   mike|
|   nike|
+-------+
```

4）查看”name”列数据以及”age+1”数据

```
scala> df.select($"name", $"age" + 1).show()
+-------+---------+
|   name|(age + 1)|
+-------+---------+
|yangkun|       24|
|    amy|     null|
|   mike|       21|
|   nike|       23|
+-------+---------+
```

5）查看”age”大于”21”的数据

```

scala> df.filter($"age" > 21).show()
+---+-------+
|age|   name|
+---+-------+
| 23|yangkun|
| 22|   nike|
+---+-------+
```

6）按照”age”分组，查看数据条数

```
scala> df.groupBy("age").count().show()
+----+-----+                                                                    
| age|count|
+----+-----+
|  22|    1|
|null|    1|
|  23|    1|
|  20|    1|
+----+-----+
```

### RDD 转化为DateFrame

注意：如果需要RDD与DF或者DS之间操作，那么都需要引入 import spark.implicits._ **【spark不是包名，而是sparkSession对象的名称**】

前置条件：**导入隐式转换并创建一个RDD**

```
people.txt

yangkun,23
haha,22
bangbang,34
```

读入文件

```
scala> val peopleRDD = sc.textFile("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.txt")
peopleRDD: org.apache.spark.rdd.RDD[String] = /opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.txt MapPartitionsRDD[21] at textFile at <console>:24
```

1）通过手动确定转换

```
scala> peopleRDD.map{x=>val para = x.split(",");(para(0),para(1).toInt)}
res8: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[22] at map at <console>:27

scala> res8.collect
res10: Array[(String, Int)] = Array((yangkun,23), (haha,22), (bangbang,34))

scala> import spark.implicits._
import spark.implicits._

scala> res8.toDF("name","age")
res13: org.apache.spark.sql.DataFrame = [name: string, age: int]

scala> val df = res8.toDF("name","age")
df: org.apache.spark.sql.DataFrame = [name: string, age: int]

scala> df.show
+--------+---+
|    name|age|
+--------+---+
| yangkun| 23|
|    haha| 22|
|bangbang| 34|
+--------+---+

```

2）通过反射确定（需要用到样例类）

（1）创建一个样例类

```
scala> case class People(name:String, age:Int)
```

（2）根据样例类将RDD转换为DataFrame

```
scala> peopleRDD.map{x => val para = x.split(","); people(para(0),para(1).toInt)}.toDF().show
+--------+---+
|    name|age|
+--------+---+
| yangkun| 23|
|    haha| 22|
|bangbang| 34|
+--------+---+
```

###   DateFrame转换为RDD  

直接调用rdd即可

1）创建一个DataFrame

```
scala> val df = spark.read.json("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
```

2）将DataFrame转换为RDD

```
scala> val dfToRDD = df.rdd
dfToRDD: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[19] at rdd at <console>:29
```

3）打印RDD

```
scala> dfToRdd.collect
res6: Array[org.apache.spark.sql.Row] = Array([23,yangkun], [null,amy], [20,mike], [22,nike])
```



```
scala> val peopleRDD = sc.textFile("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.txt")
peopleRDD: org.apache.spark.rdd.RDD[String] = /opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.txt MapPartitionsRDD[44] at textFile at <console>:24

scala>  peopleRDD.map{x=>val para = x.split(",");(para(0),para(1))}
res28: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[45] at map at <console>:27

scala> import spark.implicits._
import spark.implicits._

scala> res28.toDF("name","age")
res29: org.apache.spark.sql.DataFrame = [name: string, age: string]

scala> res29.show
+--------+---+
|    name|age|
+--------+---+
| yangkun| 23|
|    haha| 22|
|bangbang| 34|
+--------+---+


scala> val df = res28
df: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[45] at map at <console>:27

scala> df.show
<console>:34: error: value show is not a member of org.apache.spark.rdd.RDD[(String, String)]
       df.show
          ^

scala> df = res29
<console>:35: error: reassignment to val
       df = res29
          ^

scala> val df = res29
df: org.apache.spark.sql.DataFrame = [name: string, age: string]

scala> df.show
+--------+---+
|    name|age|
+--------+---+
| yangkun| 23|
|    haha| 22|
|bangbang| 34|
+--------+---+


scala> df.rdd
res33: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[55] at rdd at <console>:36

scala> res33.map(_.)
anyNull   equals       getAs        getDate      getFloat     getList   getSeq      getStruct      hashCode   mkString   toSeq      
apply     fieldIndex   getBoolean   getDecimal   getInt       getLong   getShort    getTimestamp   isNullAt   schema     toString   
copy      get          getByte      getDouble    getJavaMap   getMap    getString   getValuesMap   length     size                  

scala> res33.map(_.getString(0))
res34: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[56] at map at <console>:38

scala> res33.map(_.getString(0)).collect
res35: Array[String] = Array(yangkun, haha, bangbang)

```

##   DataSet  

### 创建

1）创建一个样例类

```
scala> case class Person(name: String, age: Long)
```

2）创建DataSet

```
scala> val caseClassDs = Seq(Person("kk",22)).toDS
caseClassDs: org.apache.spark.sql.Dataset[Person] = [name: string, age: int]
```

###   RDD转换为DataSet  

SparkSQL能够自动将包含有case类的RDD转换成DataFrame，case类定义了table的结构，case类属性通过反射变成了表的列名。

1）创建一个RDD

```
scala> val peopleRDD = sc.textFile("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.txt")
peopleRDD: org.apache.spark.rdd.RDD[String] = /opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.text MapPartitionsRDD[1] at textFile at <console>:24
```

2）创建一个样例类

```
scala> case class Person("name":String,"age":Int)
<console>:1: error: identifier expected but string literal found.
case class Person("name":String,"age":Int)
```

3）将RDD转化为DataSet

```
peopleRDD.map(line => {val para = line.split(",");Person(para(0),para(1).trim.toInt)}).toDS()
```

###  DataSet转换为RDD  

调用rdd方法即可。

```
val ds = peopleRDD.map(line => {val para = line.split(",");Person(para(0),para(1).trim.toInt)}).toDS()

scala> personRdd.map(_.name).collect
res12: Array[String] = Array(yangkun, haha, bangbang)

scala> personRdd.map(_.age).collect
res13: Array[Int] = Array(23, 22, 34)
```

###    DataFrame转DataSet的互操作  

1. DataFrame转换为DataSet

```
scala> val df = spark.read.json("/opt/module/spark-2.1.1-bin-hadoop2.7/datas/people.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
```

2）创建一个样例类

```
scala> case class People(name:String,age:Long)
defined class People

```

3）将DateFrame转化为DataSet

```
scala> val ds = df.as[People]
ds: org.apache.spark.sql.Dataset[People] = [age: bigint, name: string]

scala> ds.show
+----+-------+
| age|   name|
+----+-------+
|  23|yangkun|
|null|    amy|
|  20|   mike|
|  22|   nike|
+----+-------+
```

###   DataSet转DataFrame  

这个很简单，因为只是把case class封装成Row

（1）导入隐式转换

```
import spark.implicits._
```

（2）转换

```
scala> ds.toDF
res15: org.apache.spark.sql.DataFrame = [age: bigint, name: string]

scala> res15.collect
res16: Array[org.apache.spark.sql.Row] = Array([23,yangkun], [null,amy], [20,mike], [22,nike])
```

