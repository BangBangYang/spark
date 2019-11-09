### reducByKey总结

在进行Spark开发算法时，最有用的一个函数就是reduceByKey。

reduceByKey的作用对像是(key, value)形式的rdd，而reduce有减少、压缩之意，reduceByKey的作用就是对相同key的数据进行处理，最终每个key只保留一条记录。

保留一条记录通常有两种结果。一种是只保留我们希望的信息，比如每个key出现的次数。第二种是把value聚合在一起形成列表，这样后续可以对value做进一步的操作，比如排序。

**常用方式举例**

比如现在我们有数据goodsSale：RDD[(String, String)]，两个字段分别是goodsid、单个订单中的销售额，现在我们需要统计每个goodsid的销售额。

我们只需要保留每个goodsid的累记销售额，可以使用如下语句来实现：

```jsx
val goodsSaleSum = goodsSale.reduceByKey((x,y) => x+y)
```

熟悉之后你可以使用更简洁的方式：

```undefined
val goodsSaleSum = goodsSale.reduceByKey(_+_)
```

reduceByKey会寻找相同key的数据，当找到这样的两条记录时会对其value(分别记为x,y)做`(x,y) => x+y`的处理，即只保留求和之后的数据作为value。反复执行这个操作直至每个key只留下一条记录。

现在假设goodsSaleSum还有一个字段类目id，即 RDD[(String, String, String)] 形式，三个字段分别是类目id、goodsid、总销量，现在我们要获得第个类目id下销量最高的一个商品。

上一步聚是保留value求和之后的数据，而这里其实我们只需要保留销量更高的那条记录。不过我们不能直接对RDD[(String, String, String)]类型的数据使用reduceByKey方法，因为这并不是一个(key, value)形式的数据，所以需要使用map方法转化一下类型。

```jsx
val catGmvTopGoods = goodsSaleSum.map(x => (x._1, (x._2, x._3)))

    .reduceByKey((x, y) => if (x._2.toDouble > y._2.toDouble) x else y)

    .map(x => (x._1, x._2._1, x._2._2)
```

再进一步，假设现在我们有一个任务：推荐5个销售额最高的类目，并为每个类目推荐一个销售额最高的商品，而我们的数据就是上述RDD[(String, String, String)类型的goodsSaleSum。

这需要两步，一是计算每个类目的销售额，这和举的第一个例子一样。二是找出每个类目下销量最高的商品，这和第二个例子一样。实际上，我们可以只实用一个reduceByKey就达到上面的目的。

```jsx
val catIdGmvTopGoods = goodsSaleSum.map(x => (x._1, (x._2, x._3, x._3)))

    .reduceByKey((x, y) => if (x._2 > y._2) (x._1, x._2, x._3+y._3) else (y._1, y._2, x._3+y._3))

    .map( x => (x._1, x._2._1, x._2._2, x._2._3)

    .sortBy(_._3, false)

    .take(5)
```

由于我们需要计算每个类目的总销售额，同时需要保留商品的销售额，所以先使用map增加一个字段用来记录类目的总销售额。这样一来，我们就可以使用reduceByKey同时完成前两个例子的操作。

剩下的就是进行排序并获取前5条记录。

**聚合方式举例**

上述的三个例子都是只保留需要的信息，但有时我们需要将value聚合在一起进行排序操作，比如对每个类目下的商品按销售额进行排序。

假设我们的数据是 RDD[(String, String, String)]，三个字段分别是类目id、goodsid、销售额。

若是使用sql，那我们直接用row_number函数就可以很简单的使用分类目排序这个任务。

但由于spark-sql占用的资源会比RDD多不少，在开发任务时并不建议使用spark-sql。

我们的方法是通过reduceByKey把商品聚合成一个List，然后对这个List进行排序，再使用flatMapValues摊平数据。

我们在使用reduceyByKey时会注意到，两个value聚合后的数据类型必须和之前一致。

所以在聚合商品时我们也需要保证这点，通常有两种方法，一是使用ListBuffer，即可变长度的List。二是使用String，以分隔符来区分商品和销售额。下面我们使用第一种方式完成这个任务。

```jsx
val catIdGoodsIdSorted = goodsGmvSum.map(x => (x._1, ListBuffer(x._2, x._3.toDouble)))

    .reduceByKey((x, y) => x++y)

    .flatMapValues( x => x.toList.sortBy(_._2).reverse.zipWithIndex)
```

上述zipWithIndex给列表增加一个字段，用来记录元素的位置信息。而flatMapValues可以把List的每个元素单独拆成一条记录，详细的说明可以参考我写的另一篇文章[Spark入门-常用函数汇总]([https://www.jianshu.com/p/5696ecacce38)

**小结**

我在本文中介绍了reduceByKey的三种作用：

1. 求和汇总
2. 获得每个key下value最大的记录
3. 聚合value形成一个List之后进行排序