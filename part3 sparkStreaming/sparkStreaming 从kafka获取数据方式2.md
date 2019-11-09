### sparkStreaming 从kafka获取数据方式2

### KafkaUtils.createDirectStream

区别Receiver接收数据，这种方式定期地从kafka的topic+partition中查询最新的偏移量，再根据偏移量范围在每个batch里面处理数据，使用的是kafka的简单消费者api 
优点: 
A、 简化并行，不需要多个kafka输入流，该方法将会创建和kafka分区一样的rdd个数，而且会从kafka并行读取。 
B、高效，这种方式并不需要WAL，WAL模式需要对数据复制两次，第一次是被kafka复制，另一次是写到wal中 
C、恰好一次语义(Exactly-once-semantics)，传统的读取kafka数据是通过kafka高层次api把偏移量写入zookeeper中，存在数据丢失的可能性是zookeeper中和ssc的偏移量不一致。EOS通过实现kafka低层次api，偏移量仅仅被ssc保存在checkpoint中，消除了zk和ssc偏移量不一致的问题。缺点是无法使用基于zookeeper的kafka监控工具



案例：创建两个主题source和target，从sourcce端生产数据，通过sparkStreaming工具采集并写入到target主题



```
package com.bupt.sparkStreaming


import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSource2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ks")
    val ssc = new StreamingContext(conf, Seconds(5))
    //kafka参数
    val brokers = "hadoop101:9092"
    val zookeeper="hadoop101:2181,hadoop102:2181,hadoop103:2181"
    val sourceTopic = "source"
    val targetTopic = "target"
    val consumerId = "test"
    //封装kafka参数
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> consumerId
    )

    val kafkaDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(sourceTopic))
    val result = kafkaDStream.flatMap(t => t._2.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.print()

    kafkaDStream.foreachRDD{ rdd =>
      rdd.foreachPartition{ rddPar =>
          //写出到kafka(targetTopic)
          val value = rddPar.map(x => x._2)
          //创建生产者
          val kafkaPool = KafkaPool(brokers)
          val kafkaConn = kafkaPool.borrowObject()
          //生产者发送数据
          for(item <- rddPar){
            kafkaConn.send(targetTopic, item._1,item._2)
          }
          //关闭生产者
        kafkaPool.returnObject(kafkaConn)
      }

    }
/*    var offsetRanges = Array[OffsetRange]()
    kafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }*/

    ssc.start()
    ssc.awaitTermination()
  }
}

```



```
package com.bupt.sparkStreaming

import java.util.Properties

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BaseObject, BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer



class KafkaProxy(brokers: String) {

  //
  private val properties = new Properties()
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val kafkaConn = new KafkaProducer[String, String](properties)

  def send(topic: String, key: String, value: String): Unit = {
    kafkaConn.send(new ProducerRecord[String, String](topic, key, value))
  }

  def close(): Unit = {
    kafkaConn.close()
  }
}

class KafkaProxyFactory(brokers: String) extends BasePooledObjectFactory[KafkaProxy]{
  //创建实例
  override def create(): KafkaProxy = new KafkaProxy(brokers)
 //将池中对象封装
  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](t)
}
object KafkaPool {
  //声明池对象
  var kafkaPool: GenericObjectPool[KafkaProxy] = null

  def apply(brokers:String): GenericObjectPool[KafkaProxy]={
    if(kafkaPool == null){
      KafkaPool.synchronized{
        if(kafkaPool == null)
          kafkaPool = new GenericObjectPool(new KafkaProxyFactory(brokers))
      }
    }
    kafkaPool
  }
}

```

