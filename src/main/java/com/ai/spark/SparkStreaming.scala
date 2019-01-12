package com.ai.spark

import java.util.{Arrays, Properties}

import com.ai.utils.PropUtil
import com.google.common.primitives.Bytes
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import kafka.utils.ZkUtils
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.common.TopicPartition
import org.spark_project.jetty.server.HttpConfiguration.ConnectionFactory


/**
  * Created by Jason on 2018/12/28.
  */
object SparkStreaming {

  private val prop = PropUtil.getProps("kafka.properties")

  def run(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ctx = new SparkContext(conf)
    ctx.setLogLevel("WARN")
    val ssc = new StreamingContext(ctx, Seconds(15))
    //var zookeeper = "ip:2181,ip:2182".replaceAll("ip",prop.getProperty("HOST_IP"))
    val brokers:String = "ip:9092,ip:9093".replaceAll("ip",prop.getProperty("HOST_IP"))
    println("brokers: "+ brokers)
    val groupId = prop.getProperty("GROUP_ID")

    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupId,
      "enable.auto.commit"-> (false: java.lang.Boolean),
      "auto.offset.reset" -> "latest",
      "auto.commit.interval.ms"->"1000",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer"
    )

    val topics=Array(prop.getProperty("TOPIC"))
    println("topics: ")
    topics.foreach(println)

    val consumer = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    val messages = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      consumer
    )
    //messages.foreachRDD(rdd=>println(rdd.count()))

    val lines = messages.map(record=> record.value)
    val wordCounts = lines.map(x => {
      (x,1)
    }).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit ={
    run()
  }

}
