package com.ai.spark

import java.util.Properties

import com.ai.utils.PropUtil
import org.apache.spark._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Jason on 2018/12/28.
  */
object SparkStreaming {

  private val prop = PropUtil.getProps("kafka.properties")

  private val brokers = "ip:9092,ip:9093,ip:9094".replaceAll("ip",prop.getProperty("HOST_IP"))

  def run(): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
    val ctx = new SparkContext(conf)
    ctx.setLogLevel("WARN")
    val ssc = new StreamingContext(ctx, Seconds(5))
    val topics=Array(prop.getProperty("TOPIC"))
    val kafkaParams=Map[String,Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val consumer = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    val messages = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      consumer
    )

    val lines=messages.map(record => record.value)
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
