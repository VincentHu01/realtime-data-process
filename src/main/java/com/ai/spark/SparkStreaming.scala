package com.ai.spark

import org.apache.spark._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Jason on 2018/12/28.
  */
object SparkStreaming {

  private val brokers = "192.168.65.130:9092,192.168.65.130:9092,192.168.65.130:9092"

  def run(): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
    val ctx = new SparkContext(conf)
    ctx.setLogLevel("WARN")
    val ssc = new StreamingContext(ctx, Seconds(5))

    val topics=Array("test")
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
