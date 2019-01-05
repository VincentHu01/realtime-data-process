package com.ai.kafka

import java.util.Properties
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.ai.utils.PropUtil

/**
  * Created by Jason on 2018/12/28.
  */

object ScalaKafkaProducer {

  private val prop:Properties = PropUtil.getProps("kafka.properties")

  def getProducer(): KafkaProducer[String, String] ={
    val brokers:String = "ip:9092,ip:9093".replaceAll("ip",prop.getProperty("HOST_IP"))
    //println("brokers: "+brokers)
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "sync")
    props.put("batch.num.messages", "1")
    props.put("queue.buffering.max.messages", "1000000")
    props.put("queue.enqueue.timeout.ms", "20000000")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer:KafkaProducer[String,String] = new KafkaProducer(props)
    return producer
  }

  def produce(): Unit = {
    val data = Array("a", "b", "c", "d", "e")
    val producer = getProducer()
    val topic = prop.getProperty("TOPIC")
    while (true) {
      val rnd: Int = (new Random).nextInt(5)
      val record: ProducerRecord[String, String] = new ProducerRecord(topic, data(rnd))
      producer.send(record)
      Thread.sleep(10)
    }
  }

  def main(args: Array[String]) {
    produce()
  }

}