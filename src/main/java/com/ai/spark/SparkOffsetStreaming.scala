package com.ai.spark
import com.ai.utils.{KafkaOffsetsUtil, PropUtil}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.slf4j.LoggerFactory
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
/**
  * Created by Jason on 2019/1/12.
  */
object SparkOffsetStreaming {

  val logger = LoggerFactory.getLogger(SparkOffsetStreaming.getClass)
  val kafkaProps = PropUtil.getProps("kafka.properties")
  val brokers:String = "ip:9092,ip:9093".replaceAll("ip",kafkaProps.getProperty("HOST_IP"))
  val topics = kafkaProps.getProperty("TOPIC")
  val groupId = kafkaProps.getProperty("GROUP_ID")

  val kafkaOffsetsUtil =  new KafkaOffsetsUtil()

  val zkServers = "ip:2181,ip:2182".replaceAll("ip",kafkaProps.getProperty("HOST_IP"))
  val zkClient = new ZkClient(zkServers, 60000, 60000, new ZkSerializer {
    override def serialize(data: Object): Array[Byte] = {
      try {
        return data.toString().getBytes("UTF-8")
      } catch {
        case e: ZkMarshallingError => return null
      }
    }
    override def deserialize(bytes: Array[Byte]): Object = {
      try {
        return new String(bytes, "UTF-8")
      } catch {
        case e: ZkMarshallingError => return null
      }
    }
  })

  def createContext(checkpointDirectory: String): StreamingContext = {

    val split_rdd_time = 5

    val sparkConf = new SparkConf()
      .setAppName("SendSampleKafkaDataToApple").setMaster("local[*]")
      .set("spark.app.id", "streaming_kafka")
    val ctx = new SparkContext(sparkConf)
    ctx.setLogLevel("WARN")

    val ssc = new StreamingContext(ctx, Seconds(split_rdd_time))

    //ssc.checkpoint(checkpointDirectory)

    val topicsSet: Set[String] = topics.split(",").toSet

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupId,
      "key.deserializer"-> classOf[StringDeserializer],//"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"-> classOf[StringDeserializer],//"org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val offsetList = kafkaOffsetsUtil.get_latest_offset(groupId)
    println("offsetList："+offsetList)

    if(offsetList.size == 0){
      println("consume from offset 0")
      val consumer = ConsumerStrategies.Subscribe[String, String](topicsSet,kafkaParams)
      val rdd = KafkaUtils.createDirectStream(ssc,PreferConsistent,consumer)
      rdd.foreachRDD(rdd=>{
        println(rdd.count())
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          println(o.topic, o.partition, o.fromOffset, o.untilOffset)
          kafkaOffsetsUtil.insert_offset(List[Any](groupId,o.topic, o.partition, o.fromOffset, o.untilOffset))
        }
      })
      val counts = rdd.map(s=>(s.value(),1)).reduceByKey(_ + _)
      counts.print()
    }else {
      //val offsetList = List((topics,0, 79195L),(topics,1, 79573L))
      val offsetList = kafkaOffsetsUtil.get_latest_offset(groupId)

      println("consume from offsetList："+offsetList)
      val fromOffsets : Map[TopicPartition, Long] = setFromOffsets(offsetList)

      //val consumer = ConsumerStrategies.Subscribe[String, String](topicsSet,kafkaParams,fromOffsets)
      val consumer2 = ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList,kafkaParams,fromOffsets)

      val rdd = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,consumer2)

      rdd.foreachRDD(mess => {
        println("count: "+mess.count())
        val offsetsList = mess.asInstanceOf[HasOffsetRanges].offsetRanges
        mess.foreachPartition(lines => {
          val o: OffsetRange = offsetsList(TaskContext.get.partitionId)
          //logger.info(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")
          println(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")
          try{
            kafkaOffsetsUtil.insert_offset(List[Any](groupId,o.topic, o.partition, o.fromOffset, o.untilOffset))
          }catch {
            case e :Exception =>println(e)
          }
        })
      })
      val counts = rdd.map(s=>(s.value(),1)).reduceByKey(_ + _)
      counts.print()
    } //end else
    ssc
  }

  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicPartition, Long] = {
    var fromOffsets: Map[TopicPartition, Long] = Map()
    for (offset <- list) {
      val tp =new  TopicPartition(offset._1.toString, offset._2.toInt)
      fromOffsets += (tp -> offset._3.toLong)
    }
    fromOffsets
  }

  def run(): Unit ={
    val scc = createContext("")
    scc.start()
    scc.awaitTermination()
  }

  def main(args: Array[String]) {
    run()
  }


}
