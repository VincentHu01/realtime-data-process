package com.ai.utils
import scalikejdbc._
import scalikejdbc.config._

/**
  * Created by Jason on 2019/1/7.
  */
class KafkaOffsetsUtil {


  DBs.setup()

  def insert_offset(values:List[Any])={
    try{
      DB.autoCommit  {implicit  session=>
        sql"INSERT INTO kafka_offsets(group_id,topic,prtition,from_offset,end_offset)VALUES(?,?,?,?,?)"
          .bind(values(0), values(1), values(2), values(3), values(4)).update.apply()
      }
    }catch {
      case ex: Exception => println(ex)
    }

  }

  /*def get_latest_offset(group_id:String): Unit ={
    val offsets = DB.readOnly{implicit session=>
    sql"SELECT * FROM kafka_offsets WHERE group_id = group_id".map(rs=>(
      rs.string("group_id"),
      rs.string("topic"),
      rs.string("prtition"),
      rs.string("from_offset"),
      rs.string("end_offset")
      )).list.apply()
    }
    offsets.toList
  }*/

  def get_latest_offset(group_id:String): List[(String, Int, Long)] ={
    val offsets = DB.readOnly {implicit session=>
      //sql"SELECT * FROM kafka_offsets WHERE group_id=? ORDER BY end_offset DESC limit 2".bind(group_id).map(
      sql"SELECT *  FROM kafka_offsets WHERE group_id=? AND from_offset in (SELECT max(from_offset) from kafka_offsets GROUP BY topic, prtition)"
        .bind(group_id).map(
        s=>(s.string(2),s.int(3),s.long(4))
      ).list.apply
    }
    offsets //.head.asInstanceOf[Tuple5[Any,Any,Any,Any,Any]]
  }

 def close(): Unit ={
   DBs.closeAll()
 }


}
