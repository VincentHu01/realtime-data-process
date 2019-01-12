package com.ai.utils

import scalikejdbc._
import scalikejdbc.config._

/**
  * Created by Jason on 2019/1/7.
  */
object DButil {

  DBs.setup()

 def insert_offset(values:List[Any])={

   DB.autoCommit  {implicit  session=>
     sql"INSERT INTO kafka_offsets(group_id,topic,prtition,from_offset,end_offset)VALUES(?,?,?,?,?)"
       .bind(values(0), values(1), values(2), values(3), values(4)).update.apply()
   }
   }

  DBs.closeAll()

}
