package com.atguigu.gamll.rt.app

import java.util
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall.rt.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
import com.atguigu.gmall.rt.util.StartUpLog

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //消费kafka
    val inputDS = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //json字符串 => 一个对象 case class
    val startDS = inputDS.map {
      record =>
        val jsonString = record.value()

        val startupLog = JSON.parseObject(jsonString, classOf[StartUpLog])

        val datetime = new Date(startupLog.ts)
        val formattor = new SimpleDateFormat("yyyy-MM-dd HH")

        val datatimeStr = formattor.format(datetime)

        val datatimeArr = datatimeStr.split(" ")

        startupLog.logDate = datatimeArr(0)
        startupLog.logHour = datatimeArr(1)

        startupLog
    }

    //利用广播变量 把清单发给各个excutor 各个excutor根据清单进行比对过滤
    val filteredDS = startDS.transform {
      rdd =>
        val jedis = RedisUtil.getJedisClient
        val format = new SimpleDateFormat("yyyy-MM-dd")
        val today = format.format(new Date())
        val dauKey = "dau" + today
        val dauSet = jedis.smembers(dauKey)
        jedis.close()

        val dauBC = ssc.sparkContext.broadcast(dauSet)
        println("过滤前:" + rdd.count())

        val filteredRDD = rdd.filter { startup =>
          val dauSet = dauBC.value


          !dauSet.contains(startup.mid)
        }

        println("过滤后:" + filteredRDD.count())
        filteredRDD
    }

    //本批次自检去重
    //相同的mid保留第一条
    val groupMidDS = filteredDS.map(startuplog => (startuplog.mid, startuplog)).groupByKey()
    val filteredSefDS = groupMidDS.map { case (mid, startupItr) =>
      val topNlist = startupItr.toList.sortWith((start1, start2) =>
        start1.ts < start2.ts).take(1)
      topNlist(0)
    }

    /*
        //利用redis进行过滤 //反复连接redis可以优化
        startDS.filter {
          startup =>
            val jedis = RedisUtil.getJedisClient
            val dauKey = "dau" + startup.logDate
            val exists = jedis.sismember(dauKey, startup.mid)
            !exists
        }
    */
    filteredSefDS.cache()
    //把过滤后的数据写入redis(当日用户访问清单)
    filteredSefDS.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          startItr =>
            val client = RedisUtil.getJedisClient
            for (startup <- startItr) {
              //保存当日用户的访问清单
              //jedis type:set key:dau:[日期] value:mid
              val dauKey = "dau" + startup.logDate

              client.sadd(dauKey, startup.mid)
            }
            client.close()

        }
    }

    filteredSefDS.foreachRDD{
      rdd=>
        rdd.saveToPhoenix("GMALL2019_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }



    ssc.start()

    ssc.awaitTermination()

  }
}
