package real_time

import java.util.UUID

import common.CommonParams
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.connection.{HBaseUtil, KafkaUtil}

import scala.util.Try

/**
  * Description: 
  *
  * @author YKL on 2018/5/20.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object RealTimeSave {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RealTimeSave")
    //.setMaster("yarn-cluster")
    val ssc = new StreamingContext(conf, Seconds(10));

    val trialStreamingRDD = KafkaUtil.getStreamByKafka(ssc, CommonParams.TRIALTOPIC, CommonParams.CONSUMERGROUP)

    val finalStreamingRDD = KafkaUtil.getStreamByKafka(ssc, CommonParams.FINALTOPIC, CommonParams.CONSUMERGROUP)

    SaveRDD2Hbase(trialStreamingRDD, CommonParams.TRIALTABLENAME, CommonParams.TRIALCOLUMNFAMILY)

    SaveRDD2Hbase(finalStreamingRDD, CommonParams.FINALTABLENAME, CommonParams.FINALCOLUMNFAMILY)

    val formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)] = FormatRDD(finalStreamingRDD)

    Save2UserLoginTime(formattedRDD)

    ssc.start()
    ssc.awaitTermination()
  }

  private def SaveRDD2Hbase(finalStreamingRDD: InputDStream[ConsumerRecord[String, String]], tableName: String, columnFamily: String) = {
    finalStreamingRDD.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        try {
          //获取HBase连接,分区创建一个连接，分区不跨节点，不需要序列化
          val hbaseConf = HBaseUtil.getHBaseConnection()
          //创建json解析器
          val jsonParser = new JSONParser()
          iter.foreach(line => {
            val userTable = TableName.valueOf(tableName)
            //获取表连接
            val table = hbaseConf.getTable(userTable)
            //将string转化为jsonObject
            val message: JSONObject = jsonParser.parse(line.value()).asInstanceOf[JSONObject]
            //            val message: JSONObject = jsonParser.parse(jsonObj.getAsString("message")).asInstanceOf[JSONObject]
            //获取所有键
            val jsonKey = message.keySet()

            val iter = jsonKey.iterator()

            val put = new Put(Bytes.toBytes(UUID.randomUUID().toString + "-" + message.get("time").toString))
            while (iter.hasNext) {
              val field = iter.next()
              val value = message.get(field).toString
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field), Bytes.toBytes(value))
              Try(table.put(put)).getOrElse(table.close()) //将数据写入HBase，若出错关闭table
            }
            //分区数据写入HBase后关闭连接
            table.close()
          })
          hbaseConf.close()
        } catch {
          case e: Exception => println("写入HBase失败，{%s}", e.getMessage)
        }
      })
    })
  }

  private def Save2UserLoginTime(formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    formattedRDD.foreachRDD(userTracksRDD => {
      //更新用户标识表
      userTracksRDD.foreachPartition(iter => {
        try {
          //获取HBase连接,分区创建一个连接，分区不跨节点，不需要序列化
          val hbaseConf = HBaseUtil.getHBaseConnection()
          iter.foreach(line => {
            val newUserTable = TableName.valueOf("FinalUserLoginTime")
            //获取表连接
            val table = hbaseConf.getTable(newUserTable)

            val get = new Get(Bytes.toBytes(line._5 + "|" + line._6))
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("first_time"))
            val result = table.get(get)
            val put = new Put(Bytes.toBytes(line._5 + "|" + line._6))

            //记录用户第一次登录时间和最后一次登录时间
            if (result.isEmpty) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("first_time"), Bytes.toBytes(line._4))
              Try(table.put(put)).getOrElse(table.close()) //将数据写入HBase，若出错关闭table
              //分区数据写入HBase后关闭连接
              table.close()
            } else {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("last_time"), Bytes.toBytes(line._4))
              Try(table.put(put)).getOrElse(table.close()) //将数据写入HBase，若出错关闭table
              //分区数据写入HBase后关闭连接
              table.close()
            }
          })
          hbaseConf.close()
        } catch {
          case e: Exception => println("写入HBase失败，{%s}", e.getMessage)
        }
      })
    })
  }

  private def FormatRDD(finalStreamingRDD: InputDStream[ConsumerRecord[String, String]]) = {
    val formattedRDD = finalStreamingRDD
      .map(line => {

        //创建json解析器
        val jsonParser = new JSONParser()
        //将string转化为jsonObject
        val message: JSONObject = jsonParser.parse(line.value()).asInstanceOf[JSONObject]
        //        val message: JSONObject = jsonParser.parse(jsonObj.getAsString("message")).asInstanceOf[JSONObject]

        val uid = message.getAsString("uid")
        val path = message.getAsString("path")
        val ip = message.getAsString("ip")
        val time = message.getAsString("time")
        val imei = message.getAsString("imei")
        val meid = message.getAsString("meid")
        val os = message.getAsString("os")
        val model = message.getAsString("model")
        val channel = message.getAsString("channel")
        val lang = message.getAsString("lang")
        val location = message.getAsString("location")
        (uid, path, ip, time, imei, meid, os, model, channel, lang, location)
      })
    formattedRDD
  }
}
