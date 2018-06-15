package real_time

import java.util.UUID

import common.UserTracksParams
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import utils.JsonUtil
import utils.connection.{HBaseUtil, KafkaUtil}

import scala.util.Try

/**
  * Description:
  * @author YKL on 2018/5/20
  * @version 1.0
  * spark:梦想开始的地方
  */
object RealTimeSave2Hbase {

  def getKafkaStreamingRDD(streamingContext: StreamingContext, topic: Array[String], consumerGroup: String): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtil.getStreamByKafka(streamingContext, topic, consumerGroup)
  }

  /**
    * save UserTracks into hbase
    * @param streamingRDD
    * @param tableName
    * @param columnFamily
    */
  def saveRDD2HBase(streamingRDD: InputDStream[ConsumerRecord[String, String]], tableName: String, columnFamily: String): Unit = {
    streamingRDD.map(line => {

      val jsonParser = new JSONParser()
      val message: JSONObject = jsonParser.parse(line.value()).asInstanceOf[JSONObject]
      val jsonKey = message.keySet()
      val iter = jsonKey.iterator()
      val put = new Put(Bytes.toBytes(UUID.randomUUID().toString + "-" + message.get("time").toString))
      while (iter.hasNext) {
        val field = iter.next()
        val value = message.get(field).toString
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field), Bytes.toBytes(value))
      }
      put
    }).foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        /** 批量写入Hbase **/
        val jobConf = new JobConf(HBaseUtil.getHBaseConf())
        jobConf.set("zookeeper.znode.parent", "/hbase")
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        val table = new HTable(jobConf, TableName.valueOf(tableName))
        import scala.collection.JavaConversions._
        table.put(seqAsJavaList(iterator.toSeq))
      })
    })
  }

  def saveRDD2UserLoginTime(formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)], tableName: String, columnFamily: String): Unit = {
    formattedRDD.foreachRDD(userTracksRDD => {
      //更新用户标识表
      userTracksRDD.foreachPartition(iter => {
        try {
          //获取HBase连接,分区创建一个连接，分区不跨节点，不需要序列化
          val hbaseConf = HBaseUtil.getHBaseConnection()
          iter.foreach(line => {
            val newUserTable = TableName.valueOf(tableName)
            //获取表连接
            val table = hbaseConf.getTable(newUserTable)

            var userFlag = ""
            var isEmptyImei = false
            var isEmptyMeid = false
            if (line._5 != null) {
              isEmptyImei = line._5.isEmpty
            }
            if (line._6 != null) {
              isEmptyMeid = line._6.isEmpty
            }

            isEmptyImei match {
              case true if isEmptyMeid => userFlag = "nobody"
              case true if !isEmptyMeid => userFlag = line._6
              case false if !isEmptyMeid => userFlag = line._5 + "|" + line._6
              case false if isEmptyMeid => userFlag = line._5
            }

            val get = new Get(Bytes.toBytes(userFlag))
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("first_time"))
            val result = table.get(get)
            val put = new Put(Bytes.toBytes(userFlag))

            //记录用户第一次登录时间和最后一次登录时间
            if (result.isEmpty) {
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("first_time"), Bytes.toBytes(line._4))
              Try(table.put(put)).getOrElse(table.close()) //将数据写入HBase，若出错关闭table
              //分区数据写入HBase后关闭连接
              table.close()
            } else {
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("last_time"), Bytes.toBytes(line._4))
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

  def formatUserTracksRDD(finalStreamingRDD: InputDStream[ConsumerRecord[String, String]]): DStream[(String, String, String, String, String, String, String, String, String, String, String)] = {
    val formattedRDD = finalStreamingRDD
      .map(line => {
        val infoMap = JsonUtil.json2Map(line.value())
        var city = ""
        val uid = infoMap.getOrElse(UserTracksParams.UID, UserTracksParams.EMPTY).toString
        val path = infoMap.getOrElse(UserTracksParams.PATH, UserTracksParams.EMPTY).toString
        val device = infoMap.getOrElse(UserTracksParams.DEVICE, UserTracksParams.EMPTY).toString
        val gid = infoMap.getOrElse(UserTracksParams.GID, UserTracksParams.EMPTY).toString
        val os = infoMap.getOrElse(UserTracksParams.OS, UserTracksParams.EMPTY).toString
        val model = infoMap.getOrElse(UserTracksParams.MODEL, UserTracksParams.EMPTY).toString
        val channel = infoMap.getOrElse(UserTracksParams.CHANNEL, UserTracksParams.EMPTY).toString
        val lang = infoMap.getOrElse(UserTracksParams.LANG, UserTracksParams.EMPTY).toString
        val ip = infoMap.getOrElse(UserTracksParams.IP, UserTracksParams.EMPTY).toString
        val time = infoMap.getOrElse(UserTracksParams.TIME, UserTracksParams.EMPTY).toString
        val area = infoMap.getOrElse(UserTracksParams.AREA, UserTracksParams.EMPTY).toString
        if (area != null && !area.equals(UserTracksParams.EMPTY)) {
          val areaSplit = area.split(" ")
          if (areaSplit.length > 3) {
            city = areaSplit(2)
          }
        }
        //1-uid, 2-path, 3-ip, 4-time, 5-device, 6-gid, 7-os, 8-model, 9-channel, 10-lang, 11-city
        (uid, path, ip, time, device, gid, os, model, channel, lang, city)
      })
    formattedRDD.repartition(8)
  }

  def formatSearchRDD(searchStreamingRDD: InputDStream[ConsumerRecord[String, String]]): DStream[(String, String, String, String, String, String)] = {
    val formattedRDD = searchStreamingRDD
      .map(line => {

        //创建json解析器
        val jsonParser = new JSONParser()
        //将string转化为jsonObject
        val message: JSONObject = jsonParser.parse(line.value()).asInstanceOf[JSONObject]

        var uid = ""
        var imei = ""
        var meid = ""
        var _type = ""
        var time = ""
        var key = ""

        if (message.containsKey("uid")) {
          uid = message.getAsString("uid")
        }
        if (message.containsKey("imei")) {
          imei = message.getAsString("imei")
        }
        if (message.containsKey("meid")) {
          meid = message.getAsString("meid")
        }
        if (message.containsKey("type")) {
          _type = message.getAsString("type")
        }
        if (message.containsKey("time")) {
          time = message.getAsString("time")
        }
        if (message.containsKey("key")) {
          key = message.getAsString("key")
        }

        (uid, time, imei, meid, _type, key)
      })
    formattedRDD
  }


}
