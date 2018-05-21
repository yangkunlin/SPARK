package off_line.service

import java.util.{Calendar, UUID}

import common.CommonParams
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.connection.HBaseUtil
import utils.DateUtil

import scala.util.Try

/**
  * @author YKL on 2018/4/11.
  * @version 1.0
  *          spark： get source data from hbase and insert result data into hbase
  *          梦想开始的地方
  */
object HbaseServiceImpl {

  case class UserTrack(uid: String, path: String, ip: String, time: String, imei: String, meid: String, os: String, model: String, channel: String, lang: String, location: String)

  case class UserLoginTime(key: String, first_time: String, last_time: String)

  //用户轨迹原始数据
  val USERTRACKS_TABLE = CommonParams.FINALTABLENAME
  val USERTRACKS_FAMILYCOLUMN = CommonParams.FINALCOLUMNFAMILY

  //用户登陆时间数据
  val USERLOGINTIME_TABLE = "UserLoginTime"
  val USERLOGINTIME_FAMILYCOLUMN = "info"

  //计算结果数据
  val RESULT_TABLE = "Result"
  val RESULT_FAMILYCOLUMN = "info"

  /**
    * @param spark
    * @return DataFrame of UserLoginTime that was saved in hbase
    */
  def getUserLoginTime(spark: SparkSession): DataFrame = {

    val hbaseConf = HBaseUtil.getHBaseConf()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, USERLOGINTIME_TABLE)

    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    import spark.implicits._
    val dataDF = hbaseRDD.map({ case (_, result) =>
      val key = Bytes.toString(result.getRow)
      val first_time = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "first_time".getBytes))
      val last_time = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "last_time".getBytes))
      UserLoginTime(key, first_time, last_time)
    }).toDF()

    dataDF.createOrReplaceTempView("data")

    val (startTime: Long, endTime: Long) = getOneDayTime

    val userLoginTimeRDD = spark.sql("SELECT * FROM data")
    userLoginTimeRDD

  }

  /**
    * @param spark
    * @return DataFrame of UserTracks that was saved in hbase
    */
  def getUserTracks(spark: SparkSession): DataFrame = {

    val hbaseConf = HBaseUtil.getHBaseConf()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, USERTRACKS_TABLE)
    //从hbase中读取RDD
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    import spark.implicits._
    val dataDF = hbaseRDD.map({ case (_, result) =>
      //      val key = Bytes.toString(result.getRow)
      val uid = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "uid".getBytes))
      val path = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "path".getBytes))
      val ip = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "ip".getBytes))
      val time = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "time".getBytes))
      val imei = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "imei".getBytes))
      val meid = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "meid".getBytes))
      val os = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "os".getBytes))
      val model = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "model".getBytes))
      val channel = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "channel".getBytes))
      val lang = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "lang".getBytes))
      val location = Bytes.toString(result.getValue(USERTRACKS_FAMILYCOLUMN.getBytes, "location".getBytes))
      UserTrack(uid, path, ip, time, imei, meid, os, model, channel, lang, location)
    }).toDF()

    dataDF
  }

  /**
    * @param spark
    * @return data of one day:DataFrame
    */
  def getUserTracksOfOneDay(spark: SparkSession): DataFrame = {

    val dataDF = this.getUserTracks(spark)
    dataDF.createOrReplaceTempView("data")

    val (startTime: Long, endTime: Long) = getOneDayTime

    val oneDayDF = spark.sql("SELECT * FROM data WHERE time BETWEEN " + startTime + " AND " + endTime)
    oneDayDF

  }

  /**
    * @param spark
    * @return data of one week:DataFrame
    */
  def getUserTracksOfOneWeek(spark: SparkSession): DataFrame = {

    val dataDF = this.getUserTracks(spark)
    dataDF.createOrReplaceTempView("data")

    val (startTime: Long, endTime: Long) = getOneWeekTime
    val oneWeekDF = spark.sql("SELECT * FROM data WHERE time BETWEEN " + startTime + " AND " + endTime)
    oneWeekDF

  }

  /**
    * @param spark
    * @return data of one month:DataFrame
    */
  def getUserTracksOfOneMonth(spark: SparkSession): DataFrame = {

    val dataDF = this.getUserTracks(spark)
    dataDF.createOrReplaceTempView("data")

    val (endTime: Long, startTime: Long) = getOneMontTime


    val oneMonthDF = spark.sql("SELECT * FROM data WHERE time BETWEEN " + startTime + " AND " + endTime)
    oneMonthDF
  }

  private def getOneDayTime = {
    val startTime = DateUtil.getMillisecondsYMD(DateUtil.getYesterday())
    val endTime = startTime + 86400000
    (startTime, endTime)
  }

  private def getOneWeekTime = {
    val startTime = DateUtil.getMillisecondsYMD(DateUtil.getDateNow())
    val endTime = startTime - 604800000
    (startTime, endTime)
  }

  private def getOneMontTime = {
    //获取前月的第一天
    val cal_1 = Calendar.getInstance()
    cal_1.add(Calendar.MONTH, -1)
    cal_1.set(Calendar.DAY_OF_MONTH, 1) //设置为1号,当前日期既为前一个月第一天
    cal_1.set(Calendar.HOUR_OF_DAY, 0)
    cal_1.set(Calendar.MINUTE, 0)
    cal_1.set(Calendar.SECOND, 0)
    cal_1.set(Calendar.MILLISECOND, 0)

    //获取前月的最后一天
    val cal_2 = Calendar.getInstance()
    cal_2.set(Calendar.DAY_OF_MONTH, 1) //设置为1号,当前日期既为本月第一天
    cal_2.set(Calendar.HOUR_OF_DAY, 0)
    cal_2.set(Calendar.MINUTE, 0)
    cal_2.set(Calendar.SECOND, 0)
    cal_2.set(Calendar.MILLISECOND, 0)

    val endTime = cal_2.getTimeInMillis
    //cal_2.getTimeInMillis
    val startTime = cal_1.getTimeInMillis
    (endTime, startTime)
  }

  def setResult(_type: String, _time: String, _result: String): Unit = {

    //获取HBase连接
    val hbaseConf = HBaseUtil.getHBaseConnection()
    val userTable = TableName.valueOf(RESULT_TABLE)
    //获取表连接
    val table = hbaseConf.getTable(userTable)

    val put = new Put(Bytes.toBytes(UUID.randomUUID().toString))

    put.addColumn(Bytes.toBytes(RESULT_FAMILYCOLUMN), Bytes.toBytes("type"), Bytes.toBytes(_type))
    put.addColumn(Bytes.toBytes(RESULT_FAMILYCOLUMN), Bytes.toBytes("time"), Bytes.toBytes(_time))
    put.addColumn(Bytes.toBytes(RESULT_FAMILYCOLUMN), Bytes.toBytes("result"), Bytes.toBytes(_result))
    Try(table.put(put)).getOrElse(table.close()) //将数据写入HBase，若出错关闭table

    table.close()
    hbaseConf.close()
  }

}
