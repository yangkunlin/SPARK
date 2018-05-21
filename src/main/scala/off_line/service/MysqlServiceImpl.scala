package off_line.service

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.connection.MysqlUtil
import utils.DateUtil

/**
  * @author YKL on 2018/4/12.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object MysqlServiceImpl {

  val TABLENAME: List[String] = List("vip_order", "coin_order", "coin_bill")

  def getVipOrder(spark: SparkSession): DataFrame = {

    val vipOrderDF = MysqlUtil.getMysqlDF(spark, TABLENAME(0))

    vipOrderDF
  }

  def getCoinOrder(spark: SparkSession): DataFrame = {

    val coinOrderDF = MysqlUtil.getMysqlDF(spark, TABLENAME(1))

    coinOrderDF
  }

  def getCoinBill(spark: SparkSession): DataFrame = {

    val coinBillDF = MysqlUtil.getMysqlDF(spark, TABLENAME(2))

    coinBillDF
  }

  def getOneDayVORDD(spark: SparkSession): RDD[(Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneDayTime

    val jdbcRDD = getVipOrder(spark).select("price", "payFinishTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null) false
        else true
      })
      .map(row => {
      (row.get(0), DateUtil.getMillisecondsYMDHMS(row.get(1).toString))
    })
      .filter(tuple => {
        if (tuple._2 > startTime && tuple._2 < endTime) true
        else false
      })

    jdbcRDD
  }

  def getOneWeekVORDD(spark: SparkSession): RDD[(Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneWeekTime

    val jdbcRDD = getVipOrder(spark).select("price", "payFinishTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null) false
        else true
      })
      .map(row => {
        (row.get(0), DateUtil.getMillisecondsYMDHMS(row.get(1).toString))
      })
      .filter(tuple => {
        if (tuple._2 > startTime && tuple._2 < endTime) true
        else false
      })

    jdbcRDD
  }

  def getOneMonthVORDD(spark: SparkSession): RDD[(Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneMonthTime

    val jdbcRDD = getVipOrder(spark).select("price", "payFinishTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null) false
        else true
      })
      .map(row => {
        (row.get(0), DateUtil.getMillisecondsYMDHMS(row.get(1).toString))
      })
      .filter(tuple => {
        if (tuple._2 > startTime && tuple._2 < endTime) true
        else false
      })

    jdbcRDD
  }

  def getOneDayCORDD(spark: SparkSession): RDD[(Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneDayTime

    val jdbcRDD = getCoinOrder(spark).select("price", "payFinishTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null) false
        else true
      })
      .map(row => {
        (row.get(0), DateUtil.getMillisecondsYMDHMS(row.get(1).toString))
      })
      .filter(tuple => {
        if (tuple._2 > startTime && tuple._2 < endTime) true
        else false
      })

    jdbcRDD
  }

  def getOneWeekCORDD(spark: SparkSession): RDD[(Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneWeekTime

    val jdbcRDD = getCoinOrder(spark).select("price", "payFinishTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null) false
        else true
      })
      .map(row => {
        (row.get(0), DateUtil.getMillisecondsYMDHMS(row.get(1).toString))
      })
      .filter(tuple => {
        if (tuple._2 > startTime && tuple._2 < endTime) true
        else false
      })

    jdbcRDD
  }

  def getOneMonthCORDD(spark: SparkSession): RDD[(Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneMonthTime

    val jdbcRDD = getCoinOrder(spark).select("price", "payFinishTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null) false
        else true
      })
      .map(row => {
        (row.get(0), DateUtil.getMillisecondsYMDHMS(row.get(1).toString))
      })
      .filter(tuple => {
        if (tuple._2 > startTime && tuple._2 < endTime) true
        else false
      })

    jdbcRDD
  }

  def getOneDayCBRDD(spark: SparkSession): RDD[(Any, Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneDayTime

    val jdbcRDD = getCoinBill(spark).select("amount", "info", "createTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null || row.get(2) == null) false
        else true
      })
      .map(row => {
        (row.get(0), row.get(1), DateUtil.getMillisecondsYMDHMS(row.get(2).toString))
      })
      .filter(tuple => {
        if (tuple._3 > startTime && tuple._3 < endTime) true
        else false
      })

    jdbcRDD
  }

  def getOneWeekCBRDD(spark: SparkSession): RDD[(Any, Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneWeekTime
    val jdbcRDD = getCoinBill(spark).select("amount", "info", "createTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null || row.get(2) == null) false
        else true
      })
      .map(row => {
        (row.get(0), row.get(1), DateUtil.getMillisecondsYMDHMS(row.get(2).toString))
      })
      .filter(tuple => {
        if (tuple._3 > startTime && tuple._3 < endTime) true
        else false
      })

    jdbcRDD
  }

  def getOneMonthCBRDD(spark: SparkSession): RDD[(Any, Any, Long)] = {

    val (startTime: Long, endTime: Long) = getOneMonthTime

    val jdbcRDD = getCoinBill(spark).select("amount", "info", "createTime")
      .rdd
      .filter(row => {
        if (row.get(0) == null || row.get(1) == null || row.get(2) == null) false
        else true
      })
      .map(row => {
        (row.get(0), row.get(1), DateUtil.getMillisecondsYMDHMS(row.get(2).toString))
      })
      .filter(tuple => {
        if (tuple._3 > startTime && tuple._3 < endTime) true
        else false
      })

    jdbcRDD
  }

  private def getOneDayTime = {
    val startTime = DateUtil.getMillisecondsYMD(DateUtil.getYesterday())
    val endTime = startTime + 86400000
    (startTime, endTime)
  }

  private def getOneWeekTime = {
    val startTime = DateUtil.getMillisecondsYMD(DateUtil.getDateNow())
    val endTime = startTime - 604800000
    (endTime, startTime)
  }

  private def getOneMonthTime = {
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
    (startTime, endTime)
  }
}
