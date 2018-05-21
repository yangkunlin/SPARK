package off_line

import common.LoggerLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.{DateUtil, FilterUtil}

/**
  * @author YKL on 2018/3/23.
  * @version 1.0
  *          说明：
  *          tomcat日志分析
  */
object TomcatLogUV {

  /**
    *
    * @param cacheRDD
    * @param targetPath
    * @return
    */
  def getModuleClickNumber(cacheRDD: RDD[String], targetPath: String) = {

    val moduleClickNumber = cacheRDD.filter(line => (line.contains()))

  }

  /**
    * 获取活跃用户重复登陆N天的用户数
    *
    * @param cacheRDD
    * @param targetPath
    */
  def getActiveUserRepeatNumber(cacheRDD: RDD[String], targetPath: String) = {

    var tmp = 0

    val activeUserRepeatNumber = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessIp = items(0).trim
      val dateTime = items(3).replace("[", "")
      (accessIp + "-" + dateTime, "")
    }).sortByKey() //按IP和访问时间排序
      .map(line => {
      val items = line._1.split("-")
      (items(0) + "-" + items(1).split(":")(0), DateUtil.getMillisecondsYMD(items(1)))
    }).groupByKey().map(line => {
      val mil = line._2
      val dif = mil.last - mil.head
      (line._1, dif)
    }) //计算用户每日在线时长
      .filter(tuple => tuple._2 > 1800000)
      .map(tuple => {
        val accessIp = tuple._1.split("-")(0)
        (accessIp, tuple._2)
      }).groupByKey().map(tuple => {
      (tuple._2.size, 1)
    }).reduceByKey(_ + _).sortByKey(false).repartition(1)
      .map(x => {
      tmp = tmp + x._2
      (x._1, tmp)
    })

    activeUserRepeatNumber.saveAsTextFile(targetPath + "N天重复登陆活跃用户数")

  }

  /**
    * 获取N天重复登陆用户数
    * @param cacheRDD
    * @param targetPath
    */
  def getUserRepeatOnlineNumber(cacheRDD: RDD[String], targetPath: String) = {

    var tmp = 0

    val userRepeatOnlineNumber = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessIp = items(0).trim
      val dateTime = items(3).replace("[", "").split(":")(0)
      (accessIp + "-" + dateTime, "")
    }).groupByKey() //对每日相同用户去重
      .map(line => {
      val items = line._1.split("-")
      (items(0), items(1))
    }).groupByKey().map(tuple => {
      (tuple._2.size, 1)
    }).reduceByKey(_ + _).sortByKey(false).repartition(1)
      .map(x => {
        tmp = tmp + x._2
      (x._1, tmp)
    })

    //.foreach(println)
    userRepeatOnlineNumber.saveAsTextFile(targetPath + "N天重复登陆用户数")

  }


  /**
    * 每日活跃用户数
    * @param cacheRDD
    * @param targetPath
    */
  def getActiveUserOneDayNumber(cacheRDD: RDD[String], targetPath: String) = {

    val activeUserOneDayNumber = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessIp = items(0).trim
      val dateTime = items(3).replace("[", "")
      (accessIp + "-" + dateTime, "")
    }).sortByKey() //按IP和访问时间排序
      .map(line => {
      val items = line._1.split("-")
      (items(0) + "-" + items(1).split(":")(0), DateUtil.getMillisecondsYMD(items(1)))
    }).groupByKey().map(line => {
      val mil = line._2
      val dif = mil.last - mil.head
      (line._1, dif)
    }) //计算用户每日在线时长
      .filter(tuple => tuple._2 > 1800000)
      .map(tuple => {
        val date = tuple._1.split("-")(1)
        (date + "活跃用户数", 1)
      }).reduceByKey(_ + _).sortByKey().repartition(1)
    activeUserOneDayNumber.saveAsTextFile(targetPath + "每日活跃用户数")

  }


  /**
    * 每日在线用户数
    * @param cacheRDD
    * @param targetPath
    */
  def getUserOneDayOnlineNumber(cacheRDD: RDD[String], targetPath: String) = {

    val userOneDayOnlineNumber = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessIp = items(0).trim
      val dateTime = items(3).replace("[", "").split(":")(0)
      (accessIp + "-" + dateTime, "")
    }).groupByKey() //对每日相同用户去重
      .map(line => {
      val items = line._1.split("-")
      (items(1) + "在线用户数", 1)
    }).reduceByKey(_ + _).sortByKey().repartition(1)
    //.foreach(println)
    userOneDayOnlineNumber.saveAsTextFile(targetPath + "每日在线用户数")

  }


  /**
    * 在线用户总数
    * @param cacheRDD
    * @param targetPath
    */
  def getUserOnlineNumber(cacheRDD: RDD[String], targetPath: String) = {

    val userOnlineNumber = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessIp = items(0).trim
      (accessIp, "")
    }).groupByKey() //根据相同IP对重复用户去重
      .map(line => {
      (1, 1)
    }).reduceByKey(_ + _).repartition(1)
    userOnlineNumber.saveAsTextFile(targetPath + "在线用户总数")

  }


}


object TomcatLogPV {

  /**
    * page停留时间排序
    * @param cacheRDD
    * @param targetPath
    */
  def getPageStandingTime(cacheRDD: RDD[String], targetPath: String) = {

    val pageStandingTime = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessIp = items(0).trim
      val dateTime = items(3).replace("[", "")
      val accessResource = items(6).trim
      (accessIp + "-" + dateTime, accessResource)
    }).sortByKey().map(tuple => {
      val items = tuple._1.split("-")
      (items(0) + "-" + items(1).split(":")(0), tuple._2 + " " + DateUtil.getMillisecondsYMD(items(1)))
    }).groupByKey().map(tuple => {
      var dif = 0L
      var url = ""
      val it = tuple._2.toIterator
      while (it.hasNext) {
        val items = it.next().split(" ")
        val t1 = items(1)
        val u1 = items(0)
        if (it.hasNext) {
          val t2 = it.next().split(" ")(1)
          val d = t2.toLong - t1.toLong
          if (d > dif) {
            dif = d
            url = u1
          }
        }
      }
      (url, dif)
    }).reduceByKey(_ + _).sortBy(_._2, false).repartition(1)

    pageStandingTime.saveAsTextFile(targetPath + "page停留时间排序")

  }


  /**
    * page访问次数排序
    * @param cacheRDD
    * @param targetPath
    */
  def getAccessPageNumber(cacheRDD: RDD[String], targetPath: String) = {

    val accessPageNumber = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessResource = items(6).trim
      (accessResource, 1)
    }).reduceByKey(_ + _).map(tuple => {
      (tuple._2, tuple._1)
    }).sortByKey(false).repartition(1)
    accessPageNumber.saveAsTextFile(targetPath + "page访问次数排序")

  }

}

object TomcatLogOfflineAnalyze {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setLogLevels()

    val MONTH = "temp"

    //spark上下文
    val conf = new SparkConf().setMaster("local[3]").setAppName("LogAnalyze")
    val sc = new SparkContext(conf)

    //文件输入输出地址
    val sourcePath = "E:\\data\\temp\\*"
    val targetPath = "E:\\result\\" + MONTH + "\\"
    val ipRulesPath = "E:\\data\\IP\\*"
    //获取待分析文件
    val rdd = sc.textFile(sourcePath)
    //数据初步过滤并缓存
    val cacheRDD = rdd.filter(line => FilterUtil.timeFilter(line)).filter(line => FilterUtil.existFilter(line)).cache()

//    TomcatLogUV.getUserOnlineNumber(cacheRDD,targetPath)
//
//    TomcatLogUV.getUserOneDayOnlineNumber(cacheRDD, targetPath)
//
//    TomcatLogUV.getActiveUserOneDayNumber(cacheRDD, targetPath)
//
//    TomcatLogUV.getUserRepeatOnlineNumber(cacheRDD, targetPath)
//
//    TomcatLogUV.getActiveUserRepeatNumber(cacheRDD, targetPath)
//
//    TomcatLogPV.getAccessPageNumber(cacheRDD, targetPath)
//
//    TomcatLogPV.getPageStandingTime(cacheRDD, targetPath)
//
//    TomcatLogUV.getModuleClickNumber(cacheRDD, targetPath)

  }

}
