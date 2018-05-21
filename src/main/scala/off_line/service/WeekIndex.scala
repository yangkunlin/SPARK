package off_line.service


import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import common.LoggerLevels


/**
  * Created on 2018/4/8 10:09
  * Linked with qykx2013@126.com 
  * Updated on 2018/4/9 10:09 in version 0.1.0
  *
  * @author Zhaoyang_Q
  * @note HoPing xiyuan:bigData2018 offline
  * @todo Calculate the 15 APP index proposed by Operations Department,majority is weekly.
  * @version 0.1.0
  */
object WeekIndex {

  /**
    * 1.周新增下载
    * 2.周新增注册
    * 3.周活跃用户数
    * 4.周单天最高在线人数时段1hour
    * 5.周充值金额
    * 6.周消费分布
    * 7.周活跃在线时长
    * 8.周付费在线时长
    * 9.周新用户引导率
    * 10.周新轨迹
    * 11.周板块访问、分享比率
    * 12.用户平均生命周期
    * 13.用户信任付费时长
    * 14.注册转化率
    * 15.病毒传播h5、分享
    */
  case class LogInfo(uid: String, path: String, time: Timestamp, locationX: String, locationY: String,
                     imei: String, meid: String, ip: String, channel: Long)


  def main(args: Array[String]): Unit = {
    //日志级别默认设置为WARN
    LoggerLevels.setLogLevels()
    //创建SparkSession
    val spark = SparkSession.builder
      .appName("CalculateWeekIndex")
      .config("spark.sql.warehouse.dir", "spark-path")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    //读取数据源，json格式，选择必要行列
    val dayLogDS = spark.read.json("D:/project/data/json/day_logs/20180406/test.json",
      "D:/project/data/json/day_logs/20180407/test.json")
      .selectExpr("uid", "path", "time", "location.x as locationX",
        "location.y as locationY", "imei", "meid", "ip", "channel")
      .as[LogInfo].cache()
    dayLogDS.show()

    /**
      * *******************************************<指标计算正文>*********************************************************
      */

    /**
      * 周新注册用户数
      */
    val newRegisteredUserCount = dayLogDS.filter(_.uid != null)
      .where("uid != ''")
      .filter(_.path.contains("Regis")) //这里是注册成功的接口名【也可以获取全部uid，读取原注册用户，做差，计数】
      .dropDuplicates("uid")
      .count()
    /**
      * 周活动超过3次的用户数
      */
    val weekActiveUserCount = dayLogDS.map(
      m => {
        (m.uid + m.imei + m.meid, 1)
      }).groupBy("_1").count()
      .filter("count > 2")
      .count()

    /**
      * 周充值金额
      */
    val weekPaymentAmount = 0L

    /**
      * 周单天最高在线人数时段的1 hour
      */
    val weekMaxOnlineHours = 0

    /**
      * 周消费板块分布，按次数算
      */
    def inPayType(path: String, payList: List[String]): Boolean = {
      for (e <- payList) {
        if (path.contains(e)) {
          return true
        }
      }
      false
    }

    val weekPayForWhatRate = dayLogDS.filter(m => {
      inPayType(m.path, List("Vote", "Match","aaa","bbb"))
    }).map(m=>{
      (m.path,1)
    }).rdd.reduceByKey(_+_).toDS().show()

    /**
      * 周活跃用户在线平均时长
      */
    val weekActiveUserOnlineTime = 0

    /**
      * 周付费用户在线平均时长
      */
    val weekPayingUserOnlineTime = 0

    /**
      * 周板块访问率
      */
    val weekPageViewRate = 0

    /**
      * 周板块分享率
      */
    val weekPageShareRate = 0

    /**
      * 周新用户轨迹
      */
    val weekNewUserAccessRoute = ""

    /**
      * 用户平均生命周期
      */
    val userLifecycle = 0

    /**
      * 用户信任付费时长
      */
    val userTrust2PaySpendTime = 0

    /**
      * 注册转化率
      */
    val userRegistrationLoginRate = 0.0


    println("newRegisteredUserCount:" + newRegisteredUserCount)
    println("weekActiveUserCount:" + weekActiveUserCount)


  }

}
