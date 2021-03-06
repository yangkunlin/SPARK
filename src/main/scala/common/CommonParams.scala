package common

import utils.DateUtil

/**
  * Description: 
  *
  * @author YKL on 2018/5/15.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object CommonParams {

  /**
    * ******************************************* kafka configuration *****************************************
    */

  val KAFKASERVERS = "10.141.30.98:9092,10.141.82.240:9092,10.141.0.198:9092"

  //测试环境参数
  val TRIALTOPIC = Array("TrialBigData")

  //生产环境参数
  val FINALUSERTRACKSTOPIC = Array("FinalBigData")

  val FINALSEARCHTOPIC = Array("FinalSearch")

  val CONSUMERGROUP = "SPARKSTREAMING"
  /**
    * ******************************************* hbase configuration *****************************************
    */

  val HBASEHOST = "bigdata-slave01,bigdata-slave02,bigdata-slave03"

  val HBASEPORT = "2181"

  //测试环境参数
  val TRIALTABLENAME = "TrialUserTracks"

  val TRIALCOLUMNFAMILY = "info"

  //生产环境参数
  val FINALTABLENAME = Array("FinalUserTracks", "FinalSearch")

  val FINALCOLUMNFAMILY = "info"


  /**
    * ******************************************* redis configuration *****************************************
    */
  val REDISCLUSTERHOST: (String, String, String) = ("bigdata-slave01", "bigdata-slave02", "bigdata-slave03")

  val REDISCLUSTERPORT: (Int, Int) = (7000, 7001)

  val REDISHOST = "bigdata-master02"

  val REDISPORT = 6301

  val PATHKEY = "path_"

  val LOGINEDKEY: String = "logined_"

  val NOTLOGINKEY: String = "notlogin_"

  val BLOOMFILTERKEY: String = "_bloomfilter"

  val FOREVERKEY: String = "forever"

  val OSKEY: String = "os_"

  val MODELKEY: String = "model_"

  val CHANNELKEY: String = "channel_"

  val ONLINEKEY: String = "online_"

  val AGAINKEY: String = "again_"

  val AREAKEY: String = "area_"

  val SEARCHKEY: String = "search_"

  val NEWACTIVATION: String = "new_activation_"

  val GIDKEY: String = "gid_"

  val DAILYKEY: String = DateUtil.getDateNow()

  val WEEKLYKEY: String = DateUtil.getNowWeekStart() + "_" + DateUtil.getNowWeekEnd()

  val MONTHLYKEY: String = DateUtil.getMonthNow()

  val YEARLYKEY: String = DateUtil.getYearNow()

}
