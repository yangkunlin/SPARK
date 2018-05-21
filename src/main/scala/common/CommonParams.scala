package common

/**
  * Description: 
  *
  * @author YKL on 2018/5/15.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object CommonParams {

  /**
    * ******************************************* hbase configuration *****************************************
    */

  val KAFKASERVERS = "10.141.30.98:9092,10.141.82.240:9092,10.141.0.198:9092"

  //测试环境参数
  val TRIALTOPIC = Array("TrialBigData")

  //生产环境参数
  val FINALTOPIC = Array("FinalBigData")

  /**
    * ******************************************* hbase configuration *****************************************
    */

  val HBASEHOST = "bigdata-slave01,bigdata-slave02,bigdata-slave03"

  val HBASEPORT = "2181"

  //测试环境参数
  val TRIALTABLENAME = "TrialUserTracks"

  val TRIALCOLUMNFAMILY = "info"

  //生产环境参数
  val FINALTABLENAME = "FinalUserTracks"

  val FINALCOLUMNFAMILY = "info"

  val CONSUMERGROUP = "save"

  /**
    * ******************************************* redis configuration *****************************************
    */
  //  val REDISHOST = ("10.141.30.98", "10.141.82.240", "10.141.0.198")
  //
  //  val REDISPORT = 6300

  val REDISHOST = ("bigdata-master02")

  val REDISPORT = 6301

  val REDISBLOOMFILTERKEY = "bloomfilter"

}
