package common

/**
  * Description: 
  *
  * @author YKL on 2018/5/15.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object CommonParams {

  //测试环境参数
  val TRIALTOPIC = Array("TrialBigData")

  val TRIALTABLENAME = "TrialUserTracks"

  val TRIALCOLUMNFAMILY = "info"

  //生产环境参数
  val FINALTOPIC = Array("FinalBigData")

  val FINALTABLENAME = "FinalUserTracks"

  val FINALCOLUMNFAMILY = "info"

  val CONSUMERGROUP = "save"

//  val REDISHOST = ("10.141.30.98", "10.141.82.240", "10.141.0.198")
//
//  val REDISPORT = 6300

  val REDISHOST = ("192.168.1.123")

  val REDISPORT = 6301

}
