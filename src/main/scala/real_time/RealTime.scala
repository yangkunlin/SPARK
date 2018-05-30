package real_time

import common.CommonParams
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.filter.FilterUtil

/**
  * Description: 
  *
  * @author YKL on 2018/5/20.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object RealTime {

  def getPathRulesRDD(sc: SparkContext) = {
    sc.textFile("/user/data/PATH_API.csv")
      .map(line => {
        val fields = line.split("\t")
        val path = fields(0)
        val api = fields(1)
        (path, api)
      })
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RealTime")
    val sc = new SparkContext(conf)
    //.setMaster("yarn-cluster")
    val ssc = new StreamingContext(sc, Seconds(60))

    //全部的IP映射规则
    val ipRulesRDD = getIPRulesRDD(sc).cache()
    val ipRulesArray = ipRulesRDD.collect

    //path映射规则
    val pathRulesRDD = getPathRulesRDD(sc).cache()
    val pathRulesMap = pathRulesRDD.collect.toMap
    //广播规则，这个是由Driver向worker中广播规则
    val ipRulesBroadcast = sc.broadcast(ipRulesArray)
    val pathRulesBroadcast = sc.broadcast(pathRulesMap)

//    val trialStreamingRDD = RealTimeSave2Hbase.getKafkaStreamingRDD(ssc, CommonParams.TRIALTOPIC, CommonParams.CONSUMERGROUP)

    val finalStreamingRDD = RealTimeSave2Hbase.getKafkaStreamingRDD(ssc, CommonParams.FINALTOPIC, CommonParams.CONSUMERGROUP)

//    RealTimeSave2Hbase.saveRDD2UserTracks(trialStreamingRDD, CommonParams.TRIALTABLENAME, CommonParams.TRIALCOLUMNFAMILY)

    RealTimeSave2Hbase.saveRDD2UserTracks(finalStreamingRDD, CommonParams.FINALTABLENAME, CommonParams.FINALCOLUMNFAMILY)

    val formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)] = RealTimeSave2Hbase.formatRDD(finalStreamingRDD)

//    RealTimeSave2Hbase.saveRDD2UserLoginTime(formattedRDD, "UserLoginTime", "info")

    RealTimeAnalyze2Redis.userOnlineNumber(formattedRDD, ipRulesBroadcast.value)

    RealTimeAnalyze2Redis.pathNumber(formattedRDD, pathRulesBroadcast.value)

    ssc.start()
    ssc.awaitTermination()
  }

  private def getIPRulesRDD(sc: SparkContext) = {
    sc.textFile("/user/data/IPTABLE.txt")
      .filter(line => FilterUtil.fieldsLengthFilter(line, "\t", 7))
      .map(line => {
        val fields = line.split("\t")
        val startIPNum = fields(1)
        //        val startIP = fields(2)
        val endIPNum = fields(3)
        //        val endIP = fields(4)
        val country = fields(5)
        val local = fields(6)
        (startIPNum, endIPNum, country, local)
      })
  }
}
