package real_time

import common.CommonParams
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

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
      .set("spark.default.parallelism", "1000")
//      .set("spark.executor.instances", "8")
      .set("spark.locality.wait", "100")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     // .set("spark.cores.max ", "12").set("spark.executor.cores", "1")
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.scheduler.pool", "production")
    //.setMaster("yarn-cluster")
    val ssc = new StreamingContext(sc, Seconds(60))

    //全部的IP映射规则
//    val ipRulesRDD = getIPRulesRDD(sc).cache
//    val ipRulesArray = ipRulesRDD.collect
    //path映射规则
    val pathRulesRDD = getPathRulesRDD(sc).cache()
    val pathRulesMap = pathRulesRDD.collect.toMap
    //广播规则，这个是由Driver向worker中广播规则
//    val ipRulesBroadcast = sc.broadcast(ipRulesArray)
    val pathRulesBroadcast = sc.broadcast(pathRulesMap)

//    val trialStreamingRDD = RealTimeSave2Hbase.getKafkaStreamingRDD(ssc, CommonParams.TRIALTOPIC, CommonParams.CONSUMERGROUP)

    val userTracksStreamingRDD = RealTimeSave2Hbase.getKafkaStreamingRDD(ssc, CommonParams.FINALUSERTRACKSTOPIC, CommonParams.CONSUMERGROUP)

    val searchStreamingRDD = RealTimeSave2Hbase.getKafkaStreamingRDD(ssc, CommonParams.FINALSEARCHTOPIC, CommonParams.CONSUMERGROUP)

//    RealTimeSave2Hbase.saveRDD2UserTracks(trialStreamingRDD, CommonParams.TRIALTABLENAME, CommonParams.TRIALCOLUMNFAMILY)

    RealTimeSave2Hbase.saveRDD2HBase(userTracksStreamingRDD, CommonParams.FINALTABLENAME(0), CommonParams.FINALCOLUMNFAMILY)

    RealTimeSave2Hbase.saveRDD2HBase(searchStreamingRDD, CommonParams.FINALTABLENAME(1), CommonParams.FINALCOLUMNFAMILY)

    val formattedUserTracksRDD = RealTimeSave2Hbase.formatUserTracksRDD(userTracksStreamingRDD)

    val formattedSearchRDD = RealTimeSave2Hbase.formatSearchRDD(searchStreamingRDD)

//    RealTimeSave2Hbase.saveRDD2UserLoginTime(formattedUserTracksRDD, "UserLoginTime", "info")

    RealTimeAnalyze2Redis.analyzeUserTracks(formattedUserTracksRDD, pathRulesBroadcast.value)

    RealTimeAnalyze2Redis.analyzeSearch(formattedSearchRDD)

    ssc.start()
    ssc.awaitTermination()
  }

//  private def getIPRulesRDD(sc: SparkContext) = {
//    sc.textFile("/user/data/IPTABLE.txt")
//      .filter(line => FilterUtil.fieldsLengthFilter(line, "\t", 7))
//      .map(line => {
//        val fields = line.split("\t")
//        val startIPNum = fields(1)
//        //        val startIP = fields(2)
//        val endIPNum = fields(3)
//        //        val endIP = fields(4)
//        val country = fields(5)
//        val local = fields(6)
//        (startIPNum, endIPNum, country, local)
//      })
//  }
}
