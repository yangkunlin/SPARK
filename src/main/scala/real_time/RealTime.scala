package real_time

import common.CommonParams
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Description: 
  *
  * @author YKL on 2018/5/20.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object RealTime {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RealTime")
    //.setMaster("yarn-cluster")
    val ssc = new StreamingContext(conf, Seconds(10));

    val realTimeSave2Hbase = new RealTimeSave2Hbase(ssc)

    val trialStreamingRDD = realTimeSave2Hbase.getKafkaStreamingRDD(CommonParams.TRIALTOPIC, CommonParams.CONSUMERGROUP)

    val finalStreamingRDD = realTimeSave2Hbase.getKafkaStreamingRDD(CommonParams.FINALTOPIC, CommonParams.CONSUMERGROUP)

    realTimeSave2Hbase.saveRDD2UserTracks(trialStreamingRDD, CommonParams.TRIALTABLENAME, CommonParams.TRIALCOLUMNFAMILY)

    realTimeSave2Hbase.saveRDD2UserTracks(finalStreamingRDD, CommonParams.FINALTABLENAME, CommonParams.FINALCOLUMNFAMILY)

    val formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)] = realTimeSave2Hbase.formatRDD(finalStreamingRDD)

    realTimeSave2Hbase.saveRDD2UserLoginTime(formattedRDD, "UserLoginTime", "info")

    ssc.start()
    ssc.awaitTermination()
  }

}
