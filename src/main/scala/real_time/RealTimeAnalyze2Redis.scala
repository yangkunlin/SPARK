package real_time

import common.CommonParams
import org.apache.spark.streaming.dstream.DStream
import utils.connection.RedisUtil
import utils.filter.BloomFilter

/**
  * Description: 
  *
  * @author YKL on 2018/5/21.
  * @version 1.0
  * spark:梦想开始的地方
  */
class RealTimeAnalyze2Redis() {

  /**
    ****************************************日、周、月活跃用户数***************************************
    * @param formattedRDD
    */
  def userOnlineNumberDaily(formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)]): Unit = {
    formattedRDD.foreachRDD(userTracksRDD => {
      userTracksRDD.foreachPartition(iter => {
        val jedis = RedisUtil.getJedis()
        iter.foreach(_tuple => {
          val userFlag = _tuple._5 + "|" + _tuple._6
          val bloomFilterFlagDaily = BloomFilter.exists(CommonParams.DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
          val bloomFilterFlagWeekly = BloomFilter.exists(CommonParams.WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
          val bloomFilterFlagMonthly = BloomFilter.exists(CommonParams.WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
          if (!bloomFilterFlagDaily) {
            BloomFilter.hashValue(CommonParams.DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
            jedis.incr(CommonParams.DAILYKEY + CommonParams.RESULTKEY)
          }
          if (!bloomFilterFlagWeekly) {
            BloomFilter.hashValue(CommonParams.WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
            jedis.incr(CommonParams.WEEKLYKEY + CommonParams.RESULTKEY)
          }
          if (!bloomFilterFlagMonthly) {
            BloomFilter.hashValue(CommonParams.MONTHKEY + CommonParams.BLOOMFILTERKEY, userFlag)
            jedis.incr(CommonParams.MONTHKEY + CommonParams.RESULTKEY)
          }
        })
      })
    })
  }

}
