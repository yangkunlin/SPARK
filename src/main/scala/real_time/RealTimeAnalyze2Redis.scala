package real_time

import common.CommonParams
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.JedisCluster
import utils.IPUtils
import utils.connection.RedisUtil
import utils.filter.BloomFilter

/**
  * Description: 
  *
  * @author YKL on 2018/5/21.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object RealTimeAnalyze2Redis {

  /**
    * ***************************************日、周、月、年各地域用户数***************************************
    *
    * @param ip
    * @param value
    * @param jedis
    * @param key
    * @return
    */
  def areaNumber(ip: String, value: Array[(String, String, String, String)], jedis: JedisCluster, key: String) = {
    val ipNum = IPUtils.ip2Long(ip)
    val index = IPUtils.binarySearch(value, ipNum)
    if (index != -1) {
      val info = value(index)
      jedis.hincrBy(key, info._3, 1)
    }
  }

  /**
    * ***************************************日、周、月、年活跃用户数***************************************
    *
    * @param formattedRDD
    */
  def userOnlineNumber(formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)], value: Array[(String, String, String, String)]): Unit = {
    formattedRDD.foreachRDD(userTracksRDD => {
      userTracksRDD.foreachPartition(iter => {
        val jedis = RedisUtil.getJedisCluster()
        iter.foreach(_tuple => {

          var userFlag = ""
          var isEmptyImei = false
          var isEmptyMeid = false
          if (_tuple._5 != null) {
            isEmptyImei = _tuple._5.isEmpty
          }
          if (_tuple._6 != null) {
            isEmptyMeid = _tuple._6.isEmpty
          }

          isEmptyImei match {
            case true if isEmptyMeid => userFlag = "nobody"
            case true if !isEmptyMeid => userFlag = _tuple._6
            case false if !isEmptyMeid => userFlag = _tuple._5 + "|" + _tuple._6
            case false if isEmptyMeid => userFlag = _tuple._5
          }

          val bloomFilterFlagDaily = BloomFilter.exists(CommonParams.DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
          val bloomFilterFlagWeekly = BloomFilter.exists(CommonParams.WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
          val bloomFilterFlagMonthly = BloomFilter.exists(CommonParams.WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
          val bloomFilterFlagYearly = BloomFilter.exists(CommonParams.YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
          if (_tuple._1.isEmpty) {
            //所有用户包括普通和登陆用户
            if (!bloomFilterFlagDaily) {
              BloomFilter.hashValue(CommonParams.DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
              jedis.incr(CommonParams.DAILYKEY + CommonParams.RESULTKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + CommonParams.DAILYKEY)
              }
            }
            if (!bloomFilterFlagWeekly) {
              BloomFilter.hashValue(CommonParams.WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
              jedis.incr(CommonParams.WEEKLYKEY + CommonParams.RESULTKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + CommonParams.WEEKLYKEY)
              }
            }
            if (!bloomFilterFlagMonthly) {
              BloomFilter.hashValue(CommonParams.MONTHLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
              jedis.incr(CommonParams.MONTHLYKEY + CommonParams.RESULTKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + CommonParams.MONTHLYKEY)
              }
            }
            if (!bloomFilterFlagYearly) {
              BloomFilter.hashValue(CommonParams.YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
              jedis.incr(CommonParams.YEARLYKEY + CommonParams.RESULTKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + CommonParams.YEARLYKEY)
              }
            }
          } else {
            //登陆用户
            if (!bloomFilterFlagDaily) {
              BloomFilter.hashValue(CommonParams.DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
              jedis.incr(CommonParams.DAILYKEY + CommonParams.RESULTKEY)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.DAILYKEY + CommonParams.RESULTKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + CommonParams.DAILYKEY)
              }
            }
            if (!bloomFilterFlagWeekly) {
              BloomFilter.hashValue(CommonParams.WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
              jedis.incr(CommonParams.WEEKLYKEY + CommonParams.RESULTKEY)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.WEEKLYKEY + CommonParams.RESULTKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + CommonParams.WEEKLYKEY)
              }
            }
            if (!bloomFilterFlagMonthly) {
              BloomFilter.hashValue(CommonParams.MONTHLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
              jedis.incr(CommonParams.MONTHLYKEY + CommonParams.RESULTKEY)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.MONTHLYKEY + CommonParams.RESULTKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + CommonParams.MONTHLYKEY)
              }
            }
            if (!bloomFilterFlagYearly) {
              BloomFilter.hashValue(CommonParams.YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag)
              jedis.incr(CommonParams.YEARLYKEY + CommonParams.RESULTKEY)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.YEARLYKEY + CommonParams.RESULTKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + CommonParams.YEARLYKEY)
              }
            }
          }
        })
      })
    })
  }

  /**
    * ***************************************日、周、月、年path访问量***************************************
    *
    * @param formattedRDD
    */
  def pathNumber(formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)]): Unit = {

    formattedRDD.foreachRDD(userTracksRDD => {
      userTracksRDD.foreachPartition(iter => {
        val jedis = RedisUtil.getJedisCluster()
        iter.foreach(_tuple => {
          val path = _tuple._2
          jedis.hincrBy(CommonParams.PATHKEY + CommonParams.DAILYKEY, path, 1)
          jedis.hincrBy(CommonParams.PATHKEY + CommonParams.WEEKLYKEY, path, 1)
          jedis.hincrBy(CommonParams.PATHKEY + CommonParams.MONTHLYKEY, path, 1)
          jedis.hincrBy(CommonParams.PATHKEY + CommonParams.YEARLYKEY, path, 1)
          if (!_tuple._1.isEmpty) {
            jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + CommonParams.DAILYKEY, path, 1)
            jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + CommonParams.WEEKLYKEY, path, 1)
            jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + CommonParams.MONTHLYKEY, path, 1)
            jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + CommonParams.YEARLYKEY, path, 1)
          }
        })
      })
    })

  }
}
