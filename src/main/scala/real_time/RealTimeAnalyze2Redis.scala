package real_time

import common.CommonParams
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.JedisCluster
import utils.connection.RedisUtil
import utils.filter.BloomFilter
import utils.{DateUtil, IPUtils}

/**
  * Description: 
  * 实时流数据计算，并将结果存入redis
  *
  * @author YKL on 2018/5/21.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object RealTimeAnalyze2Redis {


  /**
    * ***************************************日、周、月、年各地域用户数*************************************
    *
    * @param ip
    * @param value
    * @param jedis
    * @param key
    */
  def areaNumber(ip: String, value: Array[(String, String, String, String)], jedis: JedisCluster, key: String): Unit = {
    val ipNum = IPUtils.ip2Long(ip)
    val index = IPUtils.binarySearch(value, ipNum)
    if (index != -1) {
      val info = value(index)
      jedis.hincrBy(key, info._3, 1)
    }
  }

  /**
    * ***************************************日、周、月、年path访问量***************************************
    *
    * @param _tuple     tuple
    * @param jedis      JedisCluster
    * @param DAILYKEY   Str
    * @param WEEKLYKEY  Str
    * @param MONTHLYKEY Str
    * @param YEARLYKEY  Str
    * @param pathRulesMap Map
    */
  def pathNumber(_tuple: (String, String, String, String, String, String, String, String, String, String, String),
                 jedis: JedisCluster, DAILYKEY: String, WEEKLYKEY: String, MONTHLYKEY: String, YEARLYKEY: String, pathRulesMap: Map[String, String]): Unit = {
    val path = _tuple._2
    if (pathRulesMap.contains(path)) {
      jedis.hincrBy(CommonParams.PATHKEY + DAILYKEY, pathRulesMap(path), 1)
      jedis.hincrBy(CommonParams.PATHKEY + WEEKLYKEY, pathRulesMap(path), 1)
      jedis.hincrBy(CommonParams.PATHKEY + MONTHLYKEY, pathRulesMap(path), 1)
      jedis.hincrBy(CommonParams.PATHKEY + YEARLYKEY, pathRulesMap(path), 1)
    }

    if (!_tuple._1.isEmpty) {
      if (pathRulesMap.contains(path)) {
        jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + DAILYKEY, pathRulesMap(path), 1)
        jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + WEEKLYKEY, pathRulesMap(path), 1)
        jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + MONTHLYKEY,pathRulesMap(path), 1)
        jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + YEARLYKEY, pathRulesMap(path), 1)
      }
    }
  }

  /**
    * ***************************************日、周、月、年活跃用户数***************************************
    * ***************************************日、周、月、年登陆用户数***************************************
    * ***************************************日、周、月、年各地域用户数*************************************
    * ***************************************次日留存用户数*************************************************
    * ***************************************日、周、月、年path访问量***************************************
    *
    * @param formattedRDD
    */
  def userOnlineNumber(formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String)],
                       value: Array[(String, String, String, String)], pathRulesMap: Map[String, String]): Unit = {
    formattedRDD.foreachRDD(userTracksRDD => {
      userTracksRDD.foreachPartition(iter => {
        val DAILYKEY: String = DateUtil.getDateNow()
        val WEEKLYKEY: String = DateUtil.getNowWeekStart() + "_" + DateUtil.getNowWeekEnd()
        val MONTHLYKEY: String = DateUtil.getMonthNow()
        val YEARLYKEY: String = DateUtil.getYearNow()
        val LASTDAILYKEY: String = DateUtil.getYesterday()
        val jedis: JedisCluster = RedisUtil.getJedisCluster
        iter.foreach(_tuple => {
          pathNumber(_tuple, jedis, DAILYKEY, WEEKLYKEY, MONTHLYKEY, YEARLYKEY, pathRulesMap)
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

          val bloomFilterFlagDaily = BloomFilter.exists(DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagWeekly = BloomFilter.exists(WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagMonthly = BloomFilter.exists(MONTHLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagYearly = BloomFilter.exists(YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagLastDaily = BloomFilter.exists(LASTDAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagForever = BloomFilter.exists(CommonParams.FOREVERKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          if (_tuple._1.isEmpty) {
            //所有用户包括普通和登陆用户
            if (!bloomFilterFlagDaily) {
              BloomFilter.hashValue(DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + DAILYKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + DAILYKEY)
              }
              if (bloomFilterFlagLastDaily) {
                jedis.incr(CommonParams.AGAINKEY + CommonParams.ONLINEKEY + DAILYKEY)
              }
            }
            if (!bloomFilterFlagWeekly) {
              BloomFilter.hashValue(WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + WEEKLYKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + WEEKLYKEY)
              }
            }
            if (!bloomFilterFlagMonthly) {
              BloomFilter.hashValue(MONTHLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + MONTHLYKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + MONTHLYKEY)
              }
            }
            if (!bloomFilterFlagYearly) {
              BloomFilter.hashValue(YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + YEARLYKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + YEARLYKEY)
              }
            }
            if (!bloomFilterFlagForever) {
              BloomFilter.hashValue(CommonParams.FOREVERKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + CommonParams.FOREVERKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.AREAKEY + CommonParams.FOREVERKEY)
              }
              if (_tuple._7 != null && !_tuple._7.isEmpty) {
                jedis.hincrBy(CommonParams.OSKEY + CommonParams.FOREVERKEY, _tuple._7, 1)
              }
              if (_tuple._8 != null && !_tuple._8.isEmpty) {
                jedis.hincrBy(CommonParams.MODELKEY + CommonParams.FOREVERKEY, _tuple._8, 1)
              }
              if (_tuple._9 != null && !_tuple._9.isEmpty) {
                jedis.hincrBy(CommonParams.CHANNELKEY + CommonParams.FOREVERKEY, _tuple._9, 1)
              }
            }
          } else {
            //登陆用户
            if (!bloomFilterFlagDaily) {
              BloomFilter.hashValue(DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + DAILYKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + DAILYKEY)
              }
              if (bloomFilterFlagLastDaily) {
                jedis.incr(CommonParams.AGAINKEY + CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + DAILYKEY)
              }
            }
            if (!bloomFilterFlagWeekly) {
              BloomFilter.hashValue(WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + WEEKLYKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + WEEKLYKEY)
              }
            }
            if (!bloomFilterFlagMonthly) {
              BloomFilter.hashValue(MONTHLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + MONTHLYKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + MONTHLYKEY)
              }
            }
            if (!bloomFilterFlagYearly) {
              BloomFilter.hashValue(YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + YEARLYKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + YEARLYKEY)
              }
            }
            if (!bloomFilterFlagForever) {
              BloomFilter.hashValue(CommonParams.FOREVERKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + CommonParams.FOREVERKEY)
              if (_tuple._3 != null && !_tuple._3.isEmpty) {
                areaNumber(_tuple._3, value, jedis, CommonParams.LOGINEDKEY + CommonParams.AREAKEY + CommonParams.FOREVERKEY)
              }
              if (_tuple._7 != null && !_tuple._7.isEmpty) {
                jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.OSKEY + CommonParams.FOREVERKEY, _tuple._7, 1)
              }
              if (_tuple._8 != null && !_tuple._8.isEmpty) {
                jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.MODELKEY + CommonParams.FOREVERKEY, _tuple._8, 1)
              }
              if (_tuple._9 != null && !_tuple._9.isEmpty) {
                jedis.hincrBy(CommonParams.LOGINEDKEY + CommonParams.CHANNELKEY + CommonParams.FOREVERKEY, _tuple._9, 1)
              }
            }
          }
        })
        jedis.close()
      })
    })
  }
}
