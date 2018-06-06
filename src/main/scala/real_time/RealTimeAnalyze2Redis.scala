package real_time

import common.CommonParams
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.JedisCluster
import utils.DateUtil
import utils.connection.RedisUtil
import utils.filter.BloomFilter

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
  def pathNumber(_tuple: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String),
                 jedis: JedisCluster, DAILYKEY: String, WEEKLYKEY: String, MONTHLYKEY: String, YEARLYKEY: String, pathRulesMap: Map[String, String]): Unit = {
    val path = _tuple._2
    if (pathRulesMap.contains(path)) {
      mapHincr(CommonParams.PATHKEY + DAILYKEY, pathRulesMap(path).toString, jedis)
      mapHincr(CommonParams.PATHKEY + WEEKLYKEY, pathRulesMap(path).toString, jedis)
      mapHincr(CommonParams.PATHKEY + MONTHLYKEY, pathRulesMap(path).toString, jedis)
      mapHincr(CommonParams.PATHKEY + YEARLYKEY, pathRulesMap(path).toString, jedis)
      mapHincr(CommonParams.PATHKEY + CommonParams.FOREVERKEY, pathRulesMap(path).toString, jedis)
    }

    if (!_tuple._1.isEmpty) {
      if (pathRulesMap.contains(path)) {
        mapHincr(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + DAILYKEY, pathRulesMap(path).toString, jedis)
        mapHincr(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + WEEKLYKEY, pathRulesMap(path).toString, jedis)
        mapHincr(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + MONTHLYKEY,pathRulesMap(path).toString, jedis)
        mapHincr(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + YEARLYKEY, pathRulesMap(path).toString, jedis)
        mapHincr(CommonParams.LOGINEDKEY + CommonParams.PATHKEY + CommonParams.FOREVERKEY, pathRulesMap(path).toString, jedis)
      }
    }
  }

  /**
    * ***************************************redis map 自增加***********************************************
    * @param key
    * @param dateStr
    * @param jedis
    * @return
    */
  def mapHincr(key: String, dateStr: String, jedis: JedisCluster): Unit = {
    jedis.hincrBy(key, dateStr, 1)
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
  def analyzeUserTracks(formattedRDD: DStream[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)],
                        pathRulesMap: Map[String, String]): Unit = {
    formattedRDD.foreachRDD(userTracksRDD => {
      userTracksRDD.foreachPartition(iter => {
        /** 获取日、周、月、年、昨日的标识符 **/
        val DAILYKEY: String = DateUtil.getDateNow()
        val WEEKLYKEY: String = DateUtil.getNowWeekStart() + "_" + DateUtil.getNowWeekEnd()
        val MONTHLYKEY: String = DateUtil.getMonthNow()
        val YEARLYKEY: String = DateUtil.getYearNow()
        val LASTDAILYKEY: String = DateUtil.getYesterday()
        /** 获取jedis连接 **/
        val jedis: JedisCluster = RedisUtil.getJedisCluster
        iter.foreach(_tuple => {
          /** 调用各地域用户数统计方法 **/
          pathNumber(_tuple, jedis, DAILYKEY, WEEKLYKEY, MONTHLYKEY, YEARLYKEY, pathRulesMap)
          /** 获取用户标识 **/
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
          /** 获取日、周、月、年、总的布隆过滤器计算结果 **/
          val bloomFilterFlagDaily = BloomFilter.exists(DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagWeekly = BloomFilter.exists(WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagMonthly = BloomFilter.exists(MONTHLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagYearly = BloomFilter.exists(YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagLastDaily = BloomFilter.exists(LASTDAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          val bloomFilterFlagForever = BloomFilter.exists(CommonParams.FOREVERKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
          /** 计算普通（非登陆）用户的相关数据 **/
          if (_tuple._1.isEmpty) {
            if (!bloomFilterFlagDaily) {
              BloomFilter.hashValue(DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + DAILYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.AREAKEY + DAILYKEY, _tuple._14, jedis)
              }
              if (bloomFilterFlagLastDaily) {
                jedis.incr(CommonParams.AGAINKEY + CommonParams.ONLINEKEY + DAILYKEY)
              }
            }
            if (!bloomFilterFlagWeekly) {
              BloomFilter.hashValue(WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + WEEKLYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.AREAKEY + WEEKLYKEY, _tuple._14, jedis)
              }
            }
            if (!bloomFilterFlagMonthly) {
              BloomFilter.hashValue(MONTHLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + MONTHLYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.AREAKEY + MONTHLYKEY, _tuple._14, jedis)
              }
            }
            if (!bloomFilterFlagYearly) {
              BloomFilter.hashValue(YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + YEARLYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.AREAKEY + YEARLYKEY, _tuple._14, jedis)
              }
            }
            if (!bloomFilterFlagForever) {
              BloomFilter.hashValue(CommonParams.FOREVERKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.ONLINEKEY + CommonParams.FOREVERKEY)
              jedis.incr(CommonParams.NEWACTIVATION + DAILYKEY)
              jedis.incr(CommonParams.NEWACTIVATION + WEEKLYKEY)
              jedis.incr(CommonParams.NEWACTIVATION + MONTHLYKEY)
              jedis.incr(CommonParams.NEWACTIVATION + YEARLYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.AREAKEY + CommonParams.FOREVERKEY, _tuple._14, jedis)
              }
              if (_tuple._7 != null && !_tuple._7.isEmpty) {
                mapHincr(CommonParams.OSKEY + CommonParams.FOREVERKEY, _tuple._7, jedis)
              }
              if (_tuple._8 != null && !_tuple._8.isEmpty) {
                mapHincr(CommonParams.MODELKEY + CommonParams.FOREVERKEY, _tuple._8, jedis)
              }
              if (_tuple._9 != null && !_tuple._9.isEmpty) {
                mapHincr(CommonParams.CHANNELKEY + CommonParams.FOREVERKEY, _tuple._9, jedis)
              }
            }
          } else {
            /** 计算登陆用户的相关数据 **/
            if (!bloomFilterFlagDaily) {
              BloomFilter.hashValue(DAILYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + DAILYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.LOGINEDKEY + CommonParams.AREAKEY + DAILYKEY, _tuple._14, jedis)
              }
              if (bloomFilterFlagLastDaily) {
                jedis.incr(CommonParams.AGAINKEY + CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + DAILYKEY)
              }
            }
            if (!bloomFilterFlagWeekly) {
              BloomFilter.hashValue(WEEKLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + WEEKLYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.LOGINEDKEY + CommonParams.AREAKEY + WEEKLYKEY, _tuple._14, jedis)
              }
            }
            if (!bloomFilterFlagMonthly) {
              BloomFilter.hashValue(MONTHLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + MONTHLYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.LOGINEDKEY + CommonParams.AREAKEY + MONTHLYKEY, _tuple._14, jedis)
              }
            }
            if (!bloomFilterFlagYearly) {
              BloomFilter.hashValue(YEARLYKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + YEARLYKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.LOGINEDKEY + CommonParams.AREAKEY + YEARLYKEY, _tuple._14, jedis)
              }
            }
            if (!bloomFilterFlagForever) {
              BloomFilter.hashValue(CommonParams.FOREVERKEY + CommonParams.BLOOMFILTERKEY, userFlag, jedis)
              jedis.incr(CommonParams.LOGINEDKEY + CommonParams.ONLINEKEY + CommonParams.FOREVERKEY)
              if (_tuple._14 != null && !_tuple._14.isEmpty) {
                mapHincr(CommonParams.LOGINEDKEY + CommonParams.AREAKEY + CommonParams.FOREVERKEY, _tuple._14, jedis)
              }
              if (_tuple._7 != null && !_tuple._7.isEmpty) {
                mapHincr(CommonParams.LOGINEDKEY + CommonParams.OSKEY + CommonParams.FOREVERKEY, _tuple._7, jedis)
              }
              if (_tuple._8 != null && !_tuple._8.isEmpty) {
                mapHincr(CommonParams.LOGINEDKEY + CommonParams.MODELKEY + CommonParams.FOREVERKEY, _tuple._8, jedis)
              }
              if (_tuple._9 != null && !_tuple._9.isEmpty) {
                mapHincr(CommonParams.LOGINEDKEY + CommonParams.CHANNELKEY + CommonParams.FOREVERKEY, _tuple._9, jedis)
              }
            }
          }
        })
        jedis.close()
      })
    })
  }

  /**
    * ***************************************搜索词统计***************************************
    * @param formattedSearchRDD
    */
  def analyzeSearch(formattedSearchRDD: DStream[(String, String, String, String, String, String)]): Unit = {
    formattedSearchRDD.foreachRDD(userTracksRDD => {
      userTracksRDD.foreachPartition(iter => {
        /** 获取日、周、月、年的标识符 **/
        val DAILYKEY: String = DateUtil.getDateNow()
        val WEEKLYKEY: String = DateUtil.getNowWeekStart() + "_" + DateUtil.getNowWeekEnd()
        val MONTHLYKEY: String = DateUtil.getMonthNow()
        val YEARLYKEY: String = DateUtil.getYearNow()
        /** 获取jedis连接 **/
        val jedis: JedisCluster = RedisUtil.getJedisCluster
        iter.foreach(_tuple => {
          if (!_tuple._6.isEmpty) {
            mapHincr(CommonParams.SEARCHKEY + DAILYKEY, _tuple._6, jedis)
            mapHincr(CommonParams.SEARCHKEY + WEEKLYKEY, _tuple._6, jedis)
            mapHincr(CommonParams.SEARCHKEY + MONTHLYKEY, _tuple._6, jedis)
            mapHincr(CommonParams.SEARCHKEY + YEARLYKEY, _tuple._6, jedis)
            mapHincr(CommonParams.SEARCHKEY + CommonParams.FOREVERKEY, _tuple._6, jedis)
          }
        })
        jedis.close()
      })
    })
  }

}
