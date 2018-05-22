package off_line

import org.apache.spark.{SparkConf, SparkContext}
import utils.filter.FilterUtil
import utils.IPUtils

/**
  * @author YKL on 2018/3/22.
  * @version 1.0
  *          说明：
  *          XXX
  */
object TomcatLogIpLocation {

  private val MONTH = "04"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("LogAnalyze")
    val sc = new SparkContext(conf)
    val path = "E:\\data\\Apr\\*"

    val rdd = sc.textFile(path)
    val cacheRDD = rdd.filter(line => FilterUtil.timeFilter(line)).filter(line => FilterUtil.existFilter(line)).cache()

    val ipRulesRdd = sc.textFile("E:\\data\\IP\\*").filter(line => FilterUtil.fieldsLengthFilter(line, "\t", 7)).map(lines =>{
      val fields = lines.split("\t")
      val start_num = fields(1)
      val end_num = fields(3)
      var location = ""
      if (fields.length > 5) {
        location = fields(5)
      } else location = "不清楚"
      var netType = ""
      if (fields.length == 7) {
        netType = fields(6)
      } else netType = "不清楚"

      (start_num, end_num, location, netType)
    })
    //全部的IP映射规则
    val ipRulesArrary = ipRulesRdd.collect()

    //广播规则，这个是由Driver向worker中广播规则
    val ipRulesBroadcast = sc.broadcast(ipRulesArrary)
    //用户地域分布情况
    val userLocationNumber = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessIp = items(0).trim
      (accessIp, "")
    }).groupByKey() //对每日相同用户去重
      .map(line => {
      val ip = line._1
      val ipNum = IPUtils.ip2Long(ip)
      val index = IPUtils.binarySearch(ipRulesBroadcast.value,ipNum)
      val info = ipRulesBroadcast.value(index)
      info
    }).map(t => {(t._3,1)}).reduceByKey(_+_).repartition(1).sortBy(_._2, false)
    userLocationNumber.saveAsTextFile("E:\\result\\" + MONTH + "用户地域分布情况")

    //每日在线用户地域分布情况
    val userOneDayLocationNumber = cacheRDD.map(line => {
      val items = line.split(" ")
      val accessIp = items(0).trim
      val dateTime = items(3).replace("[", "").split(":")(0)
      (accessIp + "-" + dateTime, "")
    }).groupByKey() //对每日相同用户去重
      .map(line => {
      val items = line._1.split("-")
      val ip = items(0)
      val ipNum = IPUtils.ip2Long(ip)
      val index = IPUtils.binarySearch(ipRulesBroadcast.value,ipNum)
      val info = ipRulesBroadcast.value(index)
      (items(1) + "-" + info._3, 1)
    }).reduceByKey(_+_).repartition(1).sortBy(_._2, false)
    userOneDayLocationNumber.saveAsTextFile("E:\\result\\" + MONTH + "每日在线用户地域分布情况")

    sc.stop()


  }

}
