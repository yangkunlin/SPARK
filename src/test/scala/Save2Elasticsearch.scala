import com.maxmind.geoip.LookupService
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @author YKL on 2018/3/29.
  * @version 1.0
  *          说明：
  *          XXX
  */
object Save2Elasticsearch {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("appName").master("local[*]")
      .config("spark.sql.warehouse.dir", "E:\\warehouse")
      .config("es.nodes", "123.206.60.24 :9200,123.206.41.189:9200") //设置es.nodes
      .config("pushdown", "true") //执行sql语句时在elasticsearch中执行只返回需要的数据。这个参数在查询时设置比较有用
      .config("es.index.auto.create", "true") //如果没有这个index自动创建
      .config("es.nodes.wan.only", "true")
      .getOrCreate()


    //      //广播规则，这个是由Driver向worker中广播规则
    //    val ipRulesBroadcast = spark.sparkContext.broadcast(lus)

    val rdd = spark.sparkContext.textFile("E:\\data\\all\\*").map(line => {
      val fields = line.split(" ")
      val ip = fields(0)
      (ip, "")
    }).groupByKey().map(tuple => {
      val lus = new LookupService("D:\\LogsAnalyze\\src\\main\\resources\\GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
      val ip = tuple._1
      try {
        val location = lus.getLocation(ip)
        (("ip", ip), ("location", location.latitude + "," + location.longitude))
      } catch {
        case ex: NullPointerException => ex.printStackTrace()
      }
    })
    EsSpark.saveToEs(rdd, "logstash_user_map/ip_location")
  }

}
