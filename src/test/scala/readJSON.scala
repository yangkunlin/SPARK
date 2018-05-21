import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author YKL on 2018/4/8.
  * @version 1.0
  *          说明：
  *          XXX
  */
object readJSON {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("E:\\test.json")
    rdd.map(line => {
      //创建json解析器
      val jsonParser = new JSONParser()
      //将string转化为jsonObject
      val jsonObj: JSONObject = jsonParser.parse(line).asInstanceOf[JSONObject]

      //获取所有键
      val jsonKey = jsonObj.keySet()

      val iter = jsonKey.iterator()
      var map: Map[String, String] = Map()
      while (iter.hasNext) {
        val field = iter.next()
        val value = jsonObj.get(field).toString
        map += (field -> value)
      }
      map
    })
      .collect().foreach(println)

  }

}
