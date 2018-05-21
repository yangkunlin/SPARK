package off_line

import converter.ConverterFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author YKL on 2018/3/20.
  * @version 1.0
  *          说明：
  *          XXX
  */
object TomcatLogSaveAsJson {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("LogAnalyze")
    val sc = new SparkContext(conf)

    val path = "E:\\data\\Feb\\*"

    //tomcatLog filting and formatting
    val converter = ConverterFactory.getConverter("tomcatLog")
    val logRDD = sc.textFile(path)
    //filter 过滤长度小于0， 过滤不包含GET与POST的URL
    val filtered = logRDD.filter(_.length() > 0).filter(line => (line.indexOf("GET") > 0 || line.indexOf("POST") > 0))

    val reformatted_str = filtered.map(rdd => converter.get.convert(rdd)).repartition(100).saveAsTextFile("E:\\result\\tomcatLog")


  }

}
