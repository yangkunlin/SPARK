package ml.ml_demo.tf_idf

import utils.ikanalyzer.WordSplit
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author YKL on 2018/4/9.
  * @version 1.0
  *          说明：
  *          XXX
  */

case class RawDataRecord(category: String, text: String)

object WordAnalyze {


  def main(args: Array[String]): Unit = {

//    LoggerLevels.setLogLevels()

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordAnalyze")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var i = 0L

    //将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
    var srcDF = sc.textFile("E:\\321.txt")
      .filter(line => {
        line.contains("搜狐体育讯")
      })
      .map(line => {
        i = i + 1
        RawDataRecord(i.toString, WordSplit.wordSplit(line).trim)
      }).toDF
//    srcDF.select("category", "text").take(1).foreach(println)

    //将分好的词转换为数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(srcDF)
//    wordsData.select($"category", $"text", $"words").take(1).foreach(println)

    //将每个词转换成Int型，并计算其在文档中的词频（TF）
    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
    //.setNumFeatures(50)
    var featurizedData = hashingTF.transform(wordsData)
    featurizedData.select($"category", $"words", $"rawFeatures").take(4).foreach(println)

    //计算TF-IDF值
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
//    rescaledData.select($"category", $"words", $"features").take(2).foreach(println)

    var trainDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
//    trainDataRdd.take(1).foreach(println)

  }



}
