package ml.ml_demo.naive_bayes

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @author YKL on 2018/4/28.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object MyNaiveBayes {

  case class RawDataRecord(category: String, text: String)

  var operaGenreMap: Map[String, Double] = Map()
//    Map(
//    "豫剧" -> "1"
//    ,
//    "二夹弦" -> "2"
//    ,
//    "越剧" -> "3"
//    ,
//    "山东梆子" -> "4"
//    ,
//    "秦腔" -> "5"
//    ,
//    "晋剧" -> "6"
//    ,
//    "湘剧" -> "7"
//    ,
//    "淮剧" -> "8"
//    ,
//    "宛梆" -> "9"
//    ,
//    "太康道情" -> "10"
//    ,
//    "四平调" -> "11"
//    ,
//    "曲剧" -> "12"
//    ,
//    "徽剧" -> "13"
//    ,
//    "吕剧" -> "14"
//    ,
//    "泗州戏" -> "15"
//    ,
//    "昆曲" -> "16"
//    ,
//    "庐剧" -> "17"
//    ,
//    "五音戏" -> "18"
//    ,
//    "怀梆" -> "19"
//    ,
//    "柳琴戏" -> "20"
//    ,
//    "坠剧" -> "21"
//    ,
//    "粤剧" -> "22"
//    ,
//    "越调" -> "23"
//    ,
//    "综艺" -> "24"
//    ,
//    "锡剧" -> "25"
//    ,
//    "河北梆子" -> "26"
//    ,
//    "京剧" -> "27"
//    ,
//    "评剧" -> "28"
//    ,
//    "黄梅戏" -> "29"
//  )

  var i = 0

  def naiveBayes(sparkSession: SparkSession, srcRDD: RDD[RawDataRecord], tarRDD: RDD[RawDataRecord]) = {

    import sparkSession.implicits._

    //    val srcRDD = sparkSession.sparkContext.textFile("E:\\DATANALYSIS\\src\\main\\resources\\sougou\\*")
    //      .map(line => {
    //        val fields = line.split(",")
    //        RawDataRecord(fields(0), fields(1))
    //      })

    //70%作为训练数据，30%作为测试数据
//    val splits = srcRDD.randomSplit(Array(0.9, 0.1))
    val trainingDF = srcRDD.toDF()//splits(0).toDF()
    val testDF = tarRDD.toDF()//splits(1).toDF()

    //将词语转换成数组
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.select("category", "text", "words").take(1).foreach(println)
    wordsData.select("category").rdd.map(row => (row.get(0), 1)).reduceByKey(_ + _).foreach(_tuple => {
      i = i + 1
      operaGenreMap += (_tuple._1.toString -> i.toDouble)
    })

    //计算每个词在文档中的词频
    val hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(wordsData) //.filter(_.getAs("rawFeatures") == null)
    println("output2：")
    featurizedData.select("category", "rawFeatures").take(1).foreach(println)

    //计算每个词的TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    println("output3：")
    rescaledData.select("category", "features").take(1).foreach(println)

    //转换成Bayes的输入格式
    val trainDataRdd = rescaledData.select("category", "features")
      .filter(row => operaGenreMap.keys.toSet.contains(row.get(0)))
      .map {
        case Row(label: String, features: Vector) =>
          val num = operaGenreMap.get(label)
          LabeledPoint(num.get, Vectors.dense(features.toArray))
      }
    println("output4：")
    trainDataRdd.take(1).foreach(println)

    //训练模型
    val model = NaiveBayes.train(trainDataRdd.rdd, lambda = 1.0, modelType = "multinomial")
    model.save(sparkSession.sparkContext, "")

    //测试数据集，做同样的特征表示及格式转换
    val testwordsData = tokenizer.transform(testDF)
    val testfeaturizedData = hashingTF.transform(testwordsData)
    val testrescaledData = idfModel.transform(testfeaturizedData)
    val testDataRdd = testrescaledData.select("category", "features").rdd.map {
      case Row(label: String, features: Vector) =>
        val num = operaGenreMap.get(label)
        LabeledPoint(num.get, Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label)).foreach(p => {
      operaGenreMap.keys.foreach(key => {
        if (operaGenreMap(key).equals(p._1))
          println("预测是：" + key)
      })

    })

//    //统计分类准确率
//    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
//    println("output5：")
//    println(testaccuracy)

  }

}
