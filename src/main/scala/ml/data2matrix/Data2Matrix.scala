package ml.data2matrix

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @author YKL on 2018/4/27.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object Data2Matrix {

  def data2Matrix(sparkSession: SparkSession) = {

    //一个StruceField你可以把它当成一个特征列。分别用列的名称和数据类型初始化
    val structFields = List(StructField("id", StringType), StructField("create_date", StringType),
      StructField("author", StringType), StructField("hits", StringType), StructField("name", StringType),
      StructField("video_excerpts", StringType), StructField("video_num_flower", StringType),
      StructField("video_num_vote", StringType), StructField("video_num_support", StringType),
      StructField("size", StringType), StructField("plat", StringType))
    //最后通过StructField的集合来初始化表的模式。
    val types = StructType(structFields)

    val rdd = sparkSession.sparkContext.textFile("E:\\data\\temp\\tb_video_info.csv")

    //Rdd的数据，里面的数据类型要和之前的StructField里面数据类型对应。否则会报错。
    val rowRdd = rdd.map(line => {
      val fields = line.split("\t")
      Row(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10))
    })
      .filter(row => row.get(4) != "" && row.get(5) != "")
    //.foreach(println)
    //通过SQLContext来创建DataFrame.
    val df = sparkSession.createDataFrame(rowRdd, types)

    //构造RFormula，指定Formula表达式为clicked ~ country + hour
    val formula = new RFormula().setFormula("hits ~ name + video_excerpts").setFeaturesCol("features").setLabelCol("label")
    //生成特征向量及label
    val output = formula.fit(df).transform(df)
    output.select("features", "label").show(false)
//    output.withColumn("vectors", Vectors.dense(output.select("features"))).show(false)
  }

}
