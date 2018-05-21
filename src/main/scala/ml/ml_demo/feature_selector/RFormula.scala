package ml.ml_demo.feature_selector

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession

/**
  * @author YKL on 2018/4/26.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object RFormula {

  def rformula(sparkSession: SparkSession) = {

    //构造数据集
    val dataset = sparkSession.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 1.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")
    dataset.select("id", "country", "hour", "clicked").show()

    val dataset2 = sparkSession.createDataFrame(Seq(
      (1, "US", 18, 0.0),
      (2, "CA", 12, 0.0),
      (3, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")

    //当需要通过country和hour来预测clicked时候，
    //构造RFormula，指定Formula表达式为clicked ~ country + hour
    val formula = new RFormula().setFormula("clicked ~ country + hour").setFeaturesCol("features").setLabelCol("label")
    //生成特征向量及label
    val output = formula.fit(dataset).transform(dataset)
    output.select("id", "country", "hour", "clicked", "features", "label")

  }

}
