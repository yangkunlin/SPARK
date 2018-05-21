package ml.ml_demo.feature_selector

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @author YKL on 2018/4/26.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object ChiSqSelector {

  def chiSqSelector(sparkSession: SparkSession) = {


    //构造数据集
    val data = Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )
    val df = sparkSession.createDataFrame(data).toDF("id", "features", "clicked")
    df.select("id", "features","clicked").show()

    //使用卡方检验，将原始特征向量（特征数为4）降维（特征数为3）
    val selector = new ChiSqSelector().setNumTopFeatures(3).setFeaturesCol("features").setLabelCol("clicked").setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)
    result.show()

  }

}
