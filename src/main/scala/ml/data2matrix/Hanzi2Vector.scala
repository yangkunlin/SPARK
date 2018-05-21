package ml.data2matrix

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.DataFrame

/**
  * @author YKL on 2018/4/27.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object Hanzi2Vector {

  def hanzi2Vector(input: DataFrame): DataFrame = {

    val word2vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)

    val model = word2vec.fit(input)

    return model.getVectors

  }

}
