package ml.ml_demo.feature_selector

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @author YKL on 2018/4/26.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object VectorSlicer {

  def vectorSlicer(sparkSession: SparkSession) = {

    //构造特征数组
    val data = Array(Row(Vectors.dense(-2.0, 2.3, 0.0)))

    //为特征数组设置属性名（字段名），分别为f1 f2 f3
    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    //构造DataFrame
    val dataRDD = sparkSession.sparkContext.parallelize(data)
    val dataset = sparkSession.createDataFrame(dataRDD, StructType(Array(attrGroup.toStructField())))

    print("原始特征：")
    dataset.take(1).foreach(println)


    //构造切割器
    var slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    //根据索引号，截取原始特征向量的第1列和第3列
    slicer.setIndices(Array(0,2))
    print("output1: ")
    //org.apache.spark.sql.Row = [[-2.0,2.3,0.0],[-2.0,0.0]]
    slicer.transform(dataset).select("userFeatures", "features").first()

    //根据字段名，截取原始特征向量的f2和f3
    slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    slicer.setNames(Array("f2","f3"))
    print("output2: ")
    //org.apache.spark.sql.Row = [[-2.0,2.3,0.0],[2.3,0.0]]
    slicer.transform(dataset).select("userFeatures", "features").first()

    //索引号和字段名也可以组合使用，截取原始特征向量的第1列和f2
    slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    slicer.setIndices(Array(0)).setNames(Array("f2"))
    print("output3: ")
    //org.apache.spark.sql.Row = [[-2.0,2.3,0.0],[-2.0,2.3]]
    slicer.transform(dataset).select("userFeatures", "features").first()

  }

}
