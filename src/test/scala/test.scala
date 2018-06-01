import ml.ml_demo.modelCF.ModelCF
import org.apache.spark.sql.SparkSession

/**
  * Description: 
  *
  * @author YKL on 2018/5/21.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("HbaseTest")
      .config("spark.sql.warehouse.dir", "spark-path")
      .master("local[*]")
      .getOrCreate()

    ModelCF.modelCF(spark)

  }

}
