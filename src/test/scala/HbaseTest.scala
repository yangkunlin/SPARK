import off_line.service.HbaseServiceImpl
import org.apache.spark.sql.SparkSession

/**
  * @author YKL on 2018/4/12.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object HbaseTest {

  def main(args: Array[String]): Unit = {

//    HbaseServiceImpl.setResult("01-01", "2018-04-12", "555")

    val spark = SparkSession.builder
      .appName("HbaseTest")
      .config("spark.sql.warehouse.dir", "spark-path")
      .master("local[*]")
      .getOrCreate()

    HbaseServiceImpl.getUserTracksOfOneMonth(spark).show()

    spark.close()
  }

}
