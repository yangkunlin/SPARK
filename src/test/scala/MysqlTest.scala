import off_line.service.MysqlServiceImpl
import org.apache.spark.sql.SparkSession

/**
  * @author YKL on 2018/4/6.
  * @version 1.0
  *          说明：
  *          XXX
  */
object MysqlTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("HbaseTest")
      .config("spark.sql.warehouse.dir", "spark-path")
      .master("local[*]")
      .getOrCreate()

    MysqlServiceImpl.getOneDayVORDD(spark).collect.foreach(println)
    spark.close()
  }

}
