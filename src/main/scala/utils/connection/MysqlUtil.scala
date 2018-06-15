package utils.connection

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

/**
  * @author YKL on 2018/4/4.
  * @version 1.0
  *          说明：
  *          XXX
  */
object MysqlUtil {


  def getMysqlDF(spark: SparkSession, tableName: String): DataFrame ={

    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://123.206.67.100:3306/xiyuanfinal?user=xiyuan&password=xiyuan@XIYUAN.2o18&rewriteBatchedStatements=true&allowMultiQueries=true&useSSL=false")
      .option("dbtable", tableName)
      .load
    jdbcDF

  }

  def append2Mysql(sc: SparkContext, rowRDD: RDD[Row], schema: StructType): Unit = {

    val sqlContext = new SQLContext(sc)

    val prop: Properties = getProperties

    val dataFrame = sqlContext.createDataFrame(rowRDD, schema)

    //将数据追加到数据库
    dataFrame.write.mode("append").jdbc("jdbc:mysql://123.206.48.254:9011/bigdata", "bigdata.result", prop)

  }

  private def getProperties = {
    //数据库相关属性
    val prop = new Properties()
    prop.put("user", "xiyuan")
    prop.put("password", "xiyuan@XIYUAN.2o18")
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop
  }
}
