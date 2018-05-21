package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * @author YKL on 2018/3/27.
  * @version 1.0
  *          说明：
  *          XXX
  */
object HBaseUtil {

  def getHBaseConf(): Configuration = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "bigdata-slave01,bigdata-slave02,bigdata-slave03")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    hbaseConf

  }

  def getHBaseConnection(): Connection = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "bigdata-slave01,bigdata-slave02,bigdata-slave03")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.defaults.for.version.skip", "true")
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf) //获取HBase连接,分区创建一个连接，分区不跨节点，不需要序列化
    return hbaseConn

  }

  def getHBaseTable(tableName: String): Table = {

    val userTable = TableName.valueOf(tableName)
    val admin = getHBaseConnection().getAdmin

    if (admin.tableExists(userTable)) {
      val table = getHBaseConnection().getTable(userTable)
      return table
    } else {
      val tableDesc = new HTableDescriptor(userTable)
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes))
      admin.createTable(tableDesc)
      val table = getHBaseConnection().getTable(userTable)
      return table
    }

  }


}
