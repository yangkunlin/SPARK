package utils.connection

import common.CommonParams
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @author YKL on 2018/3/23.
  * @version 1.0
  *          说明：
  *          XXX
  */
object KafkaUtil {

  def getStreamByKafka(ssc: StreamingContext, topic: Array[String], group: String): InputDStream[ConsumerRecord[String, String]] = {



    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> CommonParams.KAFKASERVERS,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
//      "max.poll.records" -> "100",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    //创建DStream，返回接收到的输入数据
    val stream = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent, Subscribe[String,String](topic,kafkaParam))

    stream

  }

}
