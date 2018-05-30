package utils.connection

import java.util

import common.CommonParams
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis._

/**
  * Description: 
  *
  * @author YKL on 2018/5/17.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object RedisUtil {


  val poolConfig = new GenericObjectPoolConfig()
  // 最大连接数
  poolConfig.setMaxTotal(200)
  // 最大空闲数
  poolConfig.setMaxIdle(200)
  // 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：
  // Could not get a resource from the pool
  poolConfig.setMaxWaitMillis(1000)
  //在这里搞一个redis的连接池，可以是懒加载的，不用就不加载，可以是私有的，通过方法来得到我的连接对象
  private lazy val Jpool = new JedisPool(poolConfig, CommonParams.REDISCLUSTERHOST._1, CommonParams.REDISCLUSTERPORT._1)

  val jedisClusterNodes = new util.HashSet[HostAndPort]
  //Jedis Cluster will attempt to discover cluster nodes automatically
  jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._1, CommonParams.REDISCLUSTERPORT._1))
//  jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._1, CommonParams.REDISCLUSTERPORT._2))
  jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._2, CommonParams.REDISCLUSTERPORT._1))
//  jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._2, CommonParams.REDISCLUSTERPORT._2))
  jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._3, CommonParams.REDISCLUSTERPORT._1))
//  jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._3, CommonParams.REDISCLUSTERPORT._2))
  def getJedisCluster: JedisCluster = {
    val jc: JedisCluster = new JedisCluster(jedisClusterNodes)
    jc
  }


  def getJedis:Jedis ={Jpool.getResource}

}
