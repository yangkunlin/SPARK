package utils.connection

import java.util

import common.CommonParams
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
  * Description: 
  *
  * @author YKL on 2018/5/17.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object RedisUtil {

  def getJedisCluster(): JedisCluster = {
    val jedisClusterNodes = new util.HashSet[HostAndPort]
    //Jedis Cluster will attempt to discover cluster nodes automatically
    jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._1, CommonParams.REDISCLUSTERPORT._1))
    jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._1, CommonParams.REDISCLUSTERPORT._2))
    jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._2, CommonParams.REDISCLUSTERPORT._1))
    jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._2, CommonParams.REDISCLUSTERPORT._2))
    jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._3, CommonParams.REDISCLUSTERPORT._1))
    jedisClusterNodes.add(new HostAndPort(CommonParams.REDISCLUSTERHOST._3, CommonParams.REDISCLUSTERPORT._2))
    val jc: JedisCluster = new JedisCluster(jedisClusterNodes)
    jc
  }

//  def getJedis(): Jedis = {
//    val jc: Jedis = new Jedis(CommonParams.REDISHOST, CommonParams.REDISPORT)
//    jc
//  }

}
