package utils.connection

import java.util

import common.CommonParams
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

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
    jedisClusterNodes.add(new HostAndPort(CommonParams.REDISHOST, CommonParams.REDISPORT))
    val jc: JedisCluster = new JedisCluster(jedisClusterNodes)
    jc
  }

  def getJedis(): Jedis = {
    val jc: Jedis = new Jedis(CommonParams.REDISHOST, CommonParams.REDISPORT)
    jc
  }

}
