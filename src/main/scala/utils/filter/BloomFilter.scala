package utils.filter

import common.CommonParams
import utils.connection.RedisUtil

import scala.util.hashing.MurmurHash3


/**
  * Description:
  *
  * @author YKL on 2018/5/18.
  * @version 1.0
  * spark:梦想开始的地方
  */
object BloomFilter {

  //1 << 24位长度的位图数组，存放hash值
//  val bitSetSize = 1 << 32

  //位数组
//  val bitSet = new util.BitSet()

  //传入murmurhash中的seed的范围
  val seedNums = 8

  //将hash值传入redis bit
  val jedis = RedisUtil.getJedis();

  /**
    * 根据MurmurHash3计算哈希值，设置BitSet的值
    * @param str
    */
  def hashValue(str: String): Unit = {
    if (str != null && !str.isEmpty)
      for (i <- 1 to seedNums)
        //bitSet.set(Math.abs(MurmurHash3.stringHash(str, i)) % bitSetSize, true)
        jedis.setbit(CommonParams.REDISBLOOMFILTERKEY, Math.abs(MurmurHash3.stringHash(str, i)), true)
    else
      println("please input string with value")
//    println(str + " operate over " + jedis.toString)
//    jedis.close()
  }

  /**
    * 判断一个字符串是否存在于bloomFilter
    * @param str
    * @return
    */
  def exists(str: String): Boolean = {

    def existsRecur(str: String, seed: Int): Boolean = {

      val flag = Math.abs(MurmurHash3.stringHash(str, seed))

      if (str == null || str.isEmpty)
        false
      else if (seed > seedNums)
        true
      else if (!jedis.getbit(CommonParams.REDISBLOOMFILTERKEY, flag))
        false
      else
        existsRecur(str, seed + 1)
    }
    if (str == null || str.isEmpty)
      false
    else
      existsRecur(str, 1)
  }

}
