package utils

/**
  * @author YKL on 2018/3/22.
  * @version 1.0
  * 说明：
  */
object IPUtils {

  //将IP转换成为Long类型
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //使用二分法，对IP进行查找，让ip与start_num以及end_num做对比
  def binarySearch(lines: Array[(String, String, String, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

}
