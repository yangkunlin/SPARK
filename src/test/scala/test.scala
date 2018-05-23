import utils.DateUtil

/**
  * Description: 
  *
  * @author YKL on 2018/5/21.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object test {

  def main(args: Array[String]): Unit = {

    println(DateUtil.getLastWeekStart() + DateUtil.getLastWeekEnd())

  }

}
