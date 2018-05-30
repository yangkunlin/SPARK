package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.joda.time.DateTime

/**
  * @author YKL on 2018/3/21.
  * @version 1.0
  *          说明：
  *          XXX
  */
object DateUtil {
  //  def dateHour(milliseconds: Long):DateHour = {
  //    val dateTime = new DateTime(milliseconds)
  //    DateHour(dateTime.toString("yyyy-MM-dd"), dateTime.toString("H"))
  //  }

  /**
    * 返回日期加上gu_id最后一位，作为log文件的保存目录
    *
    * @param milliseconds
    * @param gu_id
    * @return
    */
  def dateGuidPartitions(milliseconds: Long, gu_id: String): String = {
    val dateTime = new DateTime(milliseconds)
    val date = dateTime.toString("yyyy-MM-dd")
    val gu_hex = (gu_id.last).toLower
    s"date=${date}/gu_hash=${gu_hex}"
  }

  /**
    * 返回 yyyy-MM-dd 格式的日期
    *
    * @param milliseconds
    * @return
    */
  def dateStr(milliseconds: Long): String = {
    val dateTime = new DateTime(milliseconds)
    dateTime.toString("yyyy-MM-dd")
  }

  /**
    * 返回 yyyy-MM-dd HH:mm:ss 格式的日期
    *
    * @param milliseconds
    * @return
    */
  def dateHMSStr(milliseconds: Long): String = {
    val dateTime = new DateTime(milliseconds)
    dateTime.toString("yyyy-MM-dd HH:mm:ss")
  }

  /**
    * 接受一个时间戳的参数，返回日期和小时
    *
    * @param milliseconds
    * @return
    */
  def dateHourStr(milliseconds: Long): (String, String) = {
    val dateTime = new DateTime(milliseconds)
    (dateTime.toString("yyyy-MM-dd"), dateTime.toString("H"))
  }

  def getTimeNow(): Long = {
    val now: DateTime = new DateTime()
    now.getMillis
  }

  /**
    *
    * @return 返回当前的日期串
    */
  def getDateNow(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dt = dateFormat.format(now)
    dt
  }

  /**
    *
    * @return 返回当前的月份
    */
  def getMonthNow(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    val dt = dateFormat.format(now)
    dt
  }

  /**
    *
    * @return 返回当前的年份
    */
  def getYearNow(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy")
    val dt = dateFormat.format(now)
    dt
  }

  /**
    * 指定日期和间隔天数，返回指定日期前N天的日期 date - N days
    *
    * @param dt
    * @param interval
    * @return
    */
  def getDaysBefore(dt: Date, interval: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);

    cal.add(Calendar.DATE, -interval)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }


  /**
    * 指定日期和间隔天数，返回指定日期前N天的日期： date + N days
    *
    * @param dt
    * @param interval
    * @return
    */
  def getDaysLater(dt: Date, interval: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);

    cal.add(Calendar.DATE, +interval)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  /**
    * 2017-01-17  A Week Ago is 2017-01-07
    *
    * @return
    */
  def getWeekAgoDateStr(): String = {
    val dt: Date = new Date()
    val dtStr = getDaysBefore(dt, 7)
    dtStr
  }

  /**
    * 2017-01-17 A Week Later is  2017-01-21
    *
    * @return
    */
  def getWeekLaterDateStr(): String = {
    val dt: Date = new Date()
    val dtStr = getDaysLater(dt, 7)
    dtStr
  }

  def getYesterday(): String = {
    // Calendar.DATE
    val dt: Date = new Date()
    val yesterday = getDaysBefore(dt, 1)
    return yesterday
  }

  /**
    *
    * @param arg
    * @return
    */
  def getMillisecondsYMD(arg: String): Long = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.parse(arg).getTime
  }

  def getMillisecondsYMDHMS(arg: String): Long = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.parse(arg).getTime
  }

  /**
    *
    * @return 获取本周一的日期
    */
  def getNowWeekStart(): String = {
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    val period = df.format(cal.getTime())
    period
  }

  /**
    *
    * @return 获取本周末的日期
    */
  def getNowWeekEnd(): String = {
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    //    cal.add(Calendar.WEEK_OF_YEAR, 1)
    val period = df.format(cal.getTime())
    period
  }

  /**
    *
    * @return 获取上周一的日期
    */
  def getLastWeekStart(): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    val period = getDaysBefore(cal.getTime(), 7)
    period
  }

  /**
    *
    * @return 获取上周末的日期
    */
  def getLastWeekEnd(): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    //    cal.add(Calendar.WEEK_OF_YEAR, 1)
    val period = getDaysBefore(cal.getTime(), 7)
    period
  }

  /**
    *
    * @return 本月的第一天
    */
  def getNowMonthStart(): String = {
    val cal: Calendar = Calendar.getInstance();
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    val period = df.format(cal.getTime()) //本月第一天
    period
  }

  /**
    *
    * @return 本月的最后一天
    */
  def getNowMonthEnd(): String = {
    val cal: Calendar = Calendar.getInstance();
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE, -1)
    val period = df.format(cal.getTime()) //本月最后一天
    period
  }

  //  def main(args: Array[String]): Unit = {
  //    println(getMilliseconds("01/Jan/1970:08:01:00"))
  //  }

}
