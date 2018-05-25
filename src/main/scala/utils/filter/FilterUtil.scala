package utils.filter

/**
  * @author YKL on 2018/3/21.
  * @version 1.0
  *          说明：
  *          XXX
  */
object FilterUtil {
  def fieldsLengthFilter(line: String,splitStr: String, length: Int): Boolean = {
    val fields = line.split(splitStr)
    if (fields.length == length)
      true
    else false
  }

  def existFilter(line: String): Boolean = {
    val items = line.split(" ")
    if (items(6).contains("callback"))
      false
    else
      true
  }

  def timeFilter(line: String): Boolean = {
    val items = line.split(" ")
    if(items.length > 3) {
      if(items(3).contains("2018"))
        true
      else
        false
    } else false
  }

  def pathFilter(str: String): Boolean = {
    val invaildPath = Set("/Token")
    !invaildPath.contains(str)
  }

}
