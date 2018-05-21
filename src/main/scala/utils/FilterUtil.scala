package utils

/**
  * @author YKL on 2018/3/21.
  * @version 1.0
  *          说明：
  *          XXX
  */
object FilterUtil {
  def fieldsLengthFilter(line: String): Boolean = {
    val fields = line.split("\t")
    if (fields.length > 3)
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

}
