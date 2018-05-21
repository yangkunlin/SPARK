package converter

/**
  * @author YKL on 2018/3/19.
  * @version 1.0
  *          说明：
  *          XXX
  */
abstract class AbstractConverter extends Serializable {

  def convert(str: String): String

}
