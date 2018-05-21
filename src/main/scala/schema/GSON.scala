package schema

import com.google.gson.Gson

/**
  * @author YKL on 2018/3/19.
  * @version 1.0
  *          说明：
  *          XXX
  */
object GSON {

  val gson = new Gson()

  def toJSON(obj: Any): String = {
    gson.toJson(obj)
  }

}
