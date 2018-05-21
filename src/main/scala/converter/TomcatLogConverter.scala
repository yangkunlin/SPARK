package converter

import schema.TomcatLogSchema

/**
  * @author YKL on 2018/3/19.
  * @version 1.0
  *          说明：
  *          XXX
  */
class TomcatLogConverter extends AbstractConverter {
  override def convert(str: String): String = {

    val tomcatLogSchema = new TomcatLogSchema()

    var tomcatLogMap = Map[String, String]()

    val items = str.split(" ")

    if(items.length ==11) {
      tomcatLogMap += ("accessIp" -> items(0))
      tomcatLogMap += ("accessTime" -> items(3).substring(1))
      tomcatLogMap += ("accessType" -> items(5).substring(1))
      tomcatLogMap += ("accessResource" -> items(6).substring(0, items(6).length -1))
      tomcatLogMap += ("accessState" -> items(8))
      tomcatLogMap += ("accessFlow" -> items(9))
      tomcatLogMap += ("usedTime" -> items(10))
    } else if(items.length == 10) {
      tomcatLogMap += ("accessIp" -> items(0))
      tomcatLogMap += ("accessTime" -> items(3).substring(1))
      tomcatLogMap += ("accessType" -> items(5).substring(1))
      tomcatLogMap += ("accessResource" -> items(6).substring(0, items(6).length -1))
      tomcatLogMap += ("accessState" -> items(8))
      tomcatLogMap += ("accessFlow" -> items(9))
    }

    if(tomcatLogMap != null) {
      tomcatLogSchema.setAccessIp(tomcatLogMap.getOrElse("accessIp", null))
      tomcatLogSchema.setAccessTime(tomcatLogMap.getOrElse("accessTime", null))
      tomcatLogSchema.setAccessType(tomcatLogMap.getOrElse("accessType", null))
      tomcatLogSchema.setAccessResource(tomcatLogMap.getOrElse("accessResource", null))
      tomcatLogSchema.setAccessState(tomcatLogMap.getOrElse("accessState", null))
      tomcatLogSchema.setAccessFlow(tomcatLogMap.getOrElse("accessFlow", null))
      tomcatLogSchema.setUsedTime(tomcatLogMap.getOrElse("usedTime", null))
      return tomcatLogSchema.toString()
    } else {
      return null
    }




  }
}
