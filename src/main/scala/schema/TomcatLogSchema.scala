package schema

import com.google.gson.Gson

/**
  * @author YKL on 2018/3/19.
  * @version 1.0
  *          说明：
  *          XXX
  */
class TomcatLogSchema {

  //访问IP
  private var accessIp : String = null

  def setAccessIp(accessIp : String): Unit = {
    this.accessIp = accessIp
  }

  def getAccessIp(): String = {
    this.accessIp
  }

  //访问时间
  private var accessTime : String = null

  def setAccessTime(accessTime : String): Unit = {
    this.accessTime = accessTime
  }

  def getAccessTime(): String = {
    this.accessTime
  }

  //访问方式GET or POST
  private var accessType : String = null

  def setAccessType(accessType : String): Unit = {
    this.accessType = accessType
  }

  def getAccessType(): String = {
    this.accessType
  }

  //访问资源
  private var accessResource : String = null

  def setAccessResource(accessResource : String): Unit = {
    this.accessResource = accessResource
  }

  def getAccessResource(): String = {
    this.accessResource
  }

  //访问状态
  private var accessState : String = null

  def setAccessState(accessState : String): Unit = {
    this.accessState = accessState
  }

  def getAccessState(): String = {
    this.accessState
  }

  //访问该资源所需流量
  private var accessFlow : String = null

  def setAccessFlow(accessFlow : String): Unit = {
    this.accessFlow = accessFlow
  }

  def getAccessFlow(): String = {
    this.accessFlow
  }

  //访问所用时间
  private var usedTime : String = null

  def setUsedTime(usedTime : String): Unit = {
    this.usedTime = usedTime
  }

  def getUsedTime(): String = {
    this.usedTime
  }


  override def toString(): String = {
    GSON.toJSON(this)
  }
}
