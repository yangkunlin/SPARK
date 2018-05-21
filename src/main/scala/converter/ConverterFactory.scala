package converter

import org.slf4j.LoggerFactory

/**
  * @author YKL on 2018/3/19.
  * @version 1.0
  *          说明：
  *          XXX
  */
object ConverterFactory {

  private val logger = LoggerFactory.getLogger(ConverterFactory.getClass)

  def getConverter(topic: String): Option[AbstractConverter] = {

    var converter: Option[AbstractConverter] = None

    if(topic.equals("tomcatLog")) {
      converter = Option(new TomcatLogConverter)
    } else {
      logger.warn("invalid topic: ", topic)
    }
    //返回converter
    converter
  }

}
