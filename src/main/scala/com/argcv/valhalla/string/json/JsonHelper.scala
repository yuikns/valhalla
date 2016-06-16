package com.argcv.valhalla.string.json

import com.argcv.valhalla.exception.ExceptionHelper.SafeExecWithTrace
import com.argcv.valhalla.reflect.ReflectHelper
import com.google.common.base.CaseFormat

/**
 * @author yu
 */
trait JsonHelper {

  def toJson(a: Any, compact: Boolean = true) = {
    implicit val formats = JsonHelper.jsonFormatsWithDateTime
    if (compact) net.liftweb.json.Printer.compact(net.liftweb.json.JsonAST.render(
      net.liftweb.json.Extraction.decompose(a)))
    else net.liftweb.json.Printer.pretty(net.liftweb.json.JsonAST.render(
      net.liftweb.json.Extraction.decompose(a)))
  }

  def fromJson(s: String) = {
    implicit val formats = JsonHelper.jsonFormatsWithDateTime
    net.liftweb.json.JsonParser.parse(s).values
  }

  /**
   * convert one string to some other type
   *
   * @param s string to convert
   */
  implicit class JsonConverter(val s: String) {
    def toSnakeCase = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, s)

    /**
     * @param or or else ?
     * @return
     */
    def safeToIntOrElse(or: Int) = safeToInt match {
      case Some(v: Int) => v
      case None => or
    }

    /**
     * @return
     */
    def safeToInt = scala.util.control.Exception.catching(classOf[java.lang.NumberFormatException]) opt s.toInt

    /**
     * @param or or else
     * @return
     */
    def safeToLongOrElse(or: Long) = safeToLong match {
      case Some(v: Long) => v
      case None => or
    }

    /**
     * @return
     */
    def safeToLong = scala.util.control.Exception.catching(classOf[java.lang.NumberFormatException]) opt s.toLong

    /**
     * @param or or else
     * @return
     */
    def safeToDoubleOrElse(or: Double) = safeToDouble match {
      case Some(v: Double) => v
      case None => or
    }

    /**
     * @return
     */
    def safeToDouble = scala.util.control.Exception.catching(classOf[java.lang.NumberFormatException]) opt s.toDouble

    /**
     * @param or or else
     * @return
     */
    def safeToBooleanOrElse(or: Boolean) = safeToBoolean match {
      case Some(v: Boolean) => v
      case None => or
    }

    /**
     * @return
     */
    def safeToBoolean = scala.util.control.Exception.catching(classOf[java.lang.IllegalArgumentException]) opt s.toBoolean

    /**
     * @param or or else
     * @return
     */
    def parseJsonToMapOrElse(or: Map[String, Any]) = parseJsonToMap match {
      case Some(v: Map[String, Any]) => v
      case None => or
    }

    /**
     * @return
     */
    def parseJsonToMap = scala.util.control.Exception.catching(classOf[java.lang.ClassCastException]) opt parseJson.asInstanceOf[Map[String, Any]]

    /**
     * @param or or else
     * @return
     */
    def parseJsonToListOrElse(or: List[Any]) = parseJsonToList match {
      case Some(v: List[Any]) => v
      case None => or
    }

    /**
     * @return
     */
    def parseJsonToList = scala.util.control.Exception.catching(classOf[java.lang.ClassCastException]) opt parseJson.asInstanceOf[List[Any]]

    /**
     * @return
     */
    def parseJson = {
      //implicit val formats = net.liftweb.json.DefaultFormats
      implicit val formats = JsonHelper.jsonFormatsWithDateTime
      net.liftweb.json.JsonParser.parse(s).values
    }

    /**
     * parse string type json to class T
     *
     * @tparam T type to apply
     * @return opt class
     */
    def parseJsonToClass[T: scala.reflect.ClassTag]: Option[T] = {
      //implicit val formats = net.liftweb.json.DefaultFormats
      implicit val formats = JsonHelper.jsonFormatsWithDateTime
      implicit val mf = ReflectHelper.classTag2Manifest[T]
      SafeExecWithTrace(net.liftweb.json.JsonParser.parse(s).extract[T])
    }
  }

  implicit class ToJson(a: Any) {
    def toJson: String = toJson()

    def toJson(compact: Boolean = true): String = JsonHelper.toJson(a, compact)
  }

}

object JsonHelper extends JsonHelper {
  lazy val jsonFormatsWithDateTime = net.liftweb.json.DefaultFormats + DateTimeSerializer()
}
