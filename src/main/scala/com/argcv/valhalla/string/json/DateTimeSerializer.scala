package com.argcv.valhalla.string.json

import net.liftweb.json._
import org.joda.time.DateTime

class DateTimeSerializer extends net.liftweb.json.Serializer[DateTime] {
  private val DateTimeClass = classOf[DateTime]

  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), DateTime] = {
    case (TypeInfo(DateTimeClass, _), json) => json match {
      case JInt(millis) => new DateTime(millis.longValue())
      case x => throw new MappingException("Can't convert " + x + " to DateTime")
    }
  }

  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case t: DateTime => JInt(t.getMillis)
  }
}

object DateTimeSerializer {
  lazy val instance = new DateTimeSerializer

  def apply(): DateTimeSerializer = instance
}