package com.argcv.valhalla.utils

/**
 * @author yu
 */
object CommonHelper {

  implicit class SafeGetOrElse[T](val o: Option[T]) {
    def safeGetOrElse(or: T): T = o match {
      case Some(v) => v
      case None => or
    }
  }

}
