package com.argcv.valhalla.exception

import scala.util.control.Exception

/**
 * @author yu
 */
object ExceptionHelper {
  implicit class ExceptionCatching[R](exec: () => R) {
    def safeExec: Option[R] = Exception.catching(classOf[Throwable]) opt exec()

    def safeExecWithMessage: (Boolean, String) = {
      try {
        exec()
        (true, "")
      } catch {
        case t: Throwable =>
          (false, t.getMessage)
      }
    }
  }

}
