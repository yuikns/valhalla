package com.argcv.valhalla.exception

import com.argcv.valhalla.console.ColorForConsole._
import com.argcv.valhalla.utils.Awakable
import com.argcv.valhalla.utils.TraceHelper._

import scala.util.control.Exception

/**
 * @author yu
 */
object ExceptionHelper extends Awakable {

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

  object SafeExec {
    def apply[R](body: => R): Option[R] = Exception.catching(classOf[Throwable]) opt body
  }

  object SafeExecWithTrace {
    def apply[R](body: => R): Option[R] = {
      try {
        Some(body)
      } catch {
        case t: Exception =>
          val method: String = codeMethodName(2)
          val fname: String = codeFileName(2)
          val lnum: Int = codeLineNumber(2)
          logger.warn(s"${"#Exception#".withColor(RED)} :" +
            s" ${t.getMessage.withColor(LIGHT_RED)} ${method.withColor(CYAN)} <${fname.withColor(CYAN)}:${lnum.toString.withColor(ORANGE)}>" +
            s"\n${t.getStackTrace.map("\t@" + _.toColoredStringWithHighLight(method, fname, lnum)).mkString("\n")}\n")
          None
      }
    }
  }

  object SafeExecWithMessage {
    def apply[R](body: => R): (Boolean, String) = {
      try {
        body
        (true, "")
      } catch {
        case t: Exception =>
          (false, t.getMessage)
      }
    }
  }

}
