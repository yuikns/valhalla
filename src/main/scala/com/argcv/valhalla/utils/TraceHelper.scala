package com.argcv.valhalla.utils

import com.argcv.valhalla.console.ColorForConsole
import com.argcv.valhalla.console.ColorForConsole._

/**
 * @author yu
 */
object TraceHelper {
  def codeLineNumber(depth: Int = 1): Int = {
    Thread.currentThread().getStackTrace()(depth + 1).getLineNumber
  }

  def codeFileName(depth: Int = 1): String = {
    Thread.currentThread().getStackTrace()(depth + 1).getFileName
  }

  def codeMethodName(depth: Int = 1): String = {
    Thread.currentThread().getStackTrace()(depth + 1).getMethodName
  }

  implicit class StackTraceWithColor(e: StackTraceElement) {
    final def toColoredString = {
      val s = new StringBuffer()
      s.append(e.getClassName).append(".").append(e.getMethodName).append(" ")
      if (e.isNativeMethod) {
        s.append("<").append("Native Method".withColor(CYAN)).append(">")
      } else if (e.getFileName != null) {
        if (e.getLineNumber >= 0) {
          s.append("<").
            append(e.getFileName.withColor(CYAN)).
            append(":").
            append(e.getLineNumber.toString.withColor(ORANGE)).
            append(">")
        } else {
          s.append("<").
            append(e.getFileName.withColor(CYAN)).
            append(":").
            append("?".withColor(RED)).
            append(">")
        }
      } else {
        s.append("<").append("Unknown Source".withColor(CYAN)).append(">")
      }
      s.toString
    }

    final def toColoredStringWithHighLight(method: String, fname: String, lnum: Int) = {
      val s = new StringBuffer()
      val hflag = method == e.getMethodName && fname == e.getFileName && lnum == e.getLineNumber
      implicit class TryWithColor(v: String) {
        def tryWithColor(c: ColorForConsole.Value): String = {
          if (hflag) {
            v
          } else {
            v.withColor(c)
          }
        }
      }
      s.append(e.getClassName).append(".").append(e.getMethodName).append(" ")
      if (e.isNativeMethod) {
        s.append("<").append("Native Method".tryWithColor(CYAN)).append(">")
      } else if (e.getFileName != null) {
        if (e.getLineNumber >= 0) {
          s.append("<").
            append(e.getFileName.tryWithColor(CYAN)).
            append(":").
            append(e.getLineNumber.toString.tryWithColor(ORANGE)).
            append(">")
        } else {
          s.append("<").
            append(e.getFileName.tryWithColor(CYAN)).
            append(":").
            append("?".tryWithColor(RED)).
            append(">")
        }
      } else {
        s.append("<").append("Unknown Source".tryWithColor(CYAN)).append(">")
      }
      if (hflag) {
        s.toString.withColor(RED)
      } else {
        s.toString
      }
    }
  }

}
