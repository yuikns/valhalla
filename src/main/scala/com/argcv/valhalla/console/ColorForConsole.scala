package com.argcv.valhalla.console

object ColorForConsole extends Enumeration {
  val BLACK, RED, GREEN, ORANGE, BLUE, PURPLE, CYAN, LIGHT_GRAY, DARK_GRAY, LIGHT_RED, LIGHT_GREEN, YELLOW, LIGHT_BLUE, LIGHT_PURPLE, LIGHT_CYAN, WHITE, NO_COLOR = Value

  implicit class Color4CDecor(c: ColorForConsole.Value) {
    lazy val escapeCode = c match {
      case BLACK => "\u001b[0;30m"
      case RED => "\u001b[0;31m"
      case GREEN => "\u001b[0;32m"
      case ORANGE => "\u001b[0;33m"
      case BLUE => "\u001b[0;34m"
      case PURPLE => "\u001b[0;35m"
      case CYAN => "\u001b[0;36m"
      case LIGHT_GRAY => "\u001b[0;37m"
      case DARK_GRAY => "\u001b[1;30m"
      case LIGHT_RED => "\u001b[1;31m"
      case LIGHT_GREEN => "\u001b[1;32m"
      case YELLOW => "\u001b[1;33m"
      case LIGHT_BLUE => "\u001b[1;34m"
      case LIGHT_PURPLE => "\u001b[1;35m"
      case LIGHT_CYAN => "\u001b[1;36m"
      case WHITE => "\u001b[1;37m"
      case NO_COLOR => "\u001b[0m"
      case _ => "\u001b[0m"
    }
  }

  implicit class StringWithColor(s: String) {
    def withColor(c: ColorForConsole.Value) = s"${c.escapeCode}$s${NO_COLOR.escapeCode}"
  }

}

