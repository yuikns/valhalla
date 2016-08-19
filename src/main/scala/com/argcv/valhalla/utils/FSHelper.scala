package com.argcv.valhalla.utils

import java.io.File

/**
  * helper for file system
  *
  */
object FSHelper {
  def mkdir(path: String) = {
    new File(path).mkdirs()
  }

  def rm(path: String, rmSelf: Boolean = true) = {
    def getRecursively(f: File): Seq[File] =
      f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles
    val froot = new File(path)
    if (froot.exists()) {
      if (froot.isDirectory) {
        getRecursively(froot).foreach { f =>
          if (!f.delete())
            throw new RuntimeException("Failed to delete " + f.getAbsolutePath)
        }
      }
      if (rmSelf) {
        if (!froot.delete())
          throw new RuntimeException("Failed to delete " + froot.getAbsoluteFile)
      }
    }
  }
}