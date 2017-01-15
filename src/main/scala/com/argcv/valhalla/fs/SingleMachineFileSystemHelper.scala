package com.argcv.valhalla.fs

import java.io.{ BufferedWriter, File, FileReader, FileWriter }
import java.nio.charset.CodingErrorAction

import breeze.io.CSVReader
import com.argcv.valhalla.client.LevelDBClient
import com.argcv.valhalla.exception.ExceptionHelper.SafeExecWithMessage
import com.argcv.valhalla.utils.Awakable

import scala.io.{ Codec, Source }

/**
 *
 * @author Yu Jing <yu@argcv.com> on 9/28/16
 */
trait SingleMachineFileSystemHelper extends Awakable {
  def mkdir(path: String) = {
    new File(path).mkdirs()
  }

  def rm(path: String, rmSelf: Boolean = true): (Boolean, String) = {
    SafeExecWithMessage {
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

  def getLines(path: String): Iterator[String] = {
    Source.fromFile(new File(path)).getLines()
  }

  def safeGetLines(path: String): Iterator[String] = {
    val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)
    Source.fromFile(new File(path))(decoder).getLines()
  }

  /**
   * @param path file path
   * @param body writer getter
   */
  def writeLines(path: String)(body: (BufferedWriter) => Unit): Unit = {
    val pw = new BufferedWriter(new FileWriter(new File(path)))
    body(pw)
    pw.flush()
    pw.close()
  }

  /**
   * @param path file path
   * @param body writer getter
   */
  def appendLines(path: String)(body: (BufferedWriter) => Unit): Unit = {
    val pw = new BufferedWriter(new FileWriter(new File(path), true))
    body(pw)
    pw.flush()
    pw.close()
  }

  def writeToLevelDB(path: String, cacheSize: Long = 0L)(body: (LevelDBClient) => Unit): Unit = {
    lazy val ldb = new LevelDBClient(path, cacheSize)
    body(ldb)
    ldb.close()
  }

  def csvIter(path: String,
    separator: Char = ',',
    quote: Char = '"',
    escape: Char = '\\',
    skipLines: Int = 0)(body: Iterator[String] => Boolean): Unit = {
    // based on "org.scalanlp" % "breeze_2.11" % "0.11.2"
    val file = new File(path)
    if (file.isFile) {
      val it = CSVReader.iterator(new FileReader(file), separator, quote, escape, skipLines)
      var stop = false
      while (it.hasNext && !stop) {
        stop = !body(it.next().toIterator)
      }
    }
  }

}

object SingleMachineFileSystemHelper extends SingleMachineFileSystemHelper
