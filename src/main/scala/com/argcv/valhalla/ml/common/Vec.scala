package com.argcv.valhalla.ml.common

//import spire.syntax.std.{ArrayOps => SArrayOps}

import spire.implicits.cfor

import scala.collection.{ AbstractSeq, Iterable, Seq, Traversable, TraversableOnce, immutable, mutable }
import scala.reflect.ClassTag

/**
 *
 * @author Yu Jing <yu@argcv.com> on 10/9/16
 */
@SerialVersionUID(1457976278L)
case class Vec(value: Array[Double]) extends Serializable {
  //lazy val ops = new SArrayOps[Double](value)

  lazy val sum: Double = sum(0, length)
  lazy val avg: Double = avg(0, length)
  lazy val min: (Int, Double) = min(0, length)
  lazy val max: (Int, Double) = max(0, length)
  lazy val variance: Double = variance(0, length)
  lazy val standardDeviation: Double = standardDeviation(0, length)
  lazy val sd: Double = standardDeviation

  def min(start: Int, end: Int): (Int, Double) = {
    val rs = 0 max start
    val re = length min end
    var o = rs
    var v = value(o)
    cfor(rs + 1)(_ < re, _ + 1) { i =>
      if (value(i) < v) {
        o = i
        v = value(i)
      }
    }
    (o, v)
  }

  def max(start: Int, end: Int): (Int, Double) = {
    val rs = 0 max start
    val re = length min end
    var o = rs
    var v = value(o)
    cfor(rs + 1)(_ < re, _ + 1) { i =>
      if (value(i) > v) {
        o = i
        v = value(i)
      }
    }
    (o, v)
  }

  def standardDeviation(start: Int, end: Int): Double = {
    Math.sqrt(variance(start, end))
  }

  def variance(start: Int, end: Int): Double = {
    val rs = 0 max start
    val re = length min end
    val avg = this.avg(rs, re)
    var result: Double = 0.0
    cfor(rs)(_ < re, _ + 1) { i =>
      result += Math.pow(value(i) - avg, 2)
    }
    result
  }

  def avg(start: Int, end: Int): Double = {
    if (length == 0) 0.0
    else sum(start, end) / length
  }

  def sum(start: Int, end: Int): Double = {
    val rs = 0 max start
    val re = length min end
    var result: Double = 0.0
    cfor(rs)(_ < re, _ + 1) { i =>
      result += value(i)
    }
    result
  }

  def length = value.length

  def size = value.length

  def map(f: Double => Double) = Vec(value.map(f))

  def zipWithIndex: Array[(Double, Int)] = value.zipWithIndex

  def foreach[U](f: Double => U) = value.foreach(f)

  def hasDefiniteSize: Boolean = true

  def exists(p: Double => Boolean): Boolean = value.exists(p)

  def find(p: Double => Boolean): Option[Double] = value.find(p)

  def nonEmpty: Boolean = !isEmpty

  def count(p: Double => Boolean): Int = value.count(p)

  def collectFirst[B](pf: PartialFunction[Double, B]): Option[B] = value.collectFirst[B](pf)

  def /:[B](z: B)(op: (B, Double) => B): B = foldLeft(z)(op)

  def :\[B](z: B)(op: (Double, B) => B): B = foldRight(z)(op)

  def foldRight[B](z: B)(op: (Double, B) => B): B =
    value.foldRight[B](z)(op)

  def reduceRightOption[B >: Double](op: (Double, B) => B): Option[B] = if (isEmpty) None else Some(reduceRight(op))

  def reduceRight[B >: Double](op: (Double, B) => B): B = value.reduceRight[B](op)

  def isEmpty: Boolean = value.isEmpty

  def reduce[A1 >: Double](op: (A1, A1) => A1): A1 = reduceLeft(op)

  def reduceLeft[B >: Double](op: (B, Double) => B): B =
    value.reduceLeft[B](op)

  def reduceOption[A1 >: Double](op: (A1, A1) => A1): Option[A1] =
    reduceLeftOption(op)

  def reduceLeftOption[B >: Double](op: (B, Double) => B): Option[B] =
    if (isEmpty) None else Some(reduceLeft(op))

  def fold[A1 >: Double](z: A1)(op: (A1, A1) => A1): A1 = foldLeft(z)(op)

  def foldLeft[B](z: B)(op: (B, Double) => B): B = {
    var result = z
    value foreach (x => result = op(result, x))
    result
  }

  def aggregate[B](z: => B)(seqop: (B, Double) => B, combop: (B, B) => B): B =
    foldLeft(z)(seqop)

  def seq: TraversableOnce[Double] = value.seq

  def toArray[B >: Double: ClassTag]: Array[B] = value.toArray[B]

  def toBuffer[B >: Double]: mutable.Buffer[B] = value.toBuffer[B]

  def toTraversable: Traversable[Double] = value.toTraversable

  def toList: List[Double] = value.toList

  def toIterable: Iterable[Double] = value.toIterable

  def toSeq: Seq[Double] = value.toSeq

  def toIndexedSeq: immutable.IndexedSeq[Double] = value.toIndexedSeq

  def toSet[B >: Double]: immutable.Set[B] = value.toSet

  def toVector: scala.Vector[Double] = value.toVector

  def toMap[T, U](implicit ev: Double <:< (T, U)): immutable.Map[T, U] = value.toMap[T, U](ev)

  def mkString: String = mkString("")

  override def toString = s"[${mkString(", ")}]"

  def mkString(sep: String): String = mkString("", sep, "")

  def mkString(start: String, sep: String, end: String): String = value.mkString(start, sep, end)

  def apply(q: Int) = value(q)

  def toSVMString = {
    val buffer = new StringBuffer()
    val re = length
    cfor(0)(_ < re, _ + 1) { i =>
      if (value(i) != 0) {
        buffer.append(s"${i + 1}:${value(i)} ")
      }
    }
    buffer.toString.trim
  }
}

object Vec {
  //def apply(value: Array[Double]): Vec = new Vec(value)

  def apply(value: AbstractSeq[Double]): Vec = new Vec(value.toArray)

}
