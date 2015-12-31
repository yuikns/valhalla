package com.argcv.valhalla.net

import com.argcv.valhalla.net.DataSerializer._
import org.scalatest._

/**
 * test case of [[com.argcv.valhalla.net.DataSerializer]]
 */
class DataSerializerSpec extends FlatSpec with Matchers {
  "Data" should " convert between Long and Array[Byte]" in {
    val l: Long = 10000
    val bl: Array[Byte] = l.getByteArray
    bl should be(10000L.getByteArray)
    bl.length should be(8)
    bl.toLong should be(10000L)
  }

  it should " convert between Int and Array[Byte]" in {
    val i: Int = 10000
    val bi: Array[Byte] = i.getByteArray
    bi should be(10000.getByteArray)
    bi.length should be(4)
    bi.toInt should be(10000)
  }

  it should " convert between Double and Array[Byte]" in {
    val d: Double = 3.1415926
    val bd: Array[Byte] = d.getByteArray
    bd should be(d.getByteArray)
    bd.length should be(8)
    bd.toDouble should be(d)
  }

  it should " convert between Char and Array[Byte]" in {
    val c: Char = '我'
    val bc: Array[Byte] = c.getByteArray
    bc should be(c.getByteArray)
    bc.length should be(2)
    bc.toChar should be(c)
  }

}
