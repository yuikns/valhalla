package com.argcv.valhalla.ml.common

/**
 *
 * @author Yu Jing <yu@argcv.com> on 10/9/16
 */
case class LabeledPoint(label: Double, features: Vec) extends Serializable {
  override def toString: String = {
    s"($label,$features)"
  }

  def toSVMString = {
    s"$label ${features.toSVMString}"
  }
}