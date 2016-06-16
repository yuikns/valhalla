package com.argcv.valhalla.collection

import breeze.stats.distributions.Gaussian

import scala.reflect.ClassTag

object CollectionHelper {

  implicit class Filter[T: ClassTag](arr: Array[T]) {

    /**
     * @param mu mu
     * @param sigma sigma
     * @return
     */
    def normalize(v: (T) => Double, mu: Double, sigma: Double): Array[(T, Double)] = {
      arr.map(f => (f, v(f))).
        zipWithIndex.
        sortWith(_._1._2 < _._1._2).
        zip(Gaussian(mu, sigma).sample(arr.length).sortWith(_ < _)).
        sortWith(_._1._2 < _._1._2).
        map(f => Tuple2(f._1._1._1, f._2))
    }

    /**
     * @param v handler from v to real value
     * @param min minimze value
     * @param max maximize value
     * @return
     */
    def truncate(v: (T) => Double, min: Double, max: Double): Array[(T, Double)] = {
      arr.map { f =>
        Tuple2(
          f, {
            val rv = v(f)
            if (rv < min) min
            else if (rv > max) max
            else rv
          })
      }
    }
  }

}
