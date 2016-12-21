package com.argcv.valhalla.rec

import com.argcv.valhalla.utils.Awakable

import scala.collection.mutable.{ Map => MMap }

/**
 *
 * @author Yu Jing <yu@argcv.com> on 12/21/16
 */
object BasicCollaborativeFiltering extends Awakable {
  def demo: BasicCollaborativeFilteringColl = {
    val tec = BasicCollaborativeFilteringColl()
    tec.addSelfEid("e1", 1.0)
    tec.addSelfEid("e2", 1.0)
    tec.addSelfEid("e3", 1.0)
    tec.addSelfEid("e7", -1.0)

    tec.addElem("e2", "u3", 1.0)
    tec.addElem("e3", "u3", 1.0)
    tec.addElem("e4", "u3", 1.0)

    tec.addElem("e2", "u4", 1.0)
    tec.addElem("e3", "u4", 1.0)
    tec.addElem("e7", "u4", 1.0)
    tec.addElem("e9", "u4", 1.0)
    tec
  }

  /**
   * @author yu
   * @param m elem-user map : Map&lt;ElemId, Map&lt;UserId, Score&gt;&gt;
   * @param i user liked map : key: eid, value: Score
   * @param c other user ids, key: uid, value: count
   * @param z zero score
   */
  case class BasicCollaborativeFilteringColl(
    m: MMap[String, MMap[String, Double]],
    c: MMap[String, Int],
    i: MMap[String, Double],
    z: Double = 0) {
    def computeScale() = m.size * c.size

    def addElem(eid: String, uid: String, score: Double): Unit = this.synchronized {
      if (!m.contains(eid)) {
        m.put(eid, MMap[String, Double]())
      }
      m(eid).put(uid, score)
      c.put(uid, c.getOrElse(uid, 0) + 1)
    }

    def rmElem(eid: String, uid: String): Unit = this.synchronized {
      if (m.contains(eid)) {
        m(eid).remove(uid) // remove link
        if (m(eid).isEmpty) {
          m.remove(eid)
        }
        val cuc = c.getOrElse(uid, 0)
        if (cuc < 2) {
          c.remove(uid)
        } else {
          c.put(uid, cuc - 1)
        }
      }
    }

    /**
     * get top ranked eid and score
     *
     * @param n max return numbers
     * @return
     */
    def rec(n: Int): List[(String, Double)] = {
      val uids = c.keys.toList
      val sims = uids.par.map(cu => cu -> sim(cu)).toMap
      getEids.par.
        filterNot(i.contains).
        map { ce => // current eid
          val scores = uids.foldLeft((0.0, 0.0)) { (sc, cu) => // score, current uid
            val cscore = score(ce, cu)
            val csim: Double = sims(cu)
            (sc._1 + csim * cscore, sc._2 + csim)
          }
          (ce, if (scores._2 == 0) 0.0
          else scores._1 / scores._2)
        }.toList.
        sortWith(_._2 > _._2).
        take(n)
    }

    /**
     * similarity of uid and i in eid
     *
     * @param uid user id
     * @return
     */
    def sim(uid: String) = {
      val scores = getEids.par.
        foldLeft((0.0, 0.0, 0.0)) { (l, ce) =>
          val ci = i.getOrElse(ce, z)
          val cn = score(ce, uid)
          (l._1 + ci * cn, l._2 + ci * ci, l._3 + cn * cn)
        }
      if (scores._2 == 0.0 || scores._3 == 0.0) {
        0.0
      } else {
        scores._1 / (Math.sqrt(scores._2) * Math.sqrt(scores._3))
      }
    }

    /**
     * score of eid and uid
     *
     * @param eid elem id
     * @param uid user id
     * @return
     */
    def score(eid: String, uid: String): Double = {
      if (m.contains(eid)) {
        m(eid).getOrElse(uid, z)
      } else {
        z
      }
    }

    def getEids = this.m.keys.toList

    def addSelfEid(eid: String, score: Double): Unit = i.synchronized {
      i.put(eid, score)
      //    if (!m.contains(eid)) {
      //      m.put(eid, MMap[String, Double]())
      //    }
    }
  }

  object BasicCollaborativeFilteringColl {
    def apply(): BasicCollaborativeFilteringColl = {
      BasicCollaborativeFilteringColl(
        m = MMap[String, MMap[String, Double]](),
        c = MMap[String, Int](),
        i = MMap[String, Double]()
      )
    }
  }

}
