package com.argcv.valhalla.concurrent

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.argcv.valhalla.console.ColorForConsole._
import com.argcv.valhalla.utils.Awakable

import scala.concurrent.{ ExecutionContext, Future }

/**
 * @author yu
 */
trait IterHelper extends Awakable {
  lazy val ITER_NAME = "base-iter"
  lazy val ITER_NAME_WITH_LABEL = ITER_NAME.withColor(LIGHT_BLUE)
  lazy val PAR_EXECUTION_CONTEXT_POOL_SIZE_MIN = 2
  lazy val PAR_EXECUTION_CONTEXT_POOL_SIZE_MAX = 256
  lazy val PAR_EXECUTION_CONTEXT_POOL_SIZE_FACTOR = 2.0
  lazy val PAR_EXECUTION_CONTEXT_POOL_QUEUE_SIZE = 2048
  lazy val PAR_EXECUTION_CONTEXT_POOL_CURRENT_SIZE = new AtomicInteger()

  lazy val PAR_EXECUTION_CONTEXT_POOL_SIZE = {
    val sz = (Runtime.getRuntime.availableProcessors() * PAR_EXECUTION_CONTEXT_POOL_SIZE_FACTOR).toInt max
      PAR_EXECUTION_CONTEXT_POOL_SIZE_MIN min
      PAR_EXECUTION_CONTEXT_POOL_SIZE_MAX
    logger.info(s"[$ITER_NAME_WITH_LABEL] fixed thread pool for foreach " +
      s"min:${PAR_EXECUTION_CONTEXT_POOL_SIZE_MIN.toString.withColor(GREEN)}, " +
      s"max:${PAR_EXECUTION_CONTEXT_POOL_SIZE_MAX.toString.withColor(RED)}, " +
      s"factor:${PAR_EXECUTION_CONTEXT_POOL_SIZE_FACTOR.toString.withColor(CYAN)}, " +
      s"real:${sz.toString.withColor(YELLOW)}")
    sz
  }

  lazy implicit val iterParExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(PAR_EXECUTION_CONTEXT_POOL_SIZE))

  def iter[T](queueSize: Int = PAR_EXECUTION_CONTEXT_POOL_QUEUE_SIZE)(body: => T): Future[T] = {
    while (PAR_EXECUTION_CONTEXT_POOL_CURRENT_SIZE.get() > queueSize) ()
    PAR_EXECUTION_CONTEXT_POOL_CURRENT_SIZE.incrementAndGet()
    Future {
      val resp: T = body
      PAR_EXECUTION_CONTEXT_POOL_CURRENT_SIZE.decrementAndGet()
      resp
    }(iterParExecutionContext)
  }

  object iter {
    def apply[T](body: => T): Future[T] = iter[T](PAR_EXECUTION_CONTEXT_POOL_QUEUE_SIZE)(body)

    def apply[T](body: => T, queueSize: Int): Future[T] =
      iter[T](queueSize)(body)
  }

}

/**
 *
 */

object IterHelper extends IterHelper {
  lazy implicit val globalIterParExecutionContext = iterParExecutionContext
}
