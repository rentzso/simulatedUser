package insightproject.simulated

import java.util.concurrent.{ForkJoinPool, ForkJoinTask, RecursiveTask}

import scala.util.DynamicVariable

/**
  * Created by rfrigato on 6/20/17.
  */
package object user {
  val forkJoinPool = new ForkJoinPool
  class DefaultTaskScheduler {
    def schedule[T](body: => T): ForkJoinTask[T] = {
      val t = new RecursiveTask[T] {
        def compute = body
      }
      forkJoinPool.execute(t)
      t
    }
  }
  val scheduler =
    new DynamicVariable[DefaultTaskScheduler](new DefaultTaskScheduler)

  def task[T](body: => T): ForkJoinTask[T] = {
    scheduler.value.schedule(body)
  }

  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => {
        Thread.sleep(15000)
        retry(n - 1)(fn)
      }
      case util.Failure(e) => throw e
    }
  }
}
