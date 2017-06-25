package insightproject.simulated

import java.util.concurrent.{ForkJoinPool, ForkJoinTask, RecursiveTask}

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.DynamicVariable
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by rfrigato on 6/20/17.
  */
package object user {
  def consumeFutureQueue[A, B](process: (A, B) => Future[A]):
    mutable.Queue[(Future[A], B)]=> Future[A]= {

    def consume(q: mutable.Queue[(Future[A], B)]) = {
      val (f, v) = q.dequeue()
      val new_f = f.flatMap[A](process(_, v))
      q += Tuple2(new_f, v)
      new_f
    }
    consume
  }
}
