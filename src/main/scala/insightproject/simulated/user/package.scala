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

  /**
    * @param process function to process a tuple (A, B) and return a Future[A:]
    * @return a method to process a tuple (Future[A], B)
    * from a queue and add the returned future to the queue.
    *
    * To use it
    * {{{
    *
    *   def process(a: Int, b: Int) = {
    *     return Future {
    *       a + b
    *     }
    *   }
    *   val q = new mutable.Queue[(Future[Int], Int)]
    *   q += Tuple2(Future { 0 }, 1)
    *   q += Tuple2(Future { 0 }, 2)
    *   val consume = consumeFutureQueue[Int, Int](process)
    *   consume(q) // consume the (Future { 0 }, 1) enqueue (Future { 1 }, 1)
    *   consume(q) // consume the (Future { 0 }, 2) enqueue (Future { 2 }, 2)
    *
    * }}}
    *
    * Effectively this allows to create infinite chains of futures, and consume
    * them in a round robin fashion
    *
    */
  def consumeFutureQueue[A, B](process: (A, B) => Future[A]):
    mutable.Queue[(Future[A], B)] => Future[A] = {

    def consume(q: mutable.Queue[(Future[A], B)]) = {
      val (f, v) = q.dequeue()
      val new_f = f.flatMap[A](process(_, v))
      q += Tuple2(new_f, v)
      new_f
    }
    consume
  }
}
