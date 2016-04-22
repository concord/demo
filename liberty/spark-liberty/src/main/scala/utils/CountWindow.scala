package com.concord.utils

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import scala.collection.mutable.{Queue => MutableQueue}

object EnrichedStreams {
  /** Implicit cast that allows a dstream to group its records into 'counting'
    * windows. More info: (https://msdn.microsoft.com/en-us/library/ee842704.aspx) */
  implicit class CountingDStream[T: ClassTag](@transient val dstream: DStream[T])
      extends Serializable {
    /**
     * Returns the window indicies for all windows that 'recIdx' should be in.
     * Indicies are a List() type because of overlapping windows. Could return
     * an empty list if the windows are tumbling
     */
    private def windowsForRecord(
      recIdx: Long,
      maxId: Int,
      windowLength: Int,
      slideInterval: Int
    ): List[Int] = {
      val ranges = (for (i <- 0 to maxId by slideInterval) yield (i, windowLength + i))
      ranges.filter { case (begin, end) => recIdx >= begin && recIdx < end }.map(_._1).toList
    }

    /**
      * Transforms a DStream[T] into a DStream[Iterable[T]] into discrete
      * groups defined by the parameters 'windowLength' (# of records in window) and
      * 'slideInterval' (# of records until next opened window)
      */
    def countingWindow(
      windowLength: Int,
      slideInterval: Int
    )(implicit queue: MutableQueue[RDD[T]]): DStream[Iterable[T]] = {
      val leftoverStream = dstream.context.queueStream(queue)

      /** Create stream that combines previous batches open windows with new
        * incoming records, then transform each rdd to split records */
      val ret = leftoverStream.union(dstream).transform(rdd => {
        val isOpened = (w: (Iterable[T])) => w.size < windowLength
        val counts = rdd.count.toInt

        /** Group records into evenly divided buckets, move any leftovers to 'spill' */
        val allWindows = rdd
          .zipWithIndex
          .flatMap((x: Tuple2[T, Long]) => {
            val indicies =
              windowsForRecord(x._2, counts, windowLength, slideInterval)
            indicies.map((x._1, _))
          })
          .groupBy((x: Tuple2[T, Int]) => x._2)
          .map((x: Tuple2[Int, Iterable[(T, Int)]]) => x._2.map(_._1))

        /** Map to add flag so windows may be grouped by status */
        allWindows.map(x => (isOpened(x), x))
      }).groupByKey()

      /** Filter open windows, then transform Iterable[T] -> T, then enqueue */
      ret.filter(_._1 == true)
        .flatMap(_._2)
        .flatMap((x) => x)
        .foreachRDD(rdd => queue.synchronized {
          val count = rdd.count
          if (count > 0) {
            queue += rdd
          }
        })

      /** Filter open windows from ret stream */
      val closedWindows = ret
        .filter(_._1 == false)
        .flatMap(_._2)

      closedWindows
    }
  }
}
