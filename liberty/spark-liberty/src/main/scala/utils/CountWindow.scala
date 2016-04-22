package com.concord.utils

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList
import scala.reflect.ClassTag

object EnrichedStreams {
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

    def countingWindow(
      windowLength: Int,
      slideInterval: Int
    ): DStream[(Int, Iterable[T])] = {
      var leftovers: RDD[T] = dstream.context
        .sparkContext.emptyRDD.asInstanceOf[RDD[T]]

      val windowed = dstream
        .transform(rdd => {
          //val x = leftovers.union(rdd)
          val isOpened = (w: (Int, Iterable[T])) => w._2.size < windowLength

          /** Fill into buckets, RDD[(K, Iterable[T])], then strip grouping info */
          val allWindows = rdd
            .zipWithIndex
            /** RDD[(T, Long)] */
            .flatMap((x) => {
              val indicies =
                windowsForRecord(x._2, rdd.count.toInt, windowLength, slideInterval)
              indicies.map((x._1, _))
            })
            .groupBy((t: Tuple2[T, Int]) => t._2) /** RDD[(K, List[T])]) */
            .map(x => (x._1, x._2.map(_._1)))

          /** Stash windows that aren't filled */
          leftovers = allWindows
            .filter(isOpened)
            .flatMap(_._2)

          /** Stream should contain only closed windows */
          allWindows.filter((x) => !isOpened(x))
        })
      windowed
    }
  }
}
