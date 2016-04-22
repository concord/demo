package com.concord.utils

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList
import scala.reflect.ClassTag

object EnrichedStreams {
  implicit class CountingDStream[T: ClassTag](@transient val dstream: DStream[T])
      extends Serializable {

    @transient var spill: DStream[Iterable[T]] = null

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
      val windowed = if (spill == null) {
        dstream
      } else {
        spill.union(dstream)
      }

      val ret = windowed.transform(rdd => {
        //val x = leftovers.union(rdd)
        val isOpened = (w: (Int, Iterable[T])) => w._2.size < windowLength
        val counts = rdd.count.toInt
        /** Fill into buckets, RDD[(K, Iterable[T])], then strip grouping info */
        val allWindows = rdd
          .zipWithIndex
          /** RDD[(T, Long)] */
          .flatMap((x) => {
            val indicies =
              windowsForRecord(x._2, counts, windowLength, slideInterval)
            indicies.map((x._1, _))
          })
          .groupBy((t: Tuple2[T, Int]) => t._2)
          /** RDD[(K, List[T])]) */
          .map(x => (x._1, x._2.map(_._1)))

        allWindows.map((x) => (isOpened(x), x))
      }).groupByKey()

      spill = ret.filter(_._1).flatMap(_._2._2)
      ret.filter(!_._1).flatMap(_._2)
    }
  }
}
