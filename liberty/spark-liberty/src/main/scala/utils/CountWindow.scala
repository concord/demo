package com.concord.utils

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList
import scala.reflect.ClassTag

object EnrichedStreams {
  implicit class CountingDStream[T : ClassTag](@transient val dstream: DStream[T])
      extends Serializable {
    /** Returns the window indicies for all windows that 'recIdx' should be in.
      * Indicies are a List() type because of overlapping windows. Could return
      * an empty list if the windows are tumbling */
    private def windowsForRecord(
      recIdx: Long,
      maxId: Int,
      windowLength: Int,
      slideInterval: Int): List[Int] = {
      println(s"recIdx=${recIdx},maxId=${maxId},length=${windowLength},slideInterval=${slideInterval}")
      val ranges = (for(i <- 0 to maxId by slideInterval) yield (i, windowLength + i))
      ranges.filter{case(begin, end)=> recIdx >= begin && recIdx <= end}.map(_._1).toList
    }

    def countingWindow(windowLength: Int, slideInterval: Int): DStream[Iterable[(T, Int)]] = {
      // var leftovers: RDD[T] = dstream.context
      //   .sparkContext.emptyRD.asInstanceOf[RDD[T]]

      val windowed = dstream
        .transform( rdd => {
          val isOpened = (w: Iterable[(T, Int)]) => w.size < windowLength

          /** Fill into buckets, RDD[(K, Iterable[T])], then strip grouping info */
          val allWindows = rdd
            //.union(leftovers)
            .zipWithIndex /** RDD[(T, Long)] */
            .flatMap((x) => {
              val indicies = windowsForRecord(x._2, rdd.count.toInt, windowLength, slideInterval)
              indicies.map((x._1,_))
            })
            .groupBy((t: Tuple2[T, Int]) => t._2)    /** RDD[(K, List[T])]) */
            .map(_._2) // Get rid of index/grouping id

          /** Stash windows that aren't filled */
          // leftovers = allWindows
          //   .filter(isOpened)
          //   .flatMap(_.toList)

          /** Stream should contain only closed windows */
          allWindows.filter((x) => ! isOpened(x))
        })
      windowed
    }
  }
}
