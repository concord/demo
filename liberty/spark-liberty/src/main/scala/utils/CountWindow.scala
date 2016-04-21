package com.concord.utils

import com.concord.contexts.BenchmarkStreamContext

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

object EnrichedStreams {
  implicit class CountingDStream[T](dstream: DStream[T])
      extends BenchmarkStreamContext {
    /** Return the bucket number of the nth record, returns 'None' in the case that
      * our buckets are spaced and the record should be omitted from the stream  */
    private def findEdge(
      recIdx: Long,
      windowLength: Int,
      slideInterval: Int): Option[Int] = {
      val edge: Double = recIdx.toDouble / slideInterval
      if(Math.floor(edge) == edge)
        Some(edge.toInt)
      else {
        // This number is not whole
        val nextSmallest = edge.toInt * slideInterval
        if (recIdx <= nextSmallest + windowLength)
          Some(edge.toInt)
        else
          None
      }
    }

    /** Returns the window indicies for all windows that 'recIdx' should be in.
      * Indicies are a List() type because of overlapping windows. Could return
      * an empty list if the windows are tumbling */
    private def windowsForRecord(
      recIdx: Long,
      windowLength: Int,
      slideInterval: Int): List[Int] =
      findEdge(recIdx, windowLength, slideInterval) match {
        case Some(x) => x :: windowsForRecord(recIdx - 1, windowLength, slideInterval)
        case _ => List()
      }

    def countingWindow(windowLength: Int, slideInterval: Int): Unit = {
      val isOpened = (w: Iterable[(String, String)]) => w.size < windowLength

      var leftovers: RDD[(String, String)] = sparkContext.emptyRDD
      stream
        .transform( rdd => {
          /** Fill into buckets, RDD[(K, Iterable[T])], then strip grouping info */
          val allWindows = rdd
            .union(leftovers)
            .zipWithIndex
            .flatMap((x) => windowsForRecord(x._2, windowLength, slideInterval) match {
              case Nil => None
              case list => Some((x._1, list))
            })
            .flatMap((x) => x._2.map((i) => (x._1, i)))
            .groupBy(_._2)
            .map(_._2.map(_._1)) // Get rid of index/grouping id

          /** Stash windows that aren't filled */
          leftovers = allWindows
            .filter(isOpened)
            .flatMap(_.toList)

          /** Stream should contain only closed windows */
          allWindows.filter((x) => ! isOpened(x))
        })

    }
  }
}
