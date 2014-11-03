package net.imagini.aim.partition

import net.imagini.aim.types.AimSchema
import scala.collection.mutable.ListBuffer
import net.imagini.aim.segment.AimSegment
import java.util.concurrent.atomic.AtomicInteger
import net.imagini.aim.tools.RowFilter
import java.io.InputStream
import net.imagini.aim.tools.StreamMerger
import java.util.concurrent.Executors
import scala.collection.mutable.MutableList
import java.util.concurrent.Future
import java.util.concurrent.Callable
import java.io.IOException
import net.imagini.aim.cluster.ScannerInputStream
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.tools.CountScanner

class AimPartition(val schema: AimSchema, val segmentSizeBytes: Int) {
  val segments: ListBuffer[AimSegment] = ListBuffer()
  val numSegments = new AtomicInteger(0)
  def getNumSegments = numSegments.get
  def defaultRange: (Int, Int) = (0, numSegments.get - 1)
  def add(segment: AimSegment): AimSegment = {
    segments.synchronized { //TODO is this necessary
      segments += segment
      numSegments.incrementAndGet
    }
    segment
  }
  val mapreduce = Executors.newFixedThreadPool(4)
  def getCount: Long = segments.foldLeft(0L)(_ + _.count)
  def getCompressedSize: Long = segments.foldLeft(0L)(_ + _.getCompressedSize)
  def getUncompressedSize: Long = segments.foldLeft(0L)(_ + _.getOriginalSize)

  /**
   * TODO Parallel count - currently hard-coded for 4-core processors, however once
   * the table is distributed across multiple machines this thread pool should
   * be bigger until the I/O becomes bottleneck.
   */
  def count(filterStatement: String): Long = {
    //TODO range will become part of the filter
    val range = defaultRange
    val seg: Array[Int] = new Array(range._2 - range._1 + 1)
    val results: List[Future[Long]] = seg.map(s ⇒ mapreduce.submit(new Callable[Long] {
      override def call: Long = {
        filterStatement match {
          case "*" ⇒ segments(s).count
          case _ ⇒ {
            val scanner = new MergeScanner(schema, schema.name(0), filterStatement, Seq(segments(s))) with CountScanner
            val count = scanner.count
            scanner.close
            count
          }
        }
      }
    })).toList
    results.foldLeft(0L)(_ + _.get)
  }

  //  def reduce[T](filter: RowFilter, reducer:() => T): T= {
  //    val range = defaultRange
  //    val seg: Array[Int] = new Array(range._2 - range._1 + 1)
  //    val results: List[Future[Long]] = seg.map(s => mapreduce.submit(new Callable[Long] {
  //        override def call: Long = {
  //          segments.synchronized {
  //            segments(s).reduce(filter, reducer)
  //          }
  //        }
  //      })).toList
  //    results.foldLeft(0L)(_ + _.get)
  //  }

}