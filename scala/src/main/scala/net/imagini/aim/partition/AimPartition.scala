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

  def getCount: Long = segments.foldLeft(0L)(_ + _.count)

  def getCompressedSize: Long = segments.foldLeft(0L)(_ + _.getCompressedSize)

  def getUncompressedSize: Long = segments.foldLeft(0L)(_ + _.getOriginalSize)

  def add(segment: AimSegment): AimSegment = {
    segments.synchronized { //TODO is this necessary
      segments += segment
      numSegments.incrementAndGet
    }
    segment
  }

}