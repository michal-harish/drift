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
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.segment.AimSegmentQuickSort
import java.nio.ByteBuffer
import net.imagini.aim.utils.BlockStorageLZ4

class AimPartition(
  val schema: AimSchema,
  val segmentSizeBytes: Int,
  val storageType: Class[_ <: BlockStorage] = classOf[BlockStorageLZ4]) {

  val segments: ListBuffer[AimSegment] = ListBuffer()

  val numSegments = new AtomicInteger(0)

  def getNumSegments = numSegments.get

  def getCount: Long = segments.foldLeft(0L)(_ + _.count)

  def getCompressedSize: Long = segments.foldLeft(0L)(_ + _.getCompressedSize)

  def getUncompressedSize: Long = segments.foldLeft(0L)(_ + _.getOriginalSize)

  //TODO parametrize segment sort type 
  def createNewSegment: AimSegment = new AimSegmentQuickSort(schema, storageType)

  def add(segment: AimSegment) = {
    segment.close
    if (segment.count > 0) {
      segments.synchronized { //TODO use concurrent structure as this synchronisation really does nothing
        segments += segment
        numSegments.incrementAndGet
      }
    }
  }

  def appendRecord(segment: AimSegment, record: Array[ByteBuffer]): AimSegment = {
    val size = record.map(b => b.limit - b.position).foldLeft(0)(_ + _)
    val theSegment = checkSegment(segment, size)
    theSegment.appendRecord(record)
    theSegment
  }
  def appendRecord(segment: AimSegment, record: ByteBuffer): AimSegment = {
    val theSegment = checkSegment(segment, record.limit - record.position )
    theSegment.appendRecord(record)
    theSegment
  }
  private def checkSegment(segment: AimSegment, size: Int): AimSegment = {
    if (segment.getOriginalSize + size> segmentSizeBytes) try {
      add(segment)
      createNewSegment
    } catch {
      case e: Throwable ⇒ {
        throw new IOException(e);
      }
    } else {
      segment
    }
  }
}