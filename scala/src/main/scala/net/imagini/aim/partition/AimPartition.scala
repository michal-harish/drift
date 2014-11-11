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
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.utils.View

class AimPartition(
  val schema: AimSchema,
  val segmentSizeBytes: Int,
  val storageType: Class[_ <: BlockStorage] = classOf[BlockStorageMEMLZ4],
  val sortType: Class[_ <: AimSegment] = classOf[AimSegmentQuickSort]
  ) {

  val segments: ListBuffer[AimSegment] = ListBuffer()

  private val numSegments = new AtomicInteger(0)

  def getNumSegments = numSegments.get

  def getCount: Long = segments.foldLeft(0L)(_ + _.count)

  def getCompressedSize: Long = segments.foldLeft(0L)(_ + _.getCompressedSize)

  def getUncompressedSize: Long = segments.foldLeft(0L)(_ + _.getOriginalSize)

  private val segmentConstructor = sortType.getConstructor(classOf[AimSchema], classOf[Class[_ <:BlockStorage]])

  def createNewSegment: AimSegment = segmentConstructor.newInstance(schema, storageType)

  def add(segment: AimSegment) = {
    segment.close
    if (segment.count > 0) {
      segments.synchronized { //TODO use concurrent structure as this synchronisation really does nothing
        segments += segment
        numSegments.incrementAndGet
      }
    }
  }

  def appendRecord(segment: AimSegment, record: Array[Array[Byte]]): AimSegment = {
    var i = 0
    var size = 0
    while (i < record.length) {
      size += record(i).length
      i += 1
    }
    val theSegment = checkSegment(segment, size)
    theSegment.appendRecord(record)
    theSegment
  }

  def appendRecord(segment: AimSegment, record: Array[View]): AimSegment = {
    var i = 0
    var size = 0
    while (i < record.length) {
      size += record(i).size
      i += 1
    }
    val theSegment = checkSegment(segment, size)
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