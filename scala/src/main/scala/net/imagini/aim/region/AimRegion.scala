package net.imagini.aim.region

import net.imagini.aim.types.AimSchema
import scala.collection.mutable.ListBuffer
import net.imagini.aim.segment.AimSegment
import java.util.concurrent.atomic.AtomicInteger
import net.imagini.aim.segment.RowFilter
import java.io.InputStream
import net.imagini.aim.cluster.StreamMerger
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
import net.imagini.aim.utils.BlockStorage.PersistentBlockStorage
import java.io.File
import java.nio.file.Files

class AimRegion(
  val regionId: String,
  val schema: AimSchema,
  val segmentSizeBytes: Int,
  val storageType: Class[_ <: BlockStorage] = classOf[BlockStorageMEMLZ4],
  val sortType: Class[_ <: AimSegment] = classOf[AimSegmentQuickSort]) {

  val segments: ListBuffer[AimSegment] = ListBuffer()

  private val numSegments = new AtomicInteger(0)

  def getNumSegments = numSegments.get

  def getCompressedSize: Long = segments.foldLeft(0L)(_ + _.getCompressedSize)

  private val segmentConstructor = sortType.getConstructor(classOf[AimSchema])

  val persisted = storageType.getInterfaces.contains(classOf[PersistentBlockStorage])

  if (persisted) {
    val f = new File("/var/lib/drift")
    if (f.exists) f.listFiles.filter(_.getName.startsWith(regionId)).foreach(segmentFile => {
        segments += segmentConstructor.newInstance(schema).open(storageType, segmentFile)
        numSegments.incrementAndGet
    })
  }

  def newSegment: AimSegment = segmentConstructor.newInstance(schema).init(storageType, regionId)

  def add(segment: AimSegment) = {
    segment.close
    if (segment.getCompressedSize() > 0) {
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
    if (segment.getRecordedSize() + size > segmentSizeBytes) try {
      add(segment)
      newSegment
    } catch {
      case e: Throwable ⇒ {
        throw new IOException(e);
      }
    }
    else {
      segment
    }
  }
}