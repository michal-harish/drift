package net.imagini.aim.region

import java.io.File
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import net.imagini.aim.cluster.ScannerInputStream
import net.imagini.aim.cluster.StreamMerger
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.segment.CountScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.utils.View
import net.imagini.aim.utils.BlockStorage.PersistentBlockStorage
import java.util.concurrent.Future
import java.util.concurrent.Callable
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.LinkedList
import scala.collection.mutable.Queue
import net.imagini.aim.cluster.StreamUtils
import java.io.InputStream
import net.imagini.aim.types.AimTableDescriptor

class AimRegion(
  val regionId: String,
  val descriptor: AimTableDescriptor) {

  val schema = descriptor.schema
  val segmentSizeBytes = descriptor.segmentSize
  val storageType: Class[_ <: BlockStorage] = descriptor.storageType
  val sortType: Class[_ <: AimSegment] = descriptor.sortType

  val segments: ListBuffer[AimSegment] = ListBuffer()
  val compactionExecutor = Executors.newFixedThreadPool(10)
  val compactionQueue = new Queue[Future[Option[Int]]]
  val compactionLock = new ReentrantLock

  private val numSegments = new AtomicInteger(0)

  def getNumSegments = numSegments.get

  def getCompressedSize: Long = segments.foldLeft(0L)(_ + _.getCompressedSize)

  private val segmentConstructor = sortType.getConstructor(classOf[AimSchema])

  val persisted = storageType.getInterfaces.contains(classOf[PersistentBlockStorage])

  if (persisted) {
    //TODO invoke block storage instead of assuming it's in local file
    val f = new File("/var/lib/drift")
    if (f.exists) f.listFiles.filter(_.getName.startsWith(regionId)).foreach(segmentFile ⇒ {
      segments += segmentConstructor.newInstance(schema).open(storageType, segmentFile)
      numSegments.incrementAndGet
    })
  }

  def loadRecord(segment: AimSegment, in: InputStream): AimSegment = {
    //    val theSegment = checkSegment(segment, 0)
    //    theSegment.loadRecord(in)
    //    return theSegment
    val record = new Array[Array[Byte]](schema.size)
    var c = 0; while (c < schema.size) {
      record(c) = StreamUtils.read(in, schema.get(c))
      c += 1
    }
    return appendRecord(segment, record)
  }

  def newSegment: AimSegment = segmentConstructor.newInstance(schema).init(storageType, regionId)

  def add(segment: AimSegment) = {
    val callable = new Callable[Option[Int]] {
      override def call: Option[Int] = {
        segment.close
        if (segment.getCompressedSize > 0) {
          val index = numSegments.getAndIncrement
          compactionLock.synchronized {
            segments += segment
          }
          Some(index)
        } else {
          None
        }
      }
    }
    compactionLock.synchronized {
      compactionQueue.enqueue(compactionExecutor.submit(callable))
    }
  }

  def compact = {
    compactionQueue.take(compactionQueue.size).foreach(future ⇒ {
      val segmentIndex = future.get
      //TODO log.debug compatction info
    })
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