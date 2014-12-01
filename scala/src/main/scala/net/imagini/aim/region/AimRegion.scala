package net.imagini.aim.region

import java.io.File
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimTableDescriptor
import net.imagini.aim.types.SortOrder
import net.imagini.aim.types.SortType
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.utils.View
import net.imagini.aim.utils.BlockStorage.PersistentBlockStorage
import net.imagini.aim.utils.ByteUtils

class AimRegion(
  val regionId: String,
  val descriptor: AimTableDescriptor) {

  val schema = descriptor.schema
  val segmentSizeBytes = descriptor.segmentSize
  val storageType: Class[_ <: BlockStorage] = descriptor.storageType
  val sortOrder = SortOrder.ASC

  val segments: ListBuffer[AimSegment] = ListBuffer()
  val compactionExecutor = Executors.newFixedThreadPool(10)
  val compactionQueue = new Queue[Future[Option[Int]]]
  val compactionLock = new ReentrantLock

  private val numSegments = new AtomicInteger(0)

  def getNumSegments = numSegments.get

  def getCompressedSize: Long = segments.foldLeft(0L)(_ + _.getCompressedSize)

  private val segmentConstructor = classOf[AimSegment].getConstructor(classOf[AimSchema])

  val persisted = storageType.getInterfaces.contains(classOf[PersistentBlockStorage])

  if (persisted) {
    //TODO invoke block storage instead of assuming it's in local file
    val f = new File("/var/lib/drift")
    if (f.exists) f.listFiles.filter(_.getName.startsWith(regionId)).foreach(segmentFile ⇒ {
      if (segmentFile.isDirectory()) {
        segments += segmentConstructor.newInstance(schema).open(storageType, segmentFile)
        numSegments.incrementAndGet
      }
    })
  }

  def addRecords(recordList: java.util.ArrayList[View]) = {
    descriptor.sortType match {
      case SortType.QUICK_SORT ⇒ {
        Collections.sort(recordList)
        if (sortOrder.equals(SortOrder.DESC)) {
          Collections.reverse(recordList);
        }
      }
      case SortType.NO_SORT | _ ⇒ {}
    }
    val segment = new AimSegment(schema).init(storageType, regionId)
    var r = 0; while (r < recordList.size()) {
      val record = recordList.get(r)
      //System.err.println(schema.asString(record))
      segment.commitRecord(record)
      r += 1
    }

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

  protected[aim] def addTestRecords(recordList: Seq[String]*) = {
    addRecords(
      new java.util.ArrayList(
        recordList.map(recordSequence ⇒ {
          if (recordSequence.length != schema.size) {
            throw new IllegalArgumentException("Number of values doesn't match the number of fields in the schema");
          }
          val values = new Array[Array[Byte]](schema.size)
          var recordLen = 0
          var c = 0; while (c < schema.size()) {
            values(c) = schema.get(c).convert(recordSequence(c))
            recordLen += values(c).length
            c += 1
          }
          val recordBytes = new Array[Byte](recordLen)
          var recordOffset = 0
          for (v ← values) {
            ByteUtils.copy(v, 0, recordBytes, recordOffset, v.length)
            recordOffset += v.length
          }
          new View(recordBytes)
        }).asJava))
  }

}