package net.imagini.aim.segment

import java.io.EOFException
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.Array.canBuildFrom
import scala.Array.fallbackCanBuildFrom
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.types.Aim
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimTypeUUID
import net.imagini.aim.types.SortOrder
import net.imagini.aim.types.SortOrder.ASC
import net.imagini.aim.types.SortOrder.DESC
import net.imagini.aim.utils.ByteKey
import net.imagini.aim.utils.View
import java.util.TreeMap

//TODO sourceSchema can be retrieved from any of the segments provided
class MergeScanner(val selectFields: Array[String], val rowFilter: RowFilter, val segments: Seq[AimSegment])
  extends AbstractScanner {
  def this(selectStatement: String, rowFilterStatement: String, segments: Seq[AimSegment]) = this(
    if (selectStatement.contains("*")) segments(0).getSchema.names else selectStatement.split(",").map(_.trim),
    RowFilter.fromString(segments(0).getSchema, rowFilterStatement),
    segments)

  private val sourceSchema: AimSchema = segments(0).getSchema
  override val schema: AimSchema = sourceSchema.subset(selectFields)
  private val sortOrder = SortOrder.ASC
  private val keyField: String = sourceSchema.name(0)
  private val scanSchema: AimSchema = sourceSchema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  private val scanners: Array[SegmentScanner] = segments.map(segment ⇒ new SegmentScanner(scanSchema.names, rowFilter, segment)).toArray

  private val scanColumnIndex: Array[Int] = schema.names.map(n ⇒ scanSchema.get(n))
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  override val keyType = scanSchema.get(scanKeyColumnIndex)
  val keyDataType = keyType.getDataType
  rowFilter.updateFormula(scanSchema.names)

  private val executor: ExecutorService = Executors.newFixedThreadPool(scanners.size)
  private val fetchers: Seq[SegmentFetcher] = scanners.map(s ⇒ new SegmentFetcher(s, 10))
  val counter = new AtomicInteger(0)
  private val mergeQueue = new TreeMap[ByteKey, Array[View]]()// in case this needs to be conurrent : new ConcurrentSkipListMap[ByteKey, Array[View]]() 
  private var currentRow: Array[View] = null
  private var currentKey: View = null
  private var initialised = false
  private var eof = false

  override def next: Boolean = {
    if (eof) {
      return false
    } else if (!initialised) {
      fetchers.map(executor.submit(_))
      initialised = true
    }
    var f = 0
    while (f < fetchers.size) {
      val fetcher = fetchers(f)
      if (!fetcher.closed) {
        fetcher.take match {
          case None ⇒ {}
          case Some(record) ⇒ {
            val keyView = record(scanKeyColumnIndex);
            val byteKey = new ByteKey(keyView.array, keyView.offset, keyDataType.sizeOf(keyView), counter.incrementAndGet)
            mergeQueue.put(byteKey, record)
          }
        }
      }
      f += 1
    }

    val entry = sortOrder match {
      case DESC ⇒ mergeQueue.pollLastEntry
      case ASC  ⇒ mergeQueue.pollFirstEntry
    }
    if (entry == null) {
      eof = true
      currentRow = null
      currentKey = null
      false
    } else {
      val scanRow = entry.getValue
      currentKey = scanRow(scanKeyColumnIndex)
      currentRow = scanColumnIndex.map(c ⇒ scanRow(c))
      true
    }
  }

  override def selectKey: View = {
    if (eof) {
      throw new EOFException
    } else {
      currentKey
    }
  }
  override def selectRow: Array[View] = {
    if (eof) {
      throw new EOFException
    } else {
      currentRow
    }
  }

  /**
   * TODO currently hard-coded for 4-core processors, however once
   * the table is distributed across multiple machines this thread pool should
   * be bigger until the I/O becomes bottleneck.
   */
  override def count: Long = {
    val executor = Executors.newFixedThreadPool(4)
    val results: List[Future[Long]] = (0 to segments.size - 1).map(s ⇒ executor.submit(new Callable[Long] {
      override def call: Long = {
        scanners(s).count
      }
    })).toList
    eof = true
    results.foldLeft(0L)(_ + _.get)
  }

}

//System.err.println(this + " " + new AimTypeUUID(Aim.BYTEARRAY(16)).asString(scanRow(0)))

class SegmentFetcher(val scanner: SegmentScanner, val queueSize: Int) extends Runnable {
  var hasMoreData = true
  val ready = new LinkedBlockingQueue[Option[Array[View]]](queueSize)

  def take: Option[Array[View]] = ready.take

  def closed: Boolean = !hasMoreData && ready.size == 0

  override def run = {
    while (scanner.next) {
      val scanRow = scanner.selectRow
      ready.put(Some(scanRow.map(v ⇒ new View(v))))
    }
    ready.put(None)
    hasMoreData = false;
  }

}