package net.imagini.aim.segment

import java.io.EOFException
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.Aim
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimTypeUUID
import net.imagini.aim.types.SortOrder
import net.imagini.aim.types.SortOrder.ASC
import net.imagini.aim.types.SortOrder.DESC
import net.imagini.aim.utils.View
import java.util.TreeMap
import scala.collection.mutable.Queue
import scala.collection.mutable.SynchronizedQueue

//TODO sourceSchema can be retrieved from any of the segments provided
class MergeScanner(val selectFields: Array[String], val rowFilter: RowFilter, val segments: Seq[AimSegment])
  extends AbstractScanner {
  def this(selectStatement: String, rowFilterStatement: String, segments: Seq[AimSegment]) = this(
    if (selectStatement.contains("*")) segments(0).getSchema.names else selectStatement.split(",").map(_.trim),
    RowFilter.fromString(segments(0).getSchema, rowFilterStatement),
    segments)

  private val sourceSchema: AimSchema = segments(0).getSchema
  override val schema: AimSchema = sourceSchema.subset(selectFields)
  private val keyField: String = sourceSchema.name(0)
  private val scanSchema: AimSchema = sourceSchema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  private val scanners: Array[SegmentScanner] = segments.map(segment ⇒ new SegmentScanner(scanSchema.names, rowFilter, segment)).toArray

  private val scanColumnIndex: Array[Int] = schema.names.map(n ⇒ scanSchema.get(n))
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  override val keyType = scanSchema.get(scanKeyColumnIndex)
  val keyDataType = keyType.getDataType
  rowFilter.updateFormula(scanSchema.names)

  private val sortOrder = SortOrder.ASC
  private var currentScanner: SegmentScanner = null
  private var currentRow: Array[View] = null
  private var initialised = false
  private var eof = false

  override def next: Boolean = {
    if (eof) {
      return false
    } else if (!initialised) {
      scanners.map(_.next)
      initialised = true
    } else if (currentScanner != null) {
      currentScanner.next
    } 
    var s = 0
    currentRow = null
    while (s < scanners.size) {
      val scanner = scanners(s)
      if (!scanner.eof) {
        val record = scanner.selectRow
        if (currentRow == null || ((record(scanKeyColumnIndex).compareTo(currentRow(scanKeyColumnIndex)) < 0) ^ sortOrder.equals(DESC))) {
          currentRow = record.map(v => new View(v))
          currentScanner = scanner
        }
      }
      s += 1
    }
    if (currentRow == null) {
      eof = true
    } 
    !eof
  }

  override def selectKey: View = {
    if (eof) {
      throw new EOFException
    } else {
      currentRow(scanKeyColumnIndex)
    }
  }
  override def selectRow: Array[View] = {
    if (eof) {
      throw new EOFException
    } else {
      currentRow
    }
  }

  override def count: Long = {
    val executor: ExecutorService = Executors.newFixedThreadPool(scanners.size)
    val results: List[Future[Long]] = (0 to segments.size - 1).map(s ⇒ executor.submit(new Callable[Long] {
      override def call: Long = {
        scanners(s).count
      }
    })).toList
    eof = true
    results.foldLeft(0L)(_ + _.get)
  }


}