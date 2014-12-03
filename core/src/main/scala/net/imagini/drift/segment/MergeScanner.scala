package net.imagini.drift.segment

import java.io.EOFException
import java.util.TreeMap
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import scala.collection.mutable.SynchronizedQueue
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.types.SortOrder
import net.imagini.drift.types.SortOrder.ASC
import net.imagini.drift.types.SortOrder.DESC
import net.imagini.drift.utils.View
import net.imagini.drift.utils.ByteKey

//TODO sourceSchema can be retrieved from any of the segments provided
class MergeScanner(val selectFields: Array[String], val rowFilter: RowFilter, val segments: Seq[DriftSegment])
  extends AbstractScanner {
  def this(selectStatement: String, rowFilterStatement: String, segments: Seq[DriftSegment]) = this(
    if (selectStatement.contains("*")) segments(0).getSchema.names else selectStatement.split(",").map(_.trim),
    RowFilter.fromString(segments(0).getSchema, rowFilterStatement),
    segments)

  private val sourceSchema: DriftSchema = segments(0).getSchema
  override val schema: DriftSchema = sourceSchema.subset(selectFields)
  private val keyField: String = sourceSchema.name(0)
  private val scanSchema: DriftSchema = sourceSchema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  private val scanners: Array[SegmentScanner] = segments.map(segment ⇒ new SegmentScanner(scanSchema.names, rowFilter, segment)).toArray

  private val scanColumnIndex: Array[Int] = schema.names.map(n ⇒ scanSchema.get(n))
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  override val keyType = scanSchema.get(scanKeyColumnIndex)
  rowFilter.updateFormula(scanSchema.names)

  private val sortOrder = SortOrder.ASC
  private val sortQueue = new TreeMap[ByteKey, Int]
  private var currentScanner: Int = -1
  private val currentRow: Array[View] = Array.fill(scanSchema.size)(new View(null, 0, -1, 0))
  private var initialised = false
  private var eof = false

  override def next: Boolean = {
    if (eof) {
      return false
    } else if (!initialised) {
      var i = 0;
      while (i < scanners.size) {
        val s = scanners(i)
        if (s.next) {
          sortQueue.put(new ByteKey(s.selectRow(scanKeyColumnIndex), i), i)
        }
        i += 1
      }
      initialised = true
    } else if (currentScanner != -1) {
      val scanner = scanners(currentScanner)
      if (scanner.next) {
        if (currentRow(scanKeyColumnIndex).equals(scanner.selectRow(scanKeyColumnIndex))) {
          val row = scanner.selectRow
          var i = 0;
          while (i < row.length)  {
            currentRow(i).clone(row(i))
            i+=1
          }
          return true
        } else {
          sortQueue.put(new ByteKey(scanner.selectRow(scanKeyColumnIndex), currentScanner), currentScanner)
        }
      }
    }
    if (sortQueue.isEmpty) {
      eof = true
    } else {
      currentScanner = if (sortOrder.equals(ASC)) sortQueue.pollFirstEntry.getValue else sortQueue.pollLastEntry.getValue
      val row = scanners(currentScanner).selectRow
      var i = 0;
      while (i < row.length) {
        currentRow(i).clone(row(i))
        i+=1
      }
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