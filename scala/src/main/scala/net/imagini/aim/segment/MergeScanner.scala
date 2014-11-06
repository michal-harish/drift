package net.imagini.aim.segment

import java.io.EOFException
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder
import scala.collection.JavaConverters._
import java.nio.ByteBuffer
import net.imagini.aim.tools.AbstractScanner
import scala.Array.canBuildFrom
import net.imagini.aim.tools.ColumnScanner
import java.util.concurrent.Future
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import net.imagini.aim.utils.ByteUtils

class MergeScanner(val sourceSchema: AimSchema, val selectFields: Array[String], val rowFilter: RowFilter, val segments: Seq[AimSegment])
  extends AbstractScanner {
  def this(sourceSchema: AimSchema, selectStatement: String, rowFilterStatement: String, segments: Seq[AimSegment]) = this(
    sourceSchema,
    if (selectStatement.contains("*")) sourceSchema.names else selectStatement.split(",").map(_.trim),
    RowFilter.fromString(sourceSchema, rowFilterStatement),
    segments)

  override val schema: AimSchema = sourceSchema.subset(selectFields)
  private val sortOrder = SortOrder.ASC
  private val keyField: String = sourceSchema.name(0)
  private val scanSchema: AimSchema = sourceSchema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  private val scanners: Array[Array[ColumnScanner]] = segments.map(segment ⇒ segment.wrapScanners(scanSchema)).toArray
  private val scanColumnIndex: Array[Int] = schema.names.map(n ⇒ scanSchema.get(n))
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  override val keyType = scanSchema.get(scanKeyColumnIndex)
  override val keyLen = keyType.getDataType.getLen
  rowFilter.updateFormula(scanSchema.names)

  private var eof = false
  private var currentScanner: Array[ColumnScanner] = null
  private var markCurrentSegment: Array[ColumnScanner] = null
  private var currentKey: ByteBuffer = null
  private val currentRecord: Array[ByteBuffer] = new Array[ByteBuffer](schema.size)

  override def rewind = {
    scanners.foreach(_.foreach(_.rewind))
    currentScanner = null
    eof = false
    move
  }

  override def mark = {
    scanners.foreach(_.foreach(_.mark))
    markCurrentSegment = currentScanner
  }

  override def reset = {
    scanners.foreach(_.foreach(_.reset))
    currentScanner = markCurrentSegment
    markCurrentSegment = null
    eof = false
    move
  }

  /**
   * Optimized next method
   */
  var buffers: Array[ByteBuffer] = new Array[ByteBuffer](scanSchema.size)
  override def next: Boolean = {
    if (eof) {
      return false
    }
    //skip to next
    if (null != currentScanner) {
      var c = 0
      while (c < currentScanner.length) {
        currentScanner(c).skip
        c += 1
      }
      currentScanner = null
    }
    var s = 0
    while (s < scanners.length) {
      val scanner = scanners(s)
      s += 1
      //optimized filter
      var segmentHasData = true
      var filterMatch = true
      do {
        var i = 0
        while (i < scanner.length) {
          if (!filterMatch) scanner(i).skip
          segmentHasData = segmentHasData && !scanner(i).eof
          buffers(i) = scanner(i).buffer
          i += 1
        }
        if (segmentHasData) filterMatch = rowFilter.matches(buffers)
      } while (segmentHasData && !filterMatch)

      //merge sort
      if (segmentHasData) {
        if (null == currentScanner || ((sortOrder == SortOrder.ASC ^ ByteUtils.compare(scanner(scanKeyColumnIndex).buffer, currentScanner(scanKeyColumnIndex).buffer, keyLen) > 0))) {
          currentScanner = scanner
        }
      }
    }
    eof = null == currentScanner 
    move
    !eof
  }

  override def selectKey: ByteBuffer = {
    if (eof || (null == currentScanner && !next)) {
      throw new EOFException
    } else {
      currentKey
    }
  }
  override def selectRow: Array[ByteBuffer] = {
    if (eof || (null == currentScanner && !next)) {
      throw new EOFException
    } else {
      currentRecord
    }
  }

  private def move = {
    if (null == currentScanner) {
      currentKey = null
      for (i ← (0 to schema.size - 1)) currentRecord(i) = null
    } else {
      currentKey = currentScanner(scanKeyColumnIndex).buffer
      var i = 0
      while (i < currentRecord.length) {
        currentRecord(i) = currentScanner(scanColumnIndex(i)).buffer
        i += 1
      }
    }
  }

  /**
   * TODO currently hard-coded for 4-core processors, however once
   * the table is distributed across multiple machines this thread pool should
   * be bigger until the I/O becomes bottleneck.
   */
  override def count: Long = {
    rewind
    val executor = Executors.newFixedThreadPool(4)
    val results: List[Future[Long]] = (0 to segments.size - 1).map(s ⇒ executor.submit(new Callable[Long] {
      override def call: Long = {
        if (rowFilter.isEmptyFilter) {
          segments(s).count
        } else {
          //optimized filter count
          val scanner = scanners(s)
          var count = 0
          var scannerHasData = true
          do {
            var i = 0
            while (i < scanner.length) {
              scannerHasData = scannerHasData && !scanner(i).eof
              buffers(i) = scanner(i).buffer
              i += 1
            }
            if (rowFilter.matches(buffers)) count += 1
            i = 0
            while (i < scanner.length) {
              scanner(i).skip
              scannerHasData = scannerHasData && !scanner(i).eof
              i += 1
            }
          } while (scannerHasData)
          count
        }
      }
    })).toList
    eof = true
    results.foldLeft(0L)(_ + _.get)
  }

}