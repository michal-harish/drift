package net.imagini.aim.segment

import java.io.EOFException
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder
import scala.collection.JavaConverters._
import java.nio.ByteBuffer
import net.imagini.aim.tools.AbstractScanner
import scala.Array.canBuildFrom
import net.imagini.aim.types.TypeUtils
import net.imagini.aim.tools.ColumnScanner
import java.util.concurrent.Future
import java.util.concurrent.Executors
import java.util.concurrent.Callable

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
  rowFilter.updateFormula(scanSchema.names)

  private var eof = false
  private var currentScanner: Option[Array[ColumnScanner]] = None
  private var markCurrentSegment: Option[Array[ColumnScanner]] = None
  private var currentKey: ByteBuffer = null
  private val currentRecord: Array[ByteBuffer] = new Array[ByteBuffer](schema.size)

  override def rewind = {
    scanners.foreach(_.foreach(_.rewind))
    currentScanner = None
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
    markCurrentSegment = None
    eof = false
    move
  }

  /**
   * Optimized next method
   */
  val buffers: Array[ByteBuffer] = new Array[ByteBuffer](scanSchema.size)
  override def next: Boolean = {
    if (eof) {
      return false
    }
    if (currentScanner != None) {
      for (columnScanner ← currentScanner.get) columnScanner.skip
      currentScanner = None
    }
    var s = 0
    while(s < scanners.length) {
      val scanner  = scanners(s)
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
        if (currentScanner == None || ((sortOrder == SortOrder.ASC ^ TypeUtils.compare(scanner(scanKeyColumnIndex).buffer, currentScanner.get(scanKeyColumnIndex).buffer, keyType) > 0))) {
          currentScanner = Some(scanner)
        }
      }
    }
    eof = currentScanner == None
    move
    !eof
  }

  override def selectKey: ByteBuffer = {
    if (eof || (currentScanner == None && !next)) {
      throw new EOFException
    } else {
      currentKey
    }
  }
  override def selectRow: Array[ByteBuffer] = {
    if (eof || (currentScanner == None && !next)) {
      throw new EOFException
    } else {
      currentRecord
    }
  }

  private def move = {
    currentScanner match {
      case None ⇒ {
        currentKey = null
        for (i ← (0 to schema.size - 1)) currentRecord(i) = null
      }
      case Some(scanner) ⇒ {
        currentKey = scanner(scanKeyColumnIndex).buffer
        for (i ← (0 to schema.size - 1)) currentRecord(i) = scanner(scanColumnIndex(i)).buffer
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