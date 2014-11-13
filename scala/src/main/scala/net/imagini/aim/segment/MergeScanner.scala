package net.imagini.aim.segment

import java.io.EOFException
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder
import scala.collection.JavaConverters._
import net.imagini.aim.tools.AbstractScanner
import scala.Array.canBuildFrom
import java.util.concurrent.Future
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import net.imagini.aim.types.TypeUtils
import net.imagini.aim.utils.View

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

  private var eof = false
  private var currentScanner: SegmentScanner = null

  /**
   * Optimized next method
   */
  override def next: Boolean = {
    if (eof) {
      return false
    }
    if (null == currentScanner) {
      scanners.foreach(_.next)
    } else {
      currentScanner.next
      currentScanner = null
    }
    var s = 0
    while (s < scanners.length) {
      val scanner = scanners(s)
      if (!scanner.eof) {
        if (null == currentScanner || ((sortOrder == SortOrder.ASC ^ TypeUtils.compare(scanner.selectKey, currentScanner.selectKey, keyDataType) > 0))) {
          currentScanner = scanner
        }
      }
      s += 1
    }
    eof = null == currentScanner
    !eof
  }

  override def selectKey: View = {
    if (eof || (null == currentScanner && !next)) {
      throw new EOFException
    } else {
      currentScanner.selectKey
    }
  }
  override def selectRow: Array[View] = {
    if (eof || (null == currentScanner && !next)) {
      throw new EOFException
    } else {
      currentScanner.selectRow
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
      override def call: Long = scanners(s).count
     })).toList
    eof = true
    results.foldLeft(0L)(_ + _.get)
  }

}