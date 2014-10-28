package net.imagini.aim.partition

import java.io.EOFException
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.tools.PipeUtils
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.tools.Scanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder
import net.imagini.aim.utils.ByteKey
import java.util.Arrays
import java.io.InputStream
import java.util.LinkedHashMap
import net.imagini.aim.types.AimType
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import net.imagini.aim.types.AimDataType
import net.imagini.aim.types.AimTypeAbstract
import net.imagini.aim.types.Aim
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

class MergeScanner(val partition: AimPartition, val selectFields: Array[String], val rowFilter: RowFilter)
  extends AbstractScanner {
  def this(partition: AimPartition, selectStatement: String, rowFilterStatement: String) = this(
    partition,
    if (selectStatement.contains("*")) partition.schema.names else selectStatement.split(",").map(_.trim),
    RowFilter.fromString(partition.schema, rowFilterStatement))
  def this(partition: AimPartition) = this(partition, "*", "*")

  override val schema: AimSchema = partition.schema.subset(selectFields)
  val keyField: String = partition.schema.name(0)
  override val keyColumn = schema.get(keyField)
  override val keyType = schema.get(keyColumn)
  val sortOrder = SortOrder.ASC

  private val scanSchema: AimSchema = partition.schema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  private val scanners: Seq[Array[Scanner]] = partition.segments.map(segment ⇒ segment.wrapScanners(scanSchema))
  private val scanColumnIndex: Array[Int] = schema.names.map(n ⇒ scanSchema.get(n))
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  rowFilter.updateFormula(scanSchema.names)

  private var currentSegment = -1

  override def mark = scanners.foreach(_.foreach(_.mark))

  override def reset = {
    scanners.foreach(_.foreach(_.reset))
    currentSegment = -1
  }

  override def skipRow = {
    for ((t, s) ← (scanSchema.fields zip scanners(currentSegment))) s.skip(t.getDataType)
    currentSegment = -1
  }

  override def selectRow: Array[ByteBuffer] = {
    if (currentSegment == -1) {
      for (s ← (0 to scanners.size - 1)) {
        //filter
        var segmentHasData = scanners(s).forall(columnScanner ⇒ !columnScanner.eof)
        while (segmentHasData && !rowFilter.matches(scanners(s).map(_.scan))) {
          for ((t, scanner) ← (scanSchema.fields zip scanners(s))) scanner.skip(t.getDataType)
          segmentHasData = scanners(s).forall(columnScanner ⇒ !columnScanner.eof)
        }
        //merge sort
        if (segmentHasData) {
          if (currentSegment == -1 || ((sortOrder == SortOrder.ASC ^ scanners(s)(scanKeyColumnIndex).compare(scanners(currentSegment)(scanKeyColumnIndex), keyType) > 0))) {
            currentSegment = s
          }
        }
      }
    }
    currentSegment match {
      case -1     ⇒ throw new EOFException
      case s: Int ⇒ scanColumnIndex.map(i ⇒ scanners(s)(i).scan)
    }
  }

}