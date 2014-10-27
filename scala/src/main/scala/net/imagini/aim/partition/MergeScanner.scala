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
import scala.xml.dtd.EMPTY
import net.imagini.aim.utils.BlockStorageRaw

case class Selected extends Throwable

class MergeScanner(val partition: AimPartition, val selectFields: Array[String], val rowFilter: RowFilter) {
  def this(partition: AimPartition, selectStatement: String, rowFilterStatement: String) = this(
    partition,
    if (selectStatement.contains("*")) partition.schema.names else selectStatement.split(",").map(_.trim),
    RowFilter.fromString(partition.schema, rowFilterStatement))
  def this(partition: AimPartition) = this(partition, "*", "*")

  val keyField: String = partition.schema.name(0)
  val sortOrder = SortOrder.ASC
  val selectSchema: AimSchema = partition.schema.subset(selectFields)

  val scanSchema: AimSchema = partition.schema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  rowFilter.updateFormula(scanSchema.names)
  val keyColumn: Int = scanSchema.get(keyField)
  val keyType = scanSchema.get(keyColumn)
  val scanners: Seq[Array[Scanner]] = partition.segments.map(segment ⇒ segment.wrapScanners(scanSchema))

  private var currentSegment = -1

  def consumed = currentSegment = -1

  def mark = scanners.foreach(_.foreach(_.mark))

  def reset = {
    scanners.foreach(_.foreach(_.reset))
    consumed
  }

  def currentRow: Array[Scanner] = {
    if (currentSegment == -1) {
      for (s ← (0 to scanners.size - 1)) {
        if (scanners(s).forall(columnScanner ⇒ !columnScanner.eof)) {
          if (currentSegment == -1 || ((sortOrder == SortOrder.ASC ^ scanners(s)(keyColumn).compare(scanners(currentSegment)(keyColumn), keyType) > 0))) {
            currentSegment = s
          }
        }
      }
    }
    currentSegment match {
      case -1     ⇒ throw new EOFException
      case s: Int ⇒ scanners(s)
    }
  }

  def skipCurrentRow = {
    for ((t, s) ← (scanSchema.fields zip scanners(currentSegment))) PipeUtils.skip(s, t.getDataType)
    consumed
  }

  def selectNextFilteredRow = while (!rowFilter.matches(currentRow)) skipCurrentRow

  def nextResult: Seq[Scanner] = {
    selectNextFilteredRow
    val streams: Seq[Scanner] = selectSchema.names.map(f ⇒ currentRow(scanSchema.get(f)))
    scanSchema.names.filter(!selectFields.contains(_)).map(n ⇒ {
      val hiddenColumn = scanSchema.get(n)
      PipeUtils.skip(currentRow(hiddenColumn), scanSchema.dataType(hiddenColumn))
    })
    consumed

    streams
  }

  protected[aim] def nextResultAsString: String = rowAsString(nextResult)

  protected[aim] def rowAsString(streams: Seq[Scanner]): String = (selectSchema.fields, streams).zipped.map((t, s) ⇒ t.convert(PipeUtils.read(s, t.getDataType))).foldLeft("")(_ + _ + " ")

}