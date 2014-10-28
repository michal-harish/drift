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

  val keyField: String = partition.schema.name(0)
  val sortOrder = SortOrder.ASC
  val selectSchema: AimSchema = partition.schema.subset(selectFields)

  override val schema: AimSchema = partition.schema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  rowFilter.updateFormula(schema.names)
  val keyColumn: Int = schema.get(keyField)
  val keyType = schema.get(keyColumn)
  val scanners: Seq[Array[Scanner]] = partition.segments.map(segment ⇒ segment.wrapScanners(schema))

  private var currentSegment = -1

  def mark = scanners.foreach(_.foreach(_.mark))

  def reset = {
    scanners.foreach(_.foreach(_.reset))
    currentSegment = -1
  }

  //TODO Array[ByteBuffer] - requires reafctoring filters
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
    for ((t, s) ← (schema.fields zip scanners(currentSegment))) s.skip(t.getDataType)
    currentSegment = -1
  }

  def selectNextFilteredRow = while (!rowFilter.matches(currentRow)) skipCurrentRow

  def scanCurrentKey: ByteBuffer = currentRow(keyColumn).scan()

  def scanCurrentRow: Seq[ByteBuffer] = {
    selectNextFilteredRow
    val buffers = selectSchema.names.map(f ⇒ currentRow(schema.get(f)).scan())
    buffers
  }

  protected[aim] def rowAsString: String = {
    (selectSchema.fields, scanCurrentRow).zipped.map((t, b) ⇒ t.asString(b)).foldLeft("")(_ + _ + " ")
  }

  protected[aim] def nextResultAsString: String = {
    val result = rowAsString
    skipCurrentRow
    result
  }

}