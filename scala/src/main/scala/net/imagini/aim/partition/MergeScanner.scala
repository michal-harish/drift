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

case class Selected extends Throwable

/**
 * TODO refactor filter initialisation - at the moment the filter requires calling updateFormuls
 * because the set of scanners available for matching is dictated by what needs to be selected plus what needs to be filtered
 *
 * TODO scannerToInputStream converter tool
 */
class MergeScanner(val partition: AimPartition, val selectDef: String, val rowFilterDef: String, val groupFilterDef: String) /*extends InputStream*/ {
  def this(partition: AimPartition) = this(partition, "*", "*", "*")
  val keyField: String = partition.schema.name(0)
  val selectSchema = if (selectDef.contains("*")) partition.schema else partition.schema.subset(selectDef.split(","))
  val rowFilter = RowFilter.fromString(partition.schema, rowFilterDef)
  val groupFilter = RowFilter.fromString(partition.schema, groupFilterDef)
  val sortOrder = SortOrder.ASC

  val scanSchema: AimSchema = partition.schema.subset(selectSchema.names ++ rowFilter.getColumns ++ groupFilter.getColumns :+ keyField)
  rowFilter.updateFormula(scanSchema.names)
  groupFilter.updateFormula(scanSchema.names)
  val keyColumn: Int = scanSchema.get(keyField)
  val keyType = scanSchema.get(keyColumn)
  val scanners: Seq[Array[Scanner]] = partition.segments.map(segment ⇒ segment.wrapScanners(scanSchema))
  val colIndex: Seq[Int] = selectSchema.names.map(n ⇒ scanSchema.get(n)) ++ scanSchema.names.filter(!selectSchema.names.contains(_)).map(n ⇒ -1 * scanSchema.get(n) - 1)

  def nextRowAsString: String = rowAsString(nextRow)

  def rowAsString(row: Seq[Scanner]): String = (selectSchema.fields, row).zipped.map((t, s) ⇒ t.convert(PipeUtils.read(s, t.getDataType))).foldLeft("")(_ + _ + " ")

  def nextRow: Seq[Scanner] = {
    var key = nextGroup
    var row = unfilteredRow
    try {
      while (true) {
        while (Arrays.equals(row(keyColumn).asAimValue(keyType), key)) {
          if (rowFilter.matches(row)) {
            throw new Selected
          } else {
            skipRow(row)
            row = unfilteredRow
          }
        }
        key = nextGroup
      }
    } catch {
      case e: Selected ⇒ {}
    }
    colIndex.filter(f ⇒ { if (f < 0) { PipeUtils.skip(row(-1 * f - 1), scanSchema.get(-1 * f - 1).getDataType); false } else true }).map(row(_))
  }

  private def nextGroup: Array[Byte] = {
    var key: Array[Byte] = null
    var satisfiesConstraint = false
    while (!satisfiesConstraint) {
      var row = unfilteredRow
      scanners.foreach(_.foreach(_.mark))
      key = row(keyColumn).asAimValue(keyType)
      try {
        while (!satisfiesConstraint && Arrays.equals(row(keyColumn).asAimValue(keyType), key)) {
          if (groupFilter.matches(row)) {
            satisfiesConstraint = true
          } else {
            skipRow(row)
            row = unfilteredRow
          }
        }
      } catch {
        case e: EOFException ⇒ {}
      }
    }
    scanners.foreach(_.foreach(_.reset))
    key
  }

  private def skipRow(row: Array[Scanner]) = for ((t, s) ← (scanSchema.fields zip row)) PipeUtils.skip(s, t.getDataType)

  def unfilteredRow: Array[Scanner] = {
    var nextSegment = -1
    for (s ← (0 to scanners.size - 1)) {
      if (scanners(s).forall(columnScanner ⇒ !columnScanner.eof)) {
        if (nextSegment == -1 || ((sortOrder == SortOrder.ASC ^ scanners(s)(keyColumn).compare(scanners(nextSegment)(keyColumn), keyType) > 0))) {
          nextSegment = s
        }
      }
    }
    nextSegment match {
      case -1     ⇒ throw new EOFException
      case s: Int ⇒ scanners(s)
    }
  }

}