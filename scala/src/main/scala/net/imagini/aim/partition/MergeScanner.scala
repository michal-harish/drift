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
 *  because the set of scanners available for matching is dictated by what needs to be selected plus what needs to be filtered
 */
class MergeScanner(val partition: AimPartition, val rowFilterDef: String, val groupFilterDef: String) /*extends InputStream*/ {
  def this(partition: AimPartition, rowFilterDef: String) = this(partition, rowFilterDef, "*")
  def this(partition: AimPartition) = this(partition, "*", "*")
  val schema = partition.schema
  val rowFilter = RowFilter.fromString(schema, rowFilterDef)
  rowFilter.updateFormula(schema.names)
  val groupFilter = RowFilter.fromString(schema, groupFilterDef)
  groupFilter.updateFormula(schema.names)
  val sortOrder = SortOrder.ASC
  val keyColumn: Int = 0
  val keyField: String = schema.name(keyColumn)
  val keyType = schema.get(keyColumn)
  val scanners: Seq[Array[Scanner]] = partition.segments.map(segment ⇒ segment.wrapScanners(schema))

  def nextRow: Array[Scanner] = {
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
    row
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

  private def skipRow(row: Array[Scanner]) = for ((t, s) ← (schema.fields zip row)) PipeUtils.skip(s, t.getDataType)
  def rowAsString(row: Array[Scanner]): String = (schema.fields, row).zipped.map((t, s) ⇒ t.convert(PipeUtils.read(s, t.getDataType))).foldLeft("")(_ + _ + " ")
  def nextRowAsString: String = rowAsString(nextRow)

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