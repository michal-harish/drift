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

case class Selected extends Throwable

class MergeScanner(val partition: AimPartition, val selectStatement: String, val rowFilterStatement: String, val groupFilterStatement: String) {
  def this(partition: AimPartition) = this(partition, "*", "*", "*")
  val selectDef = if (selectStatement.contains("*")) partition.schema.names else selectStatement.split(",").map(_.trim)
  val selectRef = selectDef.filter(partition.schema.has(_))
  val keyField: String = partition.schema.name(0)
  val rowFilter = RowFilter.fromString(partition.schema, rowFilterStatement)
  val groupFilter = RowFilter.fromString(partition.schema, groupFilterStatement)
  val sortOrder = SortOrder.ASC
  val selectSchema: AimSchema = new AimSchema(new LinkedHashMap(ListMap[String, AimType](
    selectDef.map(f ⇒ f match {
      case field: String if (partition.schema.has(field))    ⇒ (field -> partition.schema.field(field))
      case function: String if (function.contains("(group")) ⇒ new AimTypeGROUP(partition.schema, function).typeTuple
    }): _*).asJava))

  val groupFunctions: Seq[AimTypeGROUP] = selectSchema.fields.filter(_.isInstanceOf[AimTypeGROUP]).map(_.asInstanceOf[AimTypeGROUP])

  val scanSchema: AimSchema = partition.schema.subset(
    selectRef ++ rowFilter.getColumns ++ groupFilter.getColumns
      ++ groupFunctions.flatMap(_.filter.getColumns) ++ groupFunctions.map(_.field)
      :+ keyField)
  rowFilter.updateFormula(scanSchema.names)
  groupFilter.updateFormula(scanSchema.names)
  groupFunctions.map(_.filter.updateFormula(scanSchema.names))
  val keyColumn: Int = scanSchema.get(keyField)
  val keyType = scanSchema.get(keyColumn)
  val scanners: Seq[Array[Scanner]] = partition.segments.map(segment ⇒ segment.wrapScanners(scanSchema))
  val colIndex: Seq[Int] = selectRef.map(n ⇒ scanSchema.get(n)) ++ scanSchema.names.filter(!selectSchema.names.contains(_)).map(n ⇒ -1 * scanSchema.get(n) - 1)
  private var key: Array[Byte] = null
  private var transformedKey: Array[Byte] = null

  def nextRowAsString: String = rowAsString(nextRow)
  def rowAsString(streams: Seq[InputStream]): String = (selectSchema.fields, streams).zipped.map((t, s) ⇒ {
    //println(t)
    t.convert(PipeUtils.read(s, t.getDataType))
  }).foldLeft("")(_ + _ + " ")

  def nextRow: Seq[InputStream] = {
    val scanRow = nextRowScan
    val streams: Seq[InputStream] = selectSchema.names.map(f ⇒ selectSchema.field(f) match {
      case function: AimTypeGROUP ⇒ function.toInputStream
      case field: AimType         ⇒ scanRow(scanSchema.get(f))
    })
    //val streams = colIndex.filter(f ⇒ f >= 0).map(scanRow(_))
    colIndex.map(f ⇒ if (f < 0) PipeUtils.skip(scanRow(-1 * f - 1), scanSchema.get(-1 * f - 1).getDataType))
    streams
  }

  private def skipRow(row: Array[Scanner]) = for ((t, s) ← (scanSchema.fields zip row)) PipeUtils.skip(s, t.getDataType)

  private def nextRowScan: Seq[Scanner] = {
    while (true) {
      val row = unfilteredRow
      if (key == null || !Arrays.equals(row(keyColumn).asAimValue(keyType), key)) {
        nextGroup
      }
      while (Arrays.equals(row(keyColumn).asAimValue(keyType), key)) {
        if (rowFilter.matches(row)) {
          return row
        } else {
          skipRow(row)
        }
      }
    }
    throw new EOFException
  }

  private def nextGroup = {
    var satisfiesConstraint = groupFilter.isEmptyFilter
    groupFunctions.map(_.reset)
    do {
      var row = unfilteredRow
      scanners.foreach(_.foreach(_.mark))
      key = row(keyColumn).asAimValue(keyType)
      try {
        while ((!satisfiesConstraint || !groupFunctions.forall(_.satisfied)) && Arrays.equals(row(keyColumn).asAimValue(keyType), key)) {
          if (!satisfiesConstraint && groupFilter.matches(row)) {
            satisfiesConstraint = true
          }
          groupFunctions.filter(f ⇒ !f.satisfied && f.filter.matches(row)).foreach(f ⇒ {
            f.satisfy(row(scanSchema.get(f.field)).asAimValue(f.dataType))
          })
          if (!satisfiesConstraint || !groupFunctions.forall(_.satisfied)) {
            skipRow(row)
            row = unfilteredRow
          }
        }
      } catch {
        case e: EOFException ⇒ {}
      }
    } while (!satisfiesConstraint || !groupFunctions.forall(_.satisfied))

    scanners.foreach(_.foreach(_.reset))
  }

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

  class AimTypeGROUP(val schema: AimSchema, val definition: String) extends AimTypeAbstract {
    val alias = definition.substring(0, definition.indexOf("(group"))
    val body = definition.substring(alias.length + 6, definition.length - 1).trim
    val field = body.substring(0, body.indexOf(" "))
    val dataType = schema.field(field).getDataType
    val filter = RowFilter.fromString(schema, body.substring(body.indexOf(" where ") + 7).trim)
    private var value: InputStream = null
    def reset = value = null
    def satisfied = value != null
    def satisfy(value: Array[Byte]) = this.value = new ByteArrayInputStream(value)
    def toInputStream = { value.reset; value }
    override def getDataType = dataType
    override def toString = "GROUP[" + field + " WHERE" + filter + "]"
    override def convert(value: String): Array[Byte] = dataType.convert(value)
    override def convert(value: Array[Byte]): String = dataType.convert(value)
    def typeTuple: (String, AimTypeGROUP) = alias -> this
  }

}