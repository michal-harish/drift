package net.imagini.aim.partition

import net.imagini.aim.tools.RowFilter
import java.io.EOFException
import java.util.Arrays
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimTypeAbstract
import net.imagini.aim.tools.Scanner
import java.util.LinkedHashMap
import scala.collection.immutable.ListMap
import net.imagini.aim.types.AimType
import net.imagini.aim.types.Aim
import scala.collection.JavaConverters._
import net.imagini.aim.tools.PipeUtils
import net.imagini.aim.utils.ByteUtils
import java.nio.ByteBuffer
import net.imagini.aim.types.TypeUtils

class GroupScanner(val partition: AimPartition, val groupSelectStatement: String, val rowFilterStatement: String, val groupFilterStatement: String) 
extends AbstractScanner {

  val selectDef = if (groupSelectStatement.contains("*")) partition.schema.names else groupSelectStatement.split(",").map(_.trim)
  val selectRef = selectDef.filter(partition.schema.has(_))
  override val schema: AimSchema = new AimSchema(new LinkedHashMap(ListMap[String, AimType](
    selectDef.map(f ⇒ f match {
      case field: String if (partition.schema.has(field))    ⇒ (field -> partition.schema.field(field))
      case function: String if (function.contains("(group")) ⇒ new AimTypeGROUP(partition.schema, function).typeTuple
      case empty: String                                     ⇒ empty -> Aim.EMPTY
    }): _*).asJava))

  val rowFilter = RowFilter.fromString(partition.schema, rowFilterStatement)
  val groupFilter = RowFilter.fromString(partition.schema, groupFilterStatement)
  val groupFunctions: Seq[AimTypeGROUP] = schema.fields.filter(_.isInstanceOf[AimTypeGROUP]).map(_.asInstanceOf[AimTypeGROUP])

  val requiredColumns = selectRef ++ rowFilter.getColumns ++ groupFilter.getColumns ++ groupFunctions.flatMap(_.filter.getColumns) ++ groupFunctions.map(_.field)

  val merge = new MergeScanner(partition, requiredColumns, RowFilter.fromString(partition.schema, "*"))

  rowFilter.updateFormula(merge.schema.names)
  groupFilter.updateFormula(merge.schema.names)
  groupFunctions.map(_.filter.updateFormula(merge.schema.names))

  //TODO work on storage and scanner is required to keep the buffers decompressed if marked
  private var currentKey: ByteBuffer = null

  def selectCurrentFilteredGroup = {
    var satisfiesFilter = groupFilter.isEmptyFilter
    groupFunctions.map(_.reset)
    do {
      merge.mark
      currentKey = merge.currentRow(merge.keyColumn).slice
      try {
        while ((!satisfiesFilter || !groupFunctions.forall(_.satisfied)) && merge.currentRow(merge.keyColumn).equals(currentKey, merge.keyType)) {
          if (!satisfiesFilter && groupFilter.matches(merge.currentRow)) {
            satisfiesFilter = true
          }
          groupFunctions.filter(f ⇒ !f.satisfied && f.filter.matches(merge.currentRow)).foreach(f ⇒ {
            f.satisfy(merge.currentRow(merge.schema.get(f.field)).scan())
          })
          if (!satisfiesFilter || !groupFunctions.forall(_.satisfied)) {
            merge.skipCurrentRow
          }
        }
      } catch {
        case e: EOFException ⇒ {}
      }
    } while (!satisfiesFilter || !groupFunctions.forall(_.satisfied))
    merge.reset
  }

  def selectNextFilteredGroup = {
    if (currentKey != null) {
      while (merge.currentRow(merge.keyColumn).equals(currentKey, merge.keyType)) {
        merge.skipCurrentRow
      }
    }
    selectCurrentFilteredGroup
  }

  def selectNextGroupRow: Boolean = {
    if (currentKey == null) {
      false
    } else {
      merge.selectNextFilteredRow
      if (merge.currentRow(merge.keyColumn).equals(currentKey, merge.keyType)) {
        true
      } else {
        false
      }
    }
  }

  def scanCurrentRow: Seq[ByteBuffer] = {
    while (!selectNextGroupRow || !rowFilter.matches(merge.currentRow)) {
      if (!selectNextGroupRow) selectNextFilteredGroup else merge.skipCurrentRow
    }
    val buffers: Seq[ByteBuffer] = schema.names.map(f ⇒ schema.field(f) match {
      case function: AimTypeGROUP                      ⇒ function.toByteBuffer
      case empty: AimType if (empty.equals(Aim.EMPTY)) ⇒ null
      case field: AimType                              ⇒ merge.currentRow(merge.schema.get(f)).scan()
    })
    buffers
  }
  protected[aim] def currentRowAsString: String = {
    (schema.fields, scanCurrentRow).zipped.map((t, b) ⇒ t.asString(b)).foldLeft("")(_ + _ + " ")
  }

  protected[aim] def nextResultAsString: String = {
    val result = currentRowAsString
    merge.skipCurrentRow
    result
  }

}

class AimTypeGROUP(val schema: AimSchema, val definition: String) extends AimTypeAbstract {
  val alias = definition.substring(0, definition.indexOf("(group"))
  val body = definition.substring(alias.length + 6, definition.length - 1).trim
  val field = body.substring(0, body.indexOf(" "))
  val dataType = schema.field(field).getDataType
  val filter = RowFilter.fromString(schema, body.substring(body.indexOf(" where ") + 7).trim)
  private var buffer: ByteBuffer = null
  def reset = buffer = null
  def satisfied = buffer != null
  def satisfy(value: ByteBuffer) = buffer = value.slice()
  def toByteBuffer = { buffer.rewind(); buffer }
  override def getDataType = dataType
  override def toString = "GROUP[" + field + " WHERE" + filter + "]"
  override def convert(value: String): Array[Byte] = dataType.convert(value)
  override def convert(value: Array[Byte]): String = dataType.convert(value)
  override def asString(value: ByteBuffer): String = dataType.asString(value)
  def typeTuple: (String, AimTypeGROUP) = alias -> this
}
