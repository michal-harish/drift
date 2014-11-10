package net.imagini.aim.segment

import java.io.EOFException
import java.util.LinkedHashMap
import scala.Array.canBuildFrom
import scala.Array.fallbackCanBuildFrom
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.immutable.ListMap
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType
import net.imagini.aim.types.AimTypeAbstract
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.utils.View
import net.imagini.aim.types.TypeUtils

//TODO either optimize with selectBuffer or simply extend MergeScanner
class GroupScanner(
  val groupSelectStatement: String,
  val rowFilterStatement: String,
  val groupFilterStatement: String,
  val segments: Seq[AimSegment])
  extends AbstractScanner {

  private val sourceSchema: AimSchema = segments(0).getSchema
  val selectDef = if (groupSelectStatement.contains("*")) sourceSchema.names else groupSelectStatement.split(",").map(_.trim)
  val selectRef = selectDef.filter(sourceSchema.has(_))

  override val schema: AimSchema = new AimSchema(new LinkedHashMap(ListMap[String, AimType](
    selectDef.map(f ⇒ f match {
      case field: String if (sourceSchema.has(field))        ⇒ (field -> sourceSchema.field(field))
      case function: String if (function.contains("(group")) ⇒ new AimTypeGROUP(sourceSchema, function).typeTuple
    }): _*).asJava))

  private val rowFilter = RowFilter.fromString(sourceSchema, rowFilterStatement)
  private val groupFilter = RowFilter.fromString(sourceSchema, groupFilterStatement)
  private val groupFunctions: Seq[AimTypeGROUP] = schema.fields.filter(_.isInstanceOf[AimTypeGROUP]).map(_.asInstanceOf[AimTypeGROUP])
  private val keyField = sourceSchema.name(0)
  private val scanColumns: Array[String] =
    selectRef ++
      rowFilter.getColumns ++
      groupFilter.getColumns ++
      groupFunctions.flatMap(_.filter.getColumns) ++
      groupFunctions.map(_.field) :+ keyField
  private val merge = new MergeScanner(scanColumns, RowFilter.fromString(sourceSchema, "*"), segments)
  override val keyType = sourceSchema.get(0)
  val keyDataType = keyType.getDataType
  rowFilter.updateFormula(merge.schema.names)
  groupFilter.updateFormula(merge.schema.names)
  groupFunctions.map(_.filter.updateFormula(merge.schema.names))
  private val mergeColumnIndex: Array[Int] = schema.names.map(n ⇒ if (merge.schema.has(n)) merge.schema.get(n) else -1)
  private var groupKey: View = null
  private var eof = false

  override def rewind = {
    merge.rewind
    eof = false
  }
  override def mark = {
    merge.mark //FIXME this needs its own mark, e.g. x = merge.mark ... merge.reset(x)
  }
  override def reset = {
    merge.reset
  }

  override def next: Boolean = {
    if (eof) {
      false
    } else {
      eof = !merge.next
      while (!eof) {
        if (groupKey == null || !TypeUtils.equals(merge.selectKey, groupKey, keyDataType)) {
          selectGroup
        }
        if (!eof && rowFilter.matches(merge.selectRow)) {
          return true
        } else {
          eof = !merge.next
        }
      }
      false
    }
  }

  override def selectKey: View = if (eof) throw new EOFException else groupKey

  override def selectRow: Array[View] = if (eof) throw new EOFException else {
    val mergeRow = merge.selectRow
    (schema.fields, (0 to schema.fields.length)).zipped.map((f, c) ⇒ f match {
      case function: AimTypeGROUP ⇒ function.toView
      case field: AimType         ⇒ mergeRow(mergeColumnIndex(c))
    })
  }

  def selectGroup = {
    var satisfiesFilter = groupFilter.isEmptyFilter
    groupFunctions.map(_.reset)
    var satisfiesGroupFunctions = groupFunctions.forall(_.satisfied)
    groupKey = new View(merge.selectKey)
    merge.mark
    while (!eof && (!satisfiesFilter || !satisfiesGroupFunctions)) {
      if (!satisfiesFilter && groupFilter.matches(merge.selectRow)) {
        satisfiesFilter = true
      }
      satisfiesGroupFunctions = groupFunctions.filter(f ⇒ !f.satisfied).forall(f ⇒ if (f.filter.matches(merge.selectRow)) {
        val groupFieldPos = merge.schema.get(f.field)
        f.satisfy(merge.selectRow(groupFieldPos))
        true
      } else {
        false
      })
      if (!satisfiesFilter || !satisfiesGroupFunctions) {
        eof = !merge.next
        if (!eof && !TypeUtils.equals(merge.selectKey, groupKey, keyDataType)) {
          groupKey = new View(merge.selectKey)
          merge.mark
        }
      }
    }
    merge.reset
    eof = !merge.next
  }
  override def count: Long = {
    rewind
    //TODO count for group filter after decision about refactor above
    eof = true
    0
  }

}

class AimTypeGROUP(val schema: AimSchema, val definition: String) extends AimTypeAbstract {
  val alias = definition.substring(0, definition.indexOf("(group"))
  val body = definition.substring(alias.length + 6, definition.length - 1).trim
  val field = body.substring(0, body.indexOf(" "))
  val dataType = schema.field(field).getDataType
  val filter = RowFilter.fromString(schema, body.substring(body.indexOf(" where ") + 7).trim)
  private var pointer: View = null
  def reset = pointer = null
  def satisfied = pointer != null
  def satisfy(value: View) = pointer = new View(value)
  def toView = pointer
  override def getDataType = dataType
  override def toString = "GROUP[" + field + " WHERE" + filter + "]"
  override def convert(value: String): Array[Byte] = dataType.convert(value)
  override def convert(value: Array[Byte]): String = dataType.convert(value)
  override def asString(value: View): String = dataType.asString(value)
  override def partition(value: View, numPartitions: Int): Int = throw new IllegalArgumentException
  def typeTuple: (String, AimTypeGROUP) = alias -> this
}
