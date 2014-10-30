package net.imagini.aim.partition

import java.io.EOFException
import scala.collection.mutable.LinkedList
import net.imagini.aim.types.AimSchema
import java.util.LinkedHashMap
import scala.collection.immutable.ListMap
import net.imagini.aim.types.AimType
import scala.collection.JavaConverters._
import net.imagini.aim.types.TypeUtils
import java.nio.ByteBuffer
import net.imagini.aim.tools.AbstractScanner

class EquiJoinScanner(val selectStatement: String, val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  val leftSelect = left.schema.names
  val rightSelect = right.schema.names.filter(!left.schema.has(_))

  override val keyColumn = 0

  private val selectMap = ListMap(selectStatement.split(",").map(_.trim).map(_ match {
    case f if (left.schema.has(f)) => f -> left.schema.field(f)
    case f if (right.schema.has(f)) => f -> right.schema.field(f)
  }):_*)

  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](selectMap.asJava))

  private val leftSelectIndex = selectMap.filter(t => left.schema.has(t._1)).map(t => left.schema.get(t._1))
  private val rightSelectIndex = selectMap.filter(t => !left.schema.has(t._1) && right.schema.has(t._1)).map(t => right.schema.get(t._1))

  left.next

  override def next: Boolean = {
    try {
      right.next
      //inner join
      var cmp: Int = -1
      do {
        cmp = TypeUtils.compare(left.selectKey, right.selectKey, left.keyType)
        if (cmp < 0) left.next
        else if (cmp > 0) right.next
      } while (cmp != 0)
      true
    } catch {
      case e: EOFException ⇒ false
    }
  }

  override def mark = { left.mark; right.mark }

  override def reset = { left.reset; right.reset }

  def selectRow: Array[ByteBuffer] = {

    //equi select
    val leftRow = left.selectRow
    val rightRow = right.selectRow
    leftSelectIndex.map(c => leftRow(c)).toArray ++ rightSelectIndex.map(c ⇒ rightRow(c))
  }

}