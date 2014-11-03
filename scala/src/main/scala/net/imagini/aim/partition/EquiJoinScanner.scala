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

class EquiJoinScanner(val select: Array[String], val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  val leftSelect = left.schema.names
  val rightSelect = right.schema.names.filter(!left.schema.has(_))

  override val keyColumn = 0

  private val selectMap = ListMap(select.map(_ match {
    case f if (left.schema.has(f))  ⇒ f -> left.schema.field(f)
    case f if (right.schema.has(f)) ⇒ f -> right.schema.field(f)
  }): _*)

  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](selectMap.asJava))

  private val leftSelectIndex:Array[Int] = selectMap.keys.filter(left.schema.has(_)).map(left.schema.get(_)).toArray
  private val rightSelectIndex:Array[Int] = selectMap.keys.filter(f =>(!left.schema.has(f) && right.schema.has(f))).map(right.schema.get(_)).toArray
  private val selectBuffer: Array[ByteBuffer] = new Array[ByteBuffer](schema.size)
  private var eof = false
  left.next

  override def rewind = {
    left.rewind
    right.rewind
    eof = false
    move
  }

  override def mark = {
    left.mark
    right.mark
  }

  override def reset = {
    left.reset
    right.reset
    eof = false
    move
  }

  override def selectRow: Array[ByteBuffer] = if (eof) throw new EOFException else selectBuffer

  override def next: Boolean = {
    if (eof) return false
    eof = !right.next
    if (!eof) {
      var cmp: Int = -1
      do {
        cmp = TypeUtils.compare(left.selectKey, right.selectKey, left.keyType)
        if (cmp < 0) eof = !left.next
        else if (cmp > 0) eof = !right.next
      } while (!eof && cmp != 0)
    }
    move
    !eof
  }

  private def move = eof match {
    case true ⇒ for (i ← (0 to schema.size - 1)) selectBuffer(i) = null
    case false ⇒ {
      val leftRow = left.selectRow
      val rightRow = right.selectRow
      for(i <- (0 to leftSelectIndex.size - 1)) selectBuffer(i) = leftRow(leftSelectIndex(i))
      for(i <- (0 to rightSelectIndex.size - 1)) selectBuffer(i + leftSelectIndex.size) = rightRow(rightSelectIndex(i))
    }
  }

}