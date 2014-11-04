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

class EquiJoinScanner(val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  val leftSelect = left.schema.names
  val rightSelect = right.schema.names.filter(!left.schema.has(_))

  private val selectMap = ListMap((
   left.schema.names.map(f => (f -> left.schema.field(f))) ++ 
   right.schema.names.filter(!left.schema.has(_)).map(f => (f -> right.schema.field(f)))
  ): _*)

  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](selectMap.asJava))
  override val keyType: AimType = left.keyType

  private val leftSelectIndex:Array[Int] = selectMap.keys.filter(left.schema.has(_)).map(left.schema.get(_)).toArray
  private val rightSelectIndex:Array[Int] = selectMap.keys.filter(f =>(!left.schema.has(f) && right.schema.has(f))).map(right.schema.get(_)).toArray
  private var selectedKey: ByteBuffer = null
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

  override def selectKey: ByteBuffer = if (eof) throw new EOFException else selectedKey

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
    case true ⇒ {
      selectedKey = null
      for (i ← (0 to schema.size - 1)) selectBuffer(i) = null
    }
    case false ⇒ {
      selectedKey = left.selectKey
      val leftRow = left.selectRow
      val rightRow = right.selectRow
      for(i <- (0 to leftSelectIndex.size - 1)) selectBuffer(i) = leftRow(leftSelectIndex(i))
      for(i <- (0 to rightSelectIndex.size - 1)) selectBuffer(i + leftSelectIndex.size) = rightRow(rightSelectIndex(i))
    }
  }

  override def count: Long = {
    rewind
    var count = 0
    //TODO optimize - do not call next when counting
    while(!eof && right.next) {
      var cmp: Int = -1
      do {
        cmp = TypeUtils.compare(left.selectKey, right.selectKey, left.keyType)
        if (cmp < 0) eof = !left.next
        else if (cmp > 0) eof = !right.next
      } while (!eof && cmp != 0)
        count += 1
    }
    eof = true
    count
  }
}