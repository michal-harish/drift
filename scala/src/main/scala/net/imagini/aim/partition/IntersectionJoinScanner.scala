package net.imagini.aim.partition

import net.imagini.aim.types.AimSchema
import java.util.LinkedHashMap
import net.imagini.aim.types.AimType
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import java.nio.ByteBuffer
import net.imagini.aim.types.TypeUtils
import net.imagini.aim.tools.AbstractScanner
import java.io.EOFException

class IntersectionJoinScanner(val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  private val leftSelect = left.schema.names.map(n ⇒ (n -> left.schema.field(n)))
  private val rightSelect = right.schema.names.map(n ⇒ (n -> right.schema.field(n)))
  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](
    ListMap((leftSelect ++ rightSelect): _*).asJava))
  override val keyColumn = schema.get(left.schema.name(left.keyColumn))

  private val selectBuffer: Array[ByteBuffer] = new Array[ByteBuffer](schema.size)
  private val leftColumnIndex = schema.names.map(f ⇒ if (left.schema.has(f)) left.schema.get(f) else -1)
  private val rightColumnIndex = schema.names.map(f ⇒ if (right.schema.has(f)) right.schema.get(f) else -1)
  private var currentLeft = true
  private var currentKey: Option[ByteBuffer] = None
  private var eof = false
  right.next

  override def rewind = {
    left.rewind
    right.rewind
    currentLeft = true
    currentKey = null
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
    if (currentLeft) eof = !left.next else eof = !right.next
    if (!eof) {
      if (currentKey != None) {
        if (currentLeft && !TypeUtils.equals(left.selectKey, currentKey.get, keyType)) currentLeft = false
        if (!currentLeft && !TypeUtils.equals(right.selectKey, currentKey.get, keyType)) currentKey = None
      }
      if (currentKey == None) {
        var cmp: Int = -1
        do {
          cmp = TypeUtils.compare(left.selectKey, right.selectKey, keyType)
          if (cmp < 0) eof = !left.next
          else if (cmp > 0) eof = !right.next
        } while (!eof && cmp != 0)
        if (!eof) {
            currentKey = Some(left.selectKey.slice)
            currentLeft = true
        }
      }
    }
    move
    !eof
  }

  private def move = eof match {
    case true ⇒ for (i ← (0 to schema.size - 1)) selectBuffer(i) = null
    case false  ⇒ {
      val row = if (currentLeft) left.selectRow else right.selectRow
      (0 to schema.size - 1, if (currentLeft) leftColumnIndex else rightColumnIndex).zipped.map((i, c) ⇒ c match {
        case -1     ⇒ selectBuffer(i) = null
        case c: Int ⇒ selectBuffer(i) = row(c)
      })
    }
  }
}