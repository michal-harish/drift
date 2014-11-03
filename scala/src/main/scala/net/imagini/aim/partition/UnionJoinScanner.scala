package net.imagini.aim.partition

import java.io.EOFException
import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.Array.canBuildFrom
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.immutable.ListMap

import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType
import net.imagini.aim.types.SortOrder
import net.imagini.aim.types.TypeUtils

class UnionJoinScanner(val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  private val leftSelect = left.schema.names.map(n ⇒ (n -> left.schema.field(n)))
  private val rightSelect = right.schema.names.map(n ⇒ (n -> right.schema.field(n)))
  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](
    ListMap((leftSelect ++ rightSelect): _*).asJava))
  override val keyColumn = schema.get(left.schema.name(left.keyColumn))

  private val leftColumnIndex = schema.names.map(f ⇒ if (left.schema.has(f)) left.schema.get(f) else -1)
  private val rightColumnIndex = schema.names.map(f ⇒ if (right.schema.has(f)) right.schema.get(f) else -1)
  private val sortOrder = SortOrder.ASC

  private var currentLeft = true
  private var leftHasData = true
  private var rightHasData = right.next
  private val selectBuffer: Array[ByteBuffer] = new Array[ByteBuffer](schema.size)

  override def rewind = {
    left.rewind
    right.rewind
    currentLeft = true
    leftHasData = true
    rightHasData = right.next
    move
  }

  override def mark = {
    left.mark;
    right.mark
  }

  override def reset = {
    //TODO reset should remember whether left and right have had data at mark
    left.reset
    right.reset
    move
  }

  override def selectRow: Array[ByteBuffer] = if (!rightHasData && !leftHasData) throw new EOFException else selectBuffer

  override def next: Boolean = {

    if (!rightHasData && !leftHasData) return false

    if (currentLeft) {
      leftHasData = left.next
    } else {
      rightHasData = right.next
    }

    if (rightHasData && leftHasData) {
      currentLeft = TypeUtils.compare(left.selectKey, right.selectKey, left.keyType) > 0 ^ sortOrder.equals(SortOrder.ASC)
    } else if (rightHasData ^ leftHasData) {
      currentLeft = leftHasData
    } else {
      move
      return false
    }
    move
    true
  }

  private def move = (rightHasData || leftHasData) match {
    case false ⇒ for (i ← (0 to schema.size - 1)) selectBuffer(i) = null
    case true ⇒ {
      val row = if (currentLeft) left.selectRow else right.selectRow
      (0 to schema.size - 1, if (currentLeft) leftColumnIndex else rightColumnIndex).zipped.map((i, c) ⇒ c match {
        case -1     ⇒ selectBuffer(i) = null
        case c: Int ⇒ selectBuffer(i) = row(c)
      })
    }
  }
}