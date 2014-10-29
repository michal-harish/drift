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

class OuterJoinScanner(val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  private val leftSelect = left.schema.names.map(n ⇒ (n -> left.schema.field(n)))
  private val rightSelect = right.schema.names.map(n ⇒ (n -> right.schema.field(n)))
  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](
    ListMap((leftSelect ++ rightSelect): _*).asJava))
  override val keyType = left.schema.get(left.keyColumn)
  override val keyColumn = schema.get(left.schema.name(left.keyColumn))

  private val leftColumnIndex = schema.names.map(f ⇒ if (left.schema.has(f)) left.schema.get(f) else -1)
  private val rightColumnIndex = schema.names.map(f ⇒ if (right.schema.has(f)) right.schema.get(f) else -1)
  private val sortOrder = SortOrder.ASC

  private var currentLeft = true
  private var leftHasData = true
  private var rightHasData = true

  override def next = if (currentLeft) left.next else right.next

  override def mark = { left.mark; right.mark }

  override def reset = { left.reset; right.reset }

  def selectRow: Array[ByteBuffer] = {
    //outer join
    try {
      left.selectKey
    } catch {
      case e: EOFException ⇒ leftHasData = false
    }
    try {
      right.selectKey
    } catch {
      case e: EOFException ⇒ rightHasData = false
    }

    if (rightHasData && leftHasData) {
      if (TypeUtils.compare(left.selectKey, right.selectKey, keyType) > 0 ^ sortOrder.equals(SortOrder.ASC)) {
        currentLeft = true
      } else {
        currentLeft = false
      }
    } else if (leftHasData) {
      currentLeft = true
    } else if (rightHasData) {
      currentLeft = false
    } else {
      throw new EOFException
    }

    //join select
    val row = if (currentLeft) left.selectRow else right.selectRow
    (if (currentLeft) leftColumnIndex else rightColumnIndex).map(c ⇒ c match {
      case -1     ⇒ null
      case i: Int ⇒ row(i)
    })
  }
}