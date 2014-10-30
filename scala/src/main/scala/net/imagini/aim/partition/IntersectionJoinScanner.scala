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

  private val leftColumnIndex = schema.names.map(f ⇒ if (left.schema.has(f)) left.schema.get(f) else -1)
  private val rightColumnIndex = schema.names.map(f ⇒ if (right.schema.has(f)) right.schema.get(f) else -1)
  private var currentLeft = true
  private var currentKey: ByteBuffer = null

  right.next

  override def next: Boolean = {
    if (currentLeft) left.next else right.next
    //inner join
    if (currentKey != null && currentLeft && !TypeUtils.equals(left.selectKey, currentKey, keyType)) currentLeft = false
    if (currentKey != null && !currentLeft && !TypeUtils.equals(right.selectKey, currentKey, keyType)) currentKey = null
    try {
      if (currentKey == null) {
        var cmp: Int = -1
        do {
          cmp = TypeUtils.compare(left.selectKey, right.selectKey, keyType)
          if (cmp < 0) left.next
          else if (cmp > 0) right.next
        } while (cmp != 0)
        currentKey = left.selectKey.slice
        currentLeft = true
      }
      true
    } catch {
      case e: EOFException ⇒ false
    }

  }

  override def mark = { left.mark; right.mark }

  override def reset = { left.reset; right.reset }

  def selectRow: Array[ByteBuffer] = {
    //join select
    val row = if (currentLeft) left.selectRow else right.selectRow
    (if (currentLeft) leftColumnIndex else rightColumnIndex).map(c ⇒ c match {
      case -1     ⇒ null
      case i: Int ⇒ row(i)
    })
  }

}