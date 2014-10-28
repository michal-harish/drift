package net.imagini.aim.partition

import net.imagini.aim.types.AimSchema
import java.util.LinkedHashMap
import net.imagini.aim.types.AimType
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import java.nio.ByteBuffer
import net.imagini.aim.types.TypeUtils

class InnerJoinScanner(val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  private val leftSelect = left.schema.names.map(n ⇒ (n -> left.schema.field(n)))
  private val rightSelect = right.schema.names.map(n ⇒ (n -> right.schema.field(n)))
  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](
    ListMap((leftSelect ++ rightSelect): _*).asJava))
  override val keyType = left.schema.get(left.keyColumn)
  override val keyColumn = schema.get(left.schema.name(left.keyColumn))

  val leftColumnIndex = schema.names.map(f ⇒ if (left.schema.has(f)) left.schema.get(f) else -1)
  val rightColumnIndex = schema.names.map(f ⇒ if (right.schema.has(f)) right.schema.get(f) else -1)

  var currentLeft = true
  var currentKey: ByteBuffer = null
  override def skipRow = {
    if (currentLeft) left.skipRow else right.skipRow
  }

  override def mark = { left.mark; right.mark }

  override def reset = { left.reset; right.reset }

  def selectRow: Array[ByteBuffer] = {
    //inner join
    if (currentKey != null && currentLeft && !TypeUtils.equals(left.selectKey, currentKey, keyType)) currentLeft = false
    if (currentKey != null && !currentLeft && !TypeUtils.equals(right.selectKey, currentKey, keyType)) currentKey = null
    if (currentKey == null) {
      var cmp: Int = -1
      do {
        cmp = TypeUtils.compare(left.selectKey, right.selectKey, keyType)
        if (cmp < 0) left.skipRow
        else if (cmp > 0) right.skipRow
      } while (cmp != 0)
      currentKey = left.selectKey.slice
      currentLeft = true
    }
    //join select
    val row = if (currentLeft) left.selectRow else right.selectRow
    (if (currentLeft) leftColumnIndex else rightColumnIndex).map(c ⇒ c match {
      case -1     ⇒ null
      case i: Int ⇒ row(i)
    })
  }
}