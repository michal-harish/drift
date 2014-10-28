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

  val rightSelect = right.schema.names
  val leftSelect = left.schema.names.filter(!right.schema.has(_))
  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](
    (ListMap((leftSelect.map(n ⇒ (n -> left.schema.field(n))) ++ rightSelect.map(n ⇒ ((n) -> right.schema.field(n)))): _*).asJava)))

  override val keyType = right.schema.get(right.keyColumn)
  override val keyColumn = schema.get(right.schema.name(right.keyColumn))
  private val leftSelectIndex = leftSelect.map(f ⇒ left.schema.get(f))

  override def skipRow = right.skipRow

  override def mark = { left.mark; right.mark }

  override def reset = { left.reset; right.reset }

  def selectRow: Array[ByteBuffer] = {
    //inner join
    var cmp: Int = -1
    do {
      cmp = TypeUtils.compare(left.selectKey, right.selectKey, keyType)
      if (cmp < 0) left.skipRow
      else if (cmp > 0) right.skipRow
    } while (cmp != 0)
    //equi select
    val leftRow = left.selectRow
    val rightRow = right.selectRow
    leftSelectIndex.map(c ⇒ leftRow(c)) ++ rightRow
  }

}