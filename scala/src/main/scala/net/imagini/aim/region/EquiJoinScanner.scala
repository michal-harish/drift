package net.imagini.aim.region

import java.io.EOFException
import scala.collection.mutable.LinkedList
import net.imagini.aim.types.AimSchema
import java.util.LinkedHashMap
import scala.collection.immutable.ListMap
import net.imagini.aim.types.AimType
import scala.collection.JavaConverters._
import net.imagini.aim.segment.AbstractScanner
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.utils.View
import net.imagini.aim.types.TypeUtils

class EquiJoinScanner(val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  val leftSelect = left.schema.names
  val rightSelect = right.schema.names.filter(!left.schema.has(_))

  private val selectMap = ListMap((
    left.schema.names.map(f ⇒ (f -> left.schema.field(f))) ++
    right.schema.names.filter(!left.schema.has(_)).map(f ⇒ (f -> right.schema.field(f)))): _*)

  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](selectMap.asJava))
  override val keyType: AimType = left.keyType
  private val numFields = schema.size
  private val leftSelectIndex: Array[Int] = selectMap.keys.filter(left.schema.has(_)).map(left.schema.get(_)).toArray
  private val rightSelectIndex: Array[Int] = selectMap.keys.filter(f ⇒ (!left.schema.has(f) && right.schema.has(f))).map(right.schema.get(_)).toArray
  private var selectedKey: View = null
  private val selectBuffer: Array[View] = new Array[View](schema.size)
  private var eof = false
  left.next // FIXME this will mess up the counting, use initialised var instead

  override def selectKey: View = if (eof) throw new EOFException else selectedKey

  override def selectRow: Array[View] = if (eof) throw new EOFException else selectBuffer

  override def next: Boolean = {
    if (eof) return false
    eof = !right.next
    if (!eof) {
      var cmp: Int = -1
      do {
        cmp = TypeUtils.compare(left.selectKey, right.selectKey, keyType)
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
      var i = 0; while (i < numFields) {
        selectBuffer(i) = null
        i += 1
      }
    }
    case false ⇒ {
      selectedKey = left.selectKey
      val leftRow = left.selectRow
      val rightRow = right.selectRow
      var i = 0; while (i < leftSelectIndex.length) {
        selectBuffer(i) = leftRow(leftSelectIndex(i))
        i += 1
      }
      var j = 0; while (j < rightSelectIndex.length) {
          selectBuffer(j + leftSelectIndex.length) = rightRow(rightSelectIndex(j))
          j += 1
      }
    }
  }

  override def count: Long = {
    var count = 0
    //TODO optimize - do not call next when counting
    while (!eof && right.next) {
      var cmp: Int = -1
      do {
        cmp = TypeUtils.compare(left.selectKey, right.selectKey, keyType)
        if (cmp < 0) eof = !left.next
        else if (cmp > 0) eof = !right.next
      } while (!eof && cmp != 0)
      count += 1
    }
    eof = true
    count
  }
}