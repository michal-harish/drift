package net.imagini.aim.region

import net.imagini.aim.types.AimSchema
import java.util.LinkedHashMap
import net.imagini.aim.types.AimType
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.tools.AbstractScanner
import java.io.EOFException
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.utils.View
import net.imagini.aim.types.TypeUtils

class IntersectionJoinScanner(val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  private val leftSelect = left.schema.names.map(n ⇒ (n -> left.schema.field(n)))
  private val rightSelect = right.schema.names.map(n ⇒ (n -> right.schema.field(n)))
  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](
    ListMap((leftSelect ++ rightSelect): _*).asJava))
  override val keyType: AimType = left.keyType
  val keyDataType = keyType.getDataType

  private var selectedKey: View = null
  private val selectBuffer: Array[View] = new Array[View](schema.size)
  private val leftColumnIndex = schema.names.map(f ⇒ if (left.schema.has(f)) left.schema.get(f) else -1)
  private val rightColumnIndex = schema.names.map(f ⇒ if (right.schema.has(f)) right.schema.get(f) else -1)
  private var currentLeft = true
  private var currentKey: Option[View] = None
  private var eof = false
  right.next // FIXME this will mess up the counting, use initialised var instead

  override def selectKey: View = if (eof) throw new EOFException else selectedKey

  override def selectRow: Array[View] = if (eof) throw new EOFException else selectBuffer

  override def next: Boolean = {
    if (eof) return false
    if (currentLeft) eof = !left.next else eof = !right.next
    if (!eof) {
      if (currentKey != None) {
        if (currentLeft && !TypeUtils.equals(left.selectKey, currentKey.get, keyDataType)) currentLeft = false
        if (!currentLeft && !TypeUtils.equals(right.selectKey, currentKey.get, keyDataType)) currentKey = None
      }
      if (currentKey == None) {
        var cmp: Int = -1
        do {
          cmp = TypeUtils.compare(left.selectKey, right.selectKey, keyDataType)
          if (cmp < 0) eof = !left.next
          else if (cmp > 0) eof = !right.next
        } while (!eof && cmp != 0)
        if (!eof) {
          currentKey = Some(new View(left.selectKey))
          currentLeft = true
        }
      }
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
      val row = if (currentLeft) {
        selectedKey = left.selectKey
        left.selectRow
      } else {
        selectedKey = right.selectKey
        right.selectRow
      }
      (0 to schema.size - 1, if (currentLeft) leftColumnIndex else rightColumnIndex).zipped.map((i, c) ⇒ c match {
        case -1     ⇒ selectBuffer(i) = null
        case c: Int ⇒ selectBuffer(i) = row(c)
      })
    }
  }

  override def count: Long = {
    throw new NotImplementedError
    eof = true
    0
  }

}