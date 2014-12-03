package net.imagini.drift.region

import java.io.EOFException
import java.util.LinkedHashMap
import scala.Array.canBuildFrom
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.immutable.ListMap
import net.imagini.drift.segment.AbstractScanner
import net.imagini.drift.types.AimSchema
import net.imagini.drift.types.AimType
import net.imagini.drift.types.SortOrder
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import net.imagini.drift.utils.ByteUtils
import net.imagini.drift.utils.View
import net.imagini.drift.types.TypeUtils


class UnionJoinScanner(val left: AbstractScanner, val right: AbstractScanner) extends AbstractScanner {

  private val leftSelect = left.schema.names.map(n ⇒ (n -> left.schema.field(n)))
  private val rightSelect = right.schema.names.map(n ⇒ (n -> right.schema.field(n)))
  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](
    ListMap((leftSelect ++ rightSelect): _*).asJava))
  override val keyType: AimType = left.keyType
  private val leftColumnIndex = schema.names.map(f ⇒ if (left.schema.has(f)) left.schema.get(f) else -1)
  private val rightColumnIndex = schema.names.map(f ⇒ if (right.schema.has(f)) right.schema.get(f) else -1)
  private val sortOrder = SortOrder.ASC

  private var initialised = false
  private var currentLeft = true
  private var leftHasData = false
  private var rightHasData = false
  private var selectedKey: View = null
  private val selectBuffer: Array[View] = new Array[View](schema.size)

  override def selectKey: View = if (!rightHasData && !leftHasData) throw new EOFException else selectedKey

  override def selectRow: Array[View] = if (!rightHasData && !leftHasData) throw new EOFException else selectBuffer

  override def next: Boolean = {

    if (!initialised) {
        leftHasData = true
        rightHasData = right.next
        initialised = true;
    }
    if (!rightHasData && !leftHasData) return false

    if (currentLeft) {
      leftHasData = left.next
    } else {
      rightHasData = right.next
    }

    if (rightHasData && leftHasData) {
      currentLeft = TypeUtils.compare(left.selectKey, right.selectKey, keyType) > 0 ^ sortOrder.equals(SortOrder.ASC)
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
    case false ⇒ {
      selectedKey = null
      for (i ← (0 to schema.size - 1)) selectBuffer(i) = null
    }
    case true ⇒ {
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
    val executor = Executors.newFixedThreadPool(2)
    val l = executor.submit(new Callable[Long] { override def call: Long = left.count })
    val r = executor.submit(new Callable[Long] { override def call: Long = right.count })
    l.get + r.get
  }
}