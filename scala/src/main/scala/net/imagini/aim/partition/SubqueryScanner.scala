package net.imagini.aim.partition

import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.tools.RowFilter
import java.util.LinkedHashMap
import net.imagini.aim.types.AimType
import scala.collection.immutable.ListMap
import scala.collection.JavaConverters._
import java.nio.ByteBuffer
import java.io.EOFException

class SubqueryScanner(val select: Array[String], val rowFilter: RowFilter, val scanner: AbstractScanner) extends AbstractScanner {

  rowFilter.updateFormula(scanner.schema.names)

  override val schema: AimSchema = scanner.schema.subset(select)

  override val keyType: AimType = scanner.keyType

  private val selectIndex = schema.names.map(t ⇒ scanner.schema.get(t))

  private var selectedKey: ByteBuffer = null

  private val selectBuffer: Array[ByteBuffer] = new Array[ByteBuffer](schema.size)

  private var eof = false

  override def rewind = {
    scanner.rewind
    eof = false
    move
  }

  override def mark = scanner.mark

  override def reset = {
    scanner.reset
    eof = false
    move
  }

  override def next: Boolean = {
    do {
      if (!scanner.next) {
        eof = true
        move
        return false
      }
    } while (!rowFilter.matches(scanner.selectRow))
    move
    true
  }

  override def selectKey: ByteBuffer = if (eof) throw new EOFException else selectedKey

  override def selectRow: Array[ByteBuffer] = if (eof) throw new EOFException else selectBuffer

  private def move = eof match {
    case true ⇒ {
      selectedKey = null
      for (i ← (0 to schema.size - 1)) selectBuffer(i) = null
    }
    case false ⇒ {
      selectedKey = scanner.selectKey
      val row = scanner.selectRow
      for (i ← (0 to schema.size - 1)) selectBuffer(i) = row(selectIndex(i))
    }
  }

  override def count: Long = {
    rewind
    throw new NotImplementedError
    eof = true
    0
  }
}