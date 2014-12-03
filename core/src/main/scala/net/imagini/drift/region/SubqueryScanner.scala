package net.imagini.drift.region

import net.imagini.drift.segment.AbstractScanner
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.segment.RowFilter
import java.util.LinkedHashMap
import net.imagini.drift.types.DriftType
import scala.collection.immutable.ListMap
import scala.collection.JavaConverters._
import java.io.EOFException
import net.imagini.drift.utils.View

class SubqueryScanner(val select: Array[String], val rowFilter: RowFilter, val scanner: AbstractScanner) extends AbstractScanner {

  rowFilter.updateFormula(scanner.schema.names)

  override val schema: DriftSchema = scanner.schema.subset(select)

  override val keyType: DriftType = scanner.keyType

  private val selectIndex = schema.names.map(t ⇒ scanner.schema.get(t))

  private var selectedKey: View = null

  private val selectBuffer: Array[View] = new Array[View](schema.size)

  private var eof = false

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

  override def selectKey: View = if (eof) throw new EOFException else selectedKey

  override def selectRow: Array[View] = if (eof) throw new EOFException else selectBuffer

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
    throw new NotImplementedError
    eof = true
    0
  }
}