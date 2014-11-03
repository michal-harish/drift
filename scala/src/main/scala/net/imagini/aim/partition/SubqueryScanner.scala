package net.imagini.aim.partition

import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.tools.RowFilter
import java.util.LinkedHashMap
import net.imagini.aim.types.AimType
import scala.collection.immutable.ListMap
import scala.collection.JavaConverters._
import java.nio.ByteBuffer

class SubqueryScanner(val select: Array[String], val rowFilter: RowFilter, val scanner: AbstractScanner) extends AbstractScanner {

  rowFilter.updateFormula(scanner.schema.names)

  override val schema: AimSchema = scanner.schema.subset(select)

  override val keyColumn = schema.get(scanner.schema.name(scanner.keyColumn))

  private val selectIndex = schema.names.map(t ⇒ scanner.schema.get(t))

  override def rewind = scanner.rewind

  override def next: Boolean = {
    do {
      if (!scanner.next) return false
    } while (!rowFilter.matches(scanner.selectRow))
    true
  }

  override def mark = scanner.mark

  override def reset = scanner.reset

  def selectRow: Array[ByteBuffer] = {
    val row = scanner.selectRow
    selectIndex.map(c ⇒ row(c)).toArray
  }
}