package net.imagini.aim.segment

import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.utils.View
import net.imagini.aim.types.AimType
import java.io.EOFException
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.tools.BlockView

class SegmentScanner(val selectFields: Array[String], val rowFilter: RowFilter, val segment: AimSegment) extends AbstractScanner {

  def this(selectStatement: String, filterStatement: String, segment: AimSegment) =
    this(if (selectStatement.contains("*")) segment.getSchema.names else selectStatement.split(",").map(_.trim).toArray,
      RowFilter.fromString(segment.getSchema, filterStatement),
      segment)

  private val keyField: String = segment.getSchema.name(0)
  override val schema: AimSchema = segment.getSchema.subset(selectFields)
  private val numBlocks = segment.getBlockStorage(0).numBlocks
  private val scanSchema: AimSchema = segment.getSchema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  private val scanColumnIndex: Array[Int] = scanSchema.names.map(n ⇒ segment.getSchema.get(n))
  private val scanViews: Array[View] = scanColumnIndex.map(c ⇒ new BlockView(segment.getBlockStorage(c), segment.getSchema.dataType(c))).toArray
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  override val keyType: AimType = scanSchema.get(scanKeyColumnIndex)
  private val keyDataType = scanSchema.dataType(scanKeyColumnIndex)
  rowFilter.updateFormula(scanSchema.names)
  private val selectIndex = schema.names.map(n ⇒ scanSchema.get(n))

  var eof = false
  private var initialised = false
  private val selectViews: Array[View] = new Array[View](schema.size)

  override def selectKey: View = if (eof) throw new EOFException else scanViews(scanKeyColumnIndex)

  override def selectRow: Array[View] = if (eof) throw new EOFException else selectViews

  override def count: Long = {
    var count = 0
    while (next) {
      count += 1
    }
    count
  }

  override def next: Boolean = if (eof) false else {
    var filterMatch = true
    do {
      var i = 0
      while (i < scanViews.length) {
        if (initialised) {
          scanViews(i).skip
        }
        eof = eof || scanViews(i).eof
        i += 1
      }
      initialised = true
      if (!eof && !rowFilter.isEmptyFilter) {
        filterMatch = rowFilter.matches(scanViews)
      }
    } while (!eof && !filterMatch)
    select
    !eof
  }

  private def select = {
    val l = schema.size
    var i = 0
    while (i < l) {
      selectViews(i) = if (eof) null else scanViews(selectIndex(i))
      i += 1
    }
  }

}