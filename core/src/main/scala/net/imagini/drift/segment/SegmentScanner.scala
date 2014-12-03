package net.imagini.drift.segment

import net.imagini.drift.types.DriftSchema
import net.imagini.drift.utils.View
import net.imagini.drift.types.DriftType
import java.io.EOFException
import net.imagini.drift.utils.BlockStorage
import net.imagini.drift.cluster.StreamUtils

class SegmentScanner(val selectFields: Array[String], val rowFilter: RowFilter, val segment: DriftSegment) extends AbstractScanner {

  def this(selectStatement: String, filterStatement: String, segment: DriftSegment) =
    this(if (selectStatement.contains("*")) segment.getSchema.names else selectStatement.split(",").map(_.trim).toArray,
      RowFilter.fromString(segment.getSchema, filterStatement),
      segment)

  private val keyField: String = segment.getSchema.name(0)
  override val schema: DriftSchema = segment.getSchema.subset(selectFields)
  //TODO do not add filter columns and key column if already selected
  private val scanSchema: DriftSchema = segment.getSchema.subset(selectFields ++ rowFilter.getColumns.filter(!selectFields.contains(_)) :+ keyField)
  private val scanColumnIndex: Array[Int] = scanSchema.names.map(n ⇒ segment.getSchema.get(n))
  private var scanViews = scanColumnIndex.map(c ⇒ segment.getBlockStorage(c).toView).toArray
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  override val keyType: DriftType = scanSchema.get(scanKeyColumnIndex)
  rowFilter.updateFormula(scanSchema.names)
  var eof = false
  var initialized = false

  override def selectKey: View = if (eof) throw new EOFException else scanViews(scanKeyColumnIndex)

  override def selectRow: Array[View] = if (eof) throw new EOFException else scanViews

  override def count: Long = {
    var counted = 0
    while (next) {
      counted += 1
    }
    counted
  }

  override def next: Boolean = if (eof) false else {
    if (eof) {
      return false
    }
    var filterMatch = true;
    do {
      var i = 0
      while (i < scanViews.length) {
        val view = scanViews(i)
        if (initialized) {
          view.skip
        }
        if (!view.available(scanSchema.get(i).getLen)) {
          eof = true
        }
        i += 1
      }
      initialized = true
      if (!eof && !rowFilter.isEmptyFilter) {
        filterMatch = rowFilter.matches(scanViews);
      }
    } while (!eof && !filterMatch)
    !eof
  }

}