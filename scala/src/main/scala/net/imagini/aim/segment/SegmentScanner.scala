package net.imagini.aim.segment

import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.View
import net.imagini.aim.types.AimType
import java.io.EOFException
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.types.RollingBufferView
import net.imagini.aim.cluster.StreamUtils

class SegmentScanner(val selectFields: Array[String], val rowFilter: RowFilter, val segment: AimSegment) extends AbstractScanner {

  def this(selectStatement: String, filterStatement: String, segment: AimSegment) =
    this(if (selectStatement.contains("*")) segment.getSchema.names else selectStatement.split(",").map(_.trim).toArray,
      RowFilter.fromString(segment.getSchema, filterStatement),
      segment)

  private val keyField: String = segment.getSchema.name(0)
  override val schema: AimSchema = segment.getSchema.subset(selectFields)
  private val numBlocks = segment.getBlockStorage(0).numBlocks
  //TODO do not add filter columns and key column if already selected
  private val scanSchema: AimSchema = segment.getSchema.subset(selectFields ++ rowFilter.getColumns.filter(!selectFields.contains(_)) :+ keyField)
  private val scanColumnIndex: Array[Int] = scanSchema.names.map(n ⇒ segment.getSchema.get(n))
  private val scanStreams = scanColumnIndex.map(c ⇒ segment.getBlockStorage(c).toInputStream).toArray
  private val scanBuffers: Array[View] = scanSchema.fields.map(f ⇒ new RollingBufferView(8192, f)).toArray
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  override val keyType: AimType = scanSchema.get(scanKeyColumnIndex)
  private val keyDataType = scanSchema.dataType(scanKeyColumnIndex)
  rowFilter.updateFormula(scanSchema.names)
  var eof = false

  override def selectKey: View = if (eof) throw new EOFException else scanBuffers(scanKeyColumnIndex)

  override def selectRow: Array[View] = if (eof) throw new EOFException else scanBuffers

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
      try {
        while (i < scanStreams.length) {
          scanBuffers(i).asInstanceOf[RollingBufferView].select(scanStreams(i))
          i += 1
        }
      } catch {
        case e: EOFException ⇒ eof = true
      }
      if (!eof && !rowFilter.isEmptyFilter) {
        filterMatch = rowFilter.matches(scanBuffers)
        if (!filterMatch) {
          scanBuffers.map(b => b.offset -= 1) //unselect - TODO optimize
        }
      }
    } while (!eof && !filterMatch)
    if (!eof) {
      var i = 0
      while (i < scanBuffers.length) {
        scanBuffers(i).asInstanceOf[RollingBufferView].keep
        i += 1
      }
      //System.err.println((scanSchema.fields, scanBuffers).zipped.map((t, b) ⇒ t.asString(b)).mkString(" "))
    }
    !eof
  }

}