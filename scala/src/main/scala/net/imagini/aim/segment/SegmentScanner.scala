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
  private var counted = 0

  private val fetcher = new Thread() {
    private val scanFilterView: Array[View] = new Array[View](scanBuffers.size)
    override def run = {
      var filterMatch = true
      var done = false
      while(!done) {
        var i = 0
        try {
          while (i < scanStreams.length) {
            scanFilterView(i) = scanBuffers(i).asInstanceOf[RollingBufferView].select(scanStreams(i))
            i += 1
          }
          if (!eof && !rowFilter.isEmptyFilter) {
            filterMatch = rowFilter.matches(scanFilterView)
          }
          if (filterMatch) {
            var i = 0
            while (i < scanStreams.length) {
              scanBuffers(i).asInstanceOf[RollingBufferView].keep
              i += 1
            }
            counted += 1
          }
        } catch {
          //FIXME eof has to be marked in the rolling buffers not just by a bool var
          case e: EOFException ⇒ {
            done = true
            var i = 0
            while (i < scanStreams.length) {
              scanBuffers(i).asInstanceOf[RollingBufferView].markEof
              i += 1
            }
          }
        }
      }
    }
  }

  fetcher.start

  override def selectKey: View = if (eof) throw new EOFException else scanBuffers(scanKeyColumnIndex)

  override def selectRow: Array[View] = if (eof) throw new EOFException else scanBuffers

  override def count: Long = {
    while (next) {}
    counted
  }

  override def next: Boolean = if (eof) false else {
    if (eof) {
      return false
    }
    var i = 0
    while (i < scanStreams.length) {
      val rollingBuffer = scanBuffers(i).asInstanceOf[RollingBufferView]
      if (rollingBuffer.eof) {
        eof = true
      } else {
        rollingBuffer.next
      }
      i += 1
    }
    !eof
  }

}