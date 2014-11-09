package net.imagini.aim.segment

import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.utils.View
import net.imagini.aim.types.AimType
import java.io.EOFException

class SegmentScanner(val selectFields: Array[String], val rowFilter: RowFilter, val segment: AimSegment) extends AbstractScanner {

  private val keyField: String = segment.getSchema.name(0)
  override val schema: AimSchema = segment.getSchema.subset(selectFields)
  private val numBlocks = segment.getBlockStorage(0).numBlocks
  private val scanSchema: AimSchema = segment.getSchema.subset(selectFields ++ rowFilter.getColumns :+ keyField)
  private val scanColumnIndex: Array[Int] = scanSchema.names.map(n ⇒ segment.getSchema.get(n))
  private val scanKeyColumnIndex: Int = scanSchema.get(keyField)
  override val keyType: AimType = scanSchema.get(scanKeyColumnIndex)
  private val keyDataType = scanSchema.dataType(scanKeyColumnIndex)
  override val keyLen: Int = keyType.getDataType.getLen
  rowFilter.updateFormula(scanSchema.names)
  private val selectIndex = schema.names.map(n ⇒ scanSchema.get(n))

  private var currentBlock = -1
  private var scanViews: Array[View] = new Array[View](scanSchema.size)
  private var selectViews: Array[View] = new Array[View](schema.size)
  private var markBlock = -1
  private var markKey: View = null
  private var markViews: Array[View] = null
  var eof = !switchToBlock(0)

  private def select = selectViews = if (scanViews == null) null else selectIndex.map(c ⇒ scanViews(c))

  override def count: Long = {
    if (rowFilter.isEmptyFilter()) {
      segment.count
    } else {
        rewind
        var count = 0
        while (!eof) if (next) {
          count += 1
        }
        count
    }
  }

  override def rewind = {
    eof = !switchToBlock(0)
    scanViews = null
    select
  }

  override def mark = {
    markBlock = currentBlock
    markViews = if (scanViews == null) null else scanViews.map(new View(_)).toArray
  }

  override def reset = {
    currentBlock = markBlock
    scanViews = markViews
    markKey = null
    markViews = null
    markBlock = -1
    select
  }

  override def next: Boolean = {
    while (!eof && !nextBlockRow) {
      eof = !switchToBlock(currentBlock + 1)
    }
    !eof
  }

  private def nextBlockRow: Boolean = {
    var blockHasData = true
    var filterMatch = true
    if (blockHasData) do {
      if (scanViews == null) {
        //TODO optimize
        scanViews = scanColumnIndex.map(c ⇒ segment.getBlockStorage(c).view(currentBlock)).toArray
      } else {
        var i = 0
        while (i < scanViews.length) {
          scanViews(i).offset += scanSchema.dataType(i).sizeOf(scanViews(i))
          blockHasData = blockHasData && !scanViews(i).eof
          i += 1
        }
      }
      if (blockHasData && !rowFilter.isEmptyFilter) {
        filterMatch = rowFilter.matches(scanViews)
      }
    } while (blockHasData && !filterMatch)
    select
    blockHasData
  }

  override def selectKey: View = if (eof) throw new EOFException else scanViews(scanKeyColumnIndex)

  override def selectRow: Array[View] = if (eof) throw new EOFException else selectViews

  private def switchToBlock(block: Int): Boolean = {
    val validBlock = block > -1 && block < numBlocks;
    if (currentBlock != block) {
      if (currentBlock > -1 && currentBlock < numBlocks) {
        scanColumnIndex.map(c ⇒ segment.getBlockStorage(c).close(currentBlock))
      }
    }
    currentBlock = block
    if (validBlock) {
      scanViews = null
      select
      true
    }
    validBlock
  }

}