package net.imagini.aim.cluster

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import net.imagini.aim.tools.AbstractScanner

/**
 * we want to read byte by byte all the available records in the provided scanner
 */
final class ScannerInputStream(val scanner: AbstractScanner) extends InputStream {

  val numColumns = scanner.schema.size
  var row: Array[ByteBuffer] = null
  var column = -1
  var offset = new AtomicInteger(-1)
  var length = -1

  override def read: Int = if (checkNextByte) {
    row(column).get(row(column).position + offset.getAndIncrement) & 0xff
  } else {
    -1
  }

  override def skip(n: Long): Long = {
    for (i ← (1L to n)) {
      if (!checkNextByte) {
        return i
      }
    }
    n 
  }

  selectNextRow

  private def checkNextByte: Boolean = {
    if (offset.get < length || selectNextColumn || selectNextRow) {
      return true
    }
    false
  }

  private def selectNextColumn: Boolean = {
    column += 1
    if (column >= numColumns) {
      column = 0
      offset.set(0)
      false
    } else {
      offset.set(0)
      length = scanner.schema.get(column).getDataType.sizeOf(row(column))
      true
    }
  }
  private def selectNextRow: Boolean = {
    if (!scanner.next) {
      length = 0
      offset.set(0)
      column = numColumns -1
      false
    } else {
      row = scanner.selectRow
      column = 0
      scanner.schema.get(column).getDataType.sizeOf(row(column))
      length = scanner.schema.get(column).getDataType.sizeOf(row(column))
      offset.set(0)
      true
    }
  }

}