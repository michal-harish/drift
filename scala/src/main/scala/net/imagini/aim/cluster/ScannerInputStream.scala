package net.imagini.aim.cluster

import java.io.InputStream
import java.util.concurrent.atomic.AtomicInteger
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.utils.View

/**
 * we want to read byte by byte all the available records in the provided scanner
 */
final class ScannerInputStream(val scanner: AbstractScanner) extends InputStream {

  val numColumns = scanner.schema.size
  var row: Array[View] = null
  var col = -1
  var offset = new AtomicInteger(-1)
  var length = -1

  override def read: Int = if (checkNextByte) {
    row(col).array(row(col).offset + offset.getAndIncrement) & 0xff
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
    col += 1
    if (col >= numColumns) {
      col = 0
      offset.set(0)
      false
    } else {
      offset.set(0)
      length = scanner.schema.get(col).getDataType.sizeOf(row(col))
      //System.err.println(scanner.schema.get(col).asString(row(col)))
      true
    }
  }
  private def selectNextRow: Boolean = {
    if (!scanner.next) {
      length = 0
      offset.set(0)
      col = numColumns -1
      false
    } else {
      row = scanner.selectRow
      col = 0
      scanner.schema.get(col).getDataType.sizeOf(row(col))
      length = scanner.schema.get(col).getDataType.sizeOf(row(col))
      offset.set(0)
      true
    }
  }

}