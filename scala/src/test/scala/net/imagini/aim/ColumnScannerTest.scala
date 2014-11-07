package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.utils.BlockStorageLZ4
import java.nio.ByteBuffer
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.tools.ColumnScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.Aim
import java.io.InputStream
import java.io.EOFException

class ColumnScannerTest extends FlatSpec with Matchers {

  def read(in: ColumnScanner) = {
    if (in.eof) throw new EOFException 
    val result = in.dataType.asString(in.view)
    in.skip
    result
  }

  "Marking scanner" should "restore the correct block" in {
    val storage = new BlockStorageLZ4()
    storage.addBlock(ByteUtils.createStringBuffer("Hello"))
    storage.addBlock(ByteUtils.createStringBuffer("World"))
    val scanner = new ColumnScanner(storage, Aim.STRING)
    read(scanner) should be("Hello")
    scanner.mark
    read(scanner) should be("World")
    scanner.reset
    read(scanner) should be("World")
    scanner.rewind
    read(scanner) should be("Hello")
    scanner.mark
    read(scanner) should be("World")
    scanner.reset
    read(scanner) should be("World")
    an[EOFException] must be thrownBy(read(scanner))
  }

}