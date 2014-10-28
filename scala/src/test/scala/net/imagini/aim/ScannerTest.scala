package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.utils.BlockStorageLZ4
import java.nio.ByteBuffer
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.tools.Scanner
import net.imagini.aim.tools.PipeUtils
import net.imagini.aim.types.AimSchema
import java.io.InputStream

class ScannerTest extends FlatSpec with Matchers {

  val schema = AimSchema.fromString("line(STRING)")
  def read(in: Scanner) = {
    in.eof 
    val result = schema.dataType(0).asString(in.scan)
    in.skip(schema.dataType(0))
    result
  }

  "Marking scanner" should "restore the correct block" in {
    val storage = new BlockStorageLZ4()
    storage.addBlock(ByteUtils.createStringBuffer("Hello"))
    storage.addBlock(ByteUtils.createStringBuffer("World"))
    val scanner = new Scanner(storage)
    read(scanner) should be("Hello")
    scanner.mark
    read(scanner) should be("World")
    scanner.reset
    read(scanner) should be("World")
  }

}