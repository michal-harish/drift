package net.imagini.aim

import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.tools._ColumnScanner
import net.imagini.aim.types.Aim
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.segment.SegmentScanner
import net.imagini.aim.tools.RowFilter

class SegmentScannerLZ4Test extends FlatSpec with Matchers {

  def read(in: _ColumnScanner) = {
    if (in.eof) throw new EOFException
    val result = ""//in.dataType.asString(in.view)
    in.skip
    result
  }

  "Marking scanner" should "restore the correct block" in {
    val schema = AimSchema.fromString("a(STRING),b(INT)")
    val column1 = new BlockStorageLZ4()
    column1.addBlock(ByteUtils.createStringBuffer("Hello"))
    column1.addBlock(ByteUtils.createStringBuffer("World"))
    val segment = new AimSegmentUnsorted(schema, classOf[BlockStorageLZ4])

    segment.appendRecord("Hello", "101")
    segment.appendRecord("World", "99")
    segment.close

    val scanner = new SegmentScanner(Array("a","b"), RowFilter.fromString(schema, "*"), segment)
    scanner.mark
    scanner.next should be(true); scanner.selectLine("\t") should be("Hello\t101")
    scanner.next should be(true); scanner.selectLine("\t") should be("World\t99")
    scanner.reset
    scanner.next should be(true); scanner.selectLine("\t") should be("Hello\t101")
    scanner.rewind
    scanner.next should be(true); scanner.selectLine("\t") should be("Hello\t101")
    scanner.mark
    scanner.next should be(true); scanner.selectLine("\t") should be("World\t99")
    scanner.reset
    scanner.next should be(true); scanner.selectLine("\t") should be("World\t99")
    scanner.next should be(false); an[EOFException] must be thrownBy (scanner.selectLine("\t"))
    scanner.count should be (2L)
  }

  val value1: String =
    "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "1234567890abcdefghijklmnopqrstuvwxyz0987654321ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "\n"
  val value2: String =
    "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
      "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
      "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
      "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
      "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
      "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
      "*&^%$REFGHJIO()*&TYGHJKOsdilhp*(^o87tI&^ri7k6rftu,giUTku7yFKI7krtkuYrfkU" +
      "\n"

  val instance = new BlockStorageLZ4()
  instance.addBlock(ByteUtils.wrap((value1).getBytes))
  instance.addBlock(ByteUtils.wrap((value2).getBytes))
  instance.compressedSize should equal(168)
  instance.originalSize should equal(value1.length + value2.length)

  val scanner = new _ColumnScanner(instance, Aim.STRING)
  var ch: Char = 0
  var actualValue = "";
  while (!scanner.eof) {
    ch = scanner.read.toChar
    actualValue += ch
  }
  value1 + value2 should equal(actualValue)
}