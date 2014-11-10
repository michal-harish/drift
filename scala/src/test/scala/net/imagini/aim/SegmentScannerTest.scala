package net.imagini.aim

import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.types.Aim
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.segment.SegmentScanner
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.utils.View

class SegmentScannerTest extends FlatSpec with Matchers {


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
}