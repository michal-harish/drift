package net.imagini.aim

import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.types.Aim
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.segment.SegmentScanner
import net.imagini.aim.segment.RowFilter
import net.imagini.aim.utils.View

class SegmentScannerTest extends FlatSpec with Matchers {


  "Marking scanner" should "restore the correct block" in {
    val schema = AimSchema.fromString("a(STRING),b(INT)")
    val column1 = new BlockStorageMEMLZ4()
    column1.addBlock(ByteUtils.createStringBuffer("Hello"))
    column1.addBlock(ByteUtils.createStringBuffer("World"))
    val segment = new AimSegmentUnsorted(schema).initStorage(classOf[BlockStorageMEMLZ4])

    segment.appendRecord("Hello", "101")
    segment.appendRecord("World", "99")
    segment.close

    val scanner = new SegmentScanner(Array("a","b"), RowFilter.fromString(schema, "*"), segment)
    scanner.next should be(true); scanner.selectLine("\t") should be("Hello\t101")
    scanner.next should be(true); scanner.selectLine("\t") should be("World\t99")
    scanner.next should be(false); an[EOFException] must be thrownBy (scanner.selectLine("\t"))
     val countScanner = new SegmentScanner(Array("a","b"), RowFilter.fromString(schema, "*"), segment)
    countScanner.count should be (2L)

    val scanner2 = new SegmentScanner(Array("a","b"), RowFilter.fromString(schema, "a contains 'W'"), segment)
    scanner2.next should be(true); scanner2.selectLine("\t") should be("World\t99")
    scanner2.next should be(false); an[EOFException] must be thrownBy (scanner2.selectLine("\t"))
  }
}