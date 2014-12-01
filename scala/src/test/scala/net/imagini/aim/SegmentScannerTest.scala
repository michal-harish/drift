package net.imagini.aim

import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.segment.SegmentScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimTableDescriptor
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.types.SortType
import net.imagini.aim.region.AimRegion
import net.imagini.aim.segment.RowFilter

class SegmentScannerTest extends FlatSpec with Matchers {

  "Marking scanner" should "restore the correct block" in {
    val schema = AimSchema.fromString("a(STRING),b(INT)")
    val descriptor = new AimTableDescriptor(schema, 1000, classOf[BlockStorageMEMLZ4], SortType.NO_SORT)
    val region = new AimRegion("hello", descriptor)
    region.addTestRecords(
      Seq("Hello", "101"),
      Seq("World", "99"))
    region.compact

    val scanner = new SegmentScanner(Array("a", "b"), RowFilter.fromString(schema, "*"), region.segments(0))
    scanner.next should be(true); scanner.selectLine("\t") should be("Hello\t101")
    scanner.next should be(true); scanner.selectLine("\t") should be("World\t99")
    scanner.next should be(false); an[EOFException] must be thrownBy (scanner.selectLine("\t"))
    val countScanner = new SegmentScanner(Array("a", "b"), RowFilter.fromString(schema, "*"), region.segments(0))
    countScanner.count should be(2L)

    val scanner2 = new SegmentScanner(Array("a", "b"), RowFilter.fromString(schema, "a contains 'W'"), region.segments(0))
    scanner2.next should be(true); scanner2.selectLine("\t") should be("World\t99")
    scanner2.next should be(false); an[EOFException] must be thrownBy (scanner2.selectLine("\t"))
  }
}