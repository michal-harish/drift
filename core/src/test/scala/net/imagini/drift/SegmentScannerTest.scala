package net.imagini.drift

import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.drift.segment.DriftSegment
import net.imagini.drift.segment.DriftSegment
import net.imagini.drift.segment.SegmentScanner
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.types.DriftTableDescriptor
import net.imagini.drift.utils.BlockStorageMEMLZ4
import net.imagini.drift.types.SortType
import net.imagini.drift.region.DriftRegion
import net.imagini.drift.segment.RowFilter

class SegmentScannerTest extends FlatSpec with Matchers {

  "Marking scanner" should "restore the correct block" in {
    val schema = DriftSchema.fromString("a(STRING),b(INT)")
    val descriptor = new DriftTableDescriptor(schema, 1000, classOf[BlockStorageMEMLZ4], SortType.NO_SORT)
    val region = new DriftRegion("hello", descriptor)
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