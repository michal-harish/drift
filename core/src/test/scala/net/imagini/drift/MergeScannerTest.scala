package net.imagini.drift

import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.drift.region.AimRegion
import net.imagini.drift.segment.MergeScanner
import net.imagini.drift.types.AimSchema
import net.imagini.drift.utils.BlockStorageMEMLZ4
import net.imagini.drift.types.AimTableDescriptor
import net.imagini.drift.types.SortType

class MergeScannerTest extends FlatSpec with Matchers {

  "ScannerMerge " should "produce same result as stream merge" in {
    val descriptor = new AimTableDescriptor(
      AimSchema.fromString("user_uid(UUID),column(STRING),value(STRING)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val region = new AimRegion("vdna.events", descriptor)
    region.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}"),
      Seq("17b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}"))
    region.compact

    region.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876"),
      Seq("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}"))
    region.compact

    val scanner = new MergeScanner("user_uid,column", "*", region.segments)

    scanner.next should be(true); scanner.selectLine(",") should be("17b22cfb-a29e-42c3-a3d9-12d32850e103,addthis_id")
    scanner.next should be(true); scanner.selectLine(",") should be("17b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,addthis_id")
    scanner.next should be(false)
    an[EOFException] must be thrownBy scanner.selectLine(",")

    val countScanner = new MergeScanner("user_uid,column", "*", region.segments)
    countScanner.count should be(6L)

    val mergeScan = new MergeScanner("user_uid,value", "column='pageview'", region.segments)
    mergeScan.nextLine should be("17b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.music.com}")
    mergeScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.auto.com}")
    mergeScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.ebay.com}")
    mergeScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.travel.com}")
    an[EOFException] must be thrownBy mergeScan.nextLine
    an[EOFException] must be thrownBy mergeScan.nextLine

  }
}