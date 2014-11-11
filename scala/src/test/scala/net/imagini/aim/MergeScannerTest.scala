package net.imagini.aim

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageMEMLZ4
import java.io.EOFException

class MergeScannerTest extends FlatSpec with Matchers {

  "ScannerMerge " should "produce same result as stream merge" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),column(STRING),value(STRING)")
    val s1 = new AimSegmentQuickSort(schema, classOf[BlockStorageMEMLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}")
    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}")

    val s2 = new AimSegmentQuickSort(schema, classOf[BlockStorageMEMLZ4])
    s2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}")
    s2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876")
    s2.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}")

    val partition = new AimPartition(schema, 1000)
    //TODO either implement partition.appendRecord or move segment size paremeter to the parition loader 
    partition.add(s1)
    partition.add(s2)

    val scanner = new MergeScanner("user_uid,column", "*", partition.segments)

    scanner.next should be(true); scanner.selectLine(",") should be("17b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.next should be(true); scanner.selectLine(",") should be("17b22cfb-a29e-42c3-a3d9-12d32850e103,addthis_id")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,addthis_id")
    scanner.mark
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.reset
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,addthis_id")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,pageview")
    scanner.next should be(false)
    an[EOFException] must be thrownBy scanner.selectLine(",")
    scanner.count should be (6L)

    //TODO select only subset of columns: user_uid, value
    val mergeScan = new MergeScanner("user_uid,value", "column='pageview'", partition.segments)

    mergeScan.nextLine should be("17b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.music.com}")
    mergeScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.ebay.com}")
    mergeScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.auto.com}")
    mergeScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.travel.com}")

    an[EOFException] must be thrownBy mergeScan.nextLine
    an[EOFException] must be thrownBy mergeScan.nextLine

    mergeScan.rewind

    mergeScan.nextLine should be("17b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.music.com}")
    mergeScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.ebay.com}")
    mergeScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.auto.com}")
    mergeScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\t{www.travel.com}")

    an[EOFException] must be thrownBy mergeScan.nextLine
    an[EOFException] must be thrownBy mergeScan.nextLine

  }
}