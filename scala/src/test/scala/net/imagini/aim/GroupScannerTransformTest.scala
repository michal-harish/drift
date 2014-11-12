package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.region.AimRegion
import net.imagini.aim.segment.MergeScanner
import java.io.EOFException
import net.imagini.aim.segment.GroupScanner

class GroupScannerTransformTest extends FlatSpec with Matchers {
  "ScannerMerge with GroupFilter" should "return all records for filtered group" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),column(STRING),value(STRING)")
    val s1 = new AimSegmentQuickSort(schema, classOf[BlockStorageMEMLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}")
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}")
    val s2 = new AimSegmentQuickSort(schema, classOf[BlockStorageMEMLZ4])
    s2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}")
    s2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876")
    s2.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}")
    val region = new AimRegion(schema, 1000)
    region.add(s1)
    region.add(s2)

    val scan = new GroupScanner("at_id(group value where column='addthis_id'),value,user_uid", "column='pageview'", "*", region.segments)
    scan.nextLine should be("AT1234\t{www.ebay.com}\t37b22cfb-a29e-42c3-a3d9-12d32850e103")
    scan.nextLine should be("AT1234\t{www.auto.com}\t37b22cfb-a29e-42c3-a3d9-12d32850e103")
    scan.nextLine should be("AT9876\t{www.travel.com}\ta7b22cfb-a29e-42c3-a3d9-12d32850e103")
    an[EOFException] must be thrownBy scan.nextLine
    an[EOFException] must be thrownBy scan.nextLine
  }
}