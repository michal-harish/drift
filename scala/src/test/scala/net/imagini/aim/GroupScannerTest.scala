package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.segment.MergeScanner
import java.io.EOFException
import net.imagini.aim.segment.GroupScanner

class GroupScannerTest extends FlatSpec with Matchers {
  "ScannerMerge with GroupFilter" should "return all records for filtered group" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),column(STRING),value(STRING)")
    val s1 = new AimSegmentQuickSort(schema, classOf[BlockStorageLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}")
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}")
    s1.close
    val s2 = new AimSegmentQuickSort(schema, classOf[BlockStorageLZ4])
    s2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}")
    s2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876")
    s2.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}")
    s2.close
    val partition = new AimPartition(schema, 1000)
    partition.add(s1)
    partition.add(s2)

    val groupScan = new GroupScanner(partition.schema, "value, user_uid", "column='pageview'", "column='addthis_id'", partition.segments)
    groupScan.nextLine should be("{www.ebay.com}\t37b22cfb-a29e-42c3-a3d9-12d32850e103")
    groupScan.nextLine should be("{www.auto.com}\t37b22cfb-a29e-42c3-a3d9-12d32850e103")
    groupScan.nextLine should be("{www.travel.com}\ta7b22cfb-a29e-42c3-a3d9-12d32850e103")
    an[EOFException] must be thrownBy groupScan.nextLine
    an[EOFException] must be thrownBy groupScan.nextLine

    val gropuScan2 = new GroupScanner(partition.schema, "*", "column='pageview'", "column='addthis_id' and value='AT9876'", partition.segments)
    gropuScan2.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\tpageview\t{www.travel.com}")
    an[EOFException] must be thrownBy gropuScan2.nextLine
    an[EOFException] must be thrownBy groupScan.nextLine

  }
}