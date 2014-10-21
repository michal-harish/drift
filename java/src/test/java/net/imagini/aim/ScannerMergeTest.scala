package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.tools.AimFilter
import net.imagini.aim.partition.MergeScanner

class ScannerMergeTest extends FlatSpec with Matchers {
  "ScannerMerge " should "produce same result as stream merge" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),column(STRING),value(STRING)")
    val s1 = new AimSegmentQuickSort(schema, classOf[BlockStorageLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}")
    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}")
    s1.close

    val s2 = new AimSegmentQuickSort(schema, classOf[BlockStorageLZ4])
    s2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}")
    s2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876")
    s2.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}")
    s2.close

    val subschema = schema.subset(Array("user_uid", "value"))
//    val filter = AimFilter.fromString(schema, "column='pageview'")
    val mergeScan = new MergeScanner(subschema, /*filter,*/ Array(s1,s2))

//    readRecord(subschema, mergeSort) should be("17b22cfb-a29e-42c3-a3d9-12d32850e103 {www.music.com} ")
//    readRecord(subschema, mergeSort) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 {www.auto.com} ")
//    readRecord(subschema, mergeSort) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 {www.ebay.com} ")
//    readRecord(subschema, mergeSort) should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103 {www.travel.com} ")

//    an[EOFException] must be thrownBy readRecord(subschema, mergeSort)

//    mergeScan.close

  }
}