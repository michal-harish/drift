package net.imagini.aim

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.partition.MergeScanner
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.utils.BlockStorageRaw
import net.imagini.aim.partition.JoinScanner

class ScanJoinTest extends FlatSpec with Matchers {
  "JoinMerge " should "should filter one partition by co-partition from another table supllying flags" in {

    //PAGEVIEWS
    val schemaA = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),url(STRING),timestamp(TIME:LONG)")
    //TODO ttl = 10
    val sA1 = new AimSegmentQuickSort(schemaA, classOf[BlockStorageLZ4])
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01")
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02")
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03")
    sA1.close
    val sA2 = new AimSegmentQuickSort(schemaA, classOf[BlockStorageLZ4])
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 13:01:03")
    sA2.close
    val partitionA1 = new AimPartition(schemaA, 1000)
    partitionA1.add(sA1)
    partitionA1.add(sA2)

    //CONVERSIONS
    val schemaB = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),conversion(STRING),url(STRING),timestamp(TIME:LONG)")
    //TODO ttl = 10
    val sB1 = new AimSegmentQuickSort(schemaB, classOf[BlockStorageLZ4])
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "check", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "buy", "www.travel.com/offers/holiday/book", "2014-10-10 13:01:03")
    sB1.close

    val partitionB1 = new AimPartition(schemaB, 1000)
    partitionB1.add(sB1)

    val schemaC = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),flag(STRING),value(BOOL)")
    //TODO ttl = -1
    val sC1 = new AimSegmentQuickSort(schemaC, classOf[BlockStorageLZ4])
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true")
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true")
    sC1.close
    val partitionC1 = new AimPartition(schemaC, 1000)
    partitionC1.add(sC1)

    val mergeScanA = new MergeScanner(partitionA1, "user_uid,url,conversion,quizzed", "url contains 'travel.com'", "*")
    println(mergeScanA.nextRowAsString)
    println(mergeScanA.nextRowAsString)
    println(mergeScanA.nextRowAsString)

    val mergeScanB = new MergeScanner(partitionB1, "user_uid,url,conversion,quizzed", "url contains 'travel.com'", "*")
    println(mergeScanB.nextRowAsString)

    val mergeScanC = new MergeScanner(partitionC1, "user_uid,url,conversion,quizzed(group value where flag='quizzed')", "*", "flag='quizzed' and value=true")
    println(mergeScanC.nextRowAsString)

    val joinScan = new JoinScanner(
      (partitionA1, "user_uid,url filter url contains 'travel.com'"),
      (partitionB1, "user_uid,url,conversion filter url contains 'travel.com'"),
      (partitionC1, "user_uid,flag,value group flag='quizzed' and value=true"))

    /**
     * Expected result contains only first user, combined schema from pageviews and conversions 
     * and table C is used only for gorup filter but not for select result.
     * 
     * user_uid(UUID:BYTEARRAY[16])         | url(STRING)                           | conversion(STRING)    |
     * =====================================+=======================================+=======================+
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers                 | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers/holiday         | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers                 | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers/holiday/book    | buy                   |
     * =====================================+=======================================+=======================|
     */
  }
}