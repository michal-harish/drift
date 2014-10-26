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
    val schemaA = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),url(STRING),timestamp(TIME:LONG)")
    //TODO ttl = 10
    val sA1 = new AimSegmentQuickSort(schemaA, classOf[BlockStorageLZ4])
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01")
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02")
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03")
    sA1.close
    val sA2 = new AimSegmentQuickSort(schemaA, classOf[BlockStorageLZ4])
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday/book", "2014-10-10 13:01:03")
    sA2.close
    val partitionA1 = new AimPartition(schemaA, 1000)
    partitionA1.add(sA1)
    partitionA1.add(sA2)

    val schemaB = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),flag(STRING),value(BOOL)")
    //TODO ttl = -1
    val sB1 = new AimSegmentQuickSort(schemaB, classOf[BlockStorageLZ4])
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true")
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true")
    sB1.close
    val partitionB1 = new AimPartition(schemaB, 1000)
    partitionB1.add(sB1)

    val mergeScanA = new MergeScanner(partitionA1, "url contains 'travel.com'")
    println(mergeScanA.nextRowAsString)
    println(mergeScanA.nextRowAsString)

    val mergeScanB = new MergeScanner(partitionB1, "flag='quizzed' and value=true")
    println(mergeScanB.nextRowAsString)

    //A(select user_uid,url,timestamp from A filter url contains 'travel.com') join B(quizzed=true)
    val joinScan = new JoinScanner(
      (partitionA1, "url contains 'travel.com'"),
      (partitionB1, "flag='quizzed' and value=true"))
  }
}