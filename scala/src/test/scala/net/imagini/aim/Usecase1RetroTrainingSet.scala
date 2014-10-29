package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.segment.GroupScanner
import net.imagini.aim.partition.EquiJoinScanner
import net.imagini.aim.partition.InnerJoinScanner
import net.imagini.aim.partition.OuterJoinScanner
import net.imagini.aim.types.Aim
import java.io.EOFException

class Usecase1RetroTrainingSet extends FlatSpec with Matchers {

  "Usecase1-retroactive measured set " should "should filter one partition by co-partition from another table supllying flags" in {

    //PAGEVIEWS
    val schemaA = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),url(STRING),timestamp(TIME:LONG)")
    //TODO ttl = 10
    val sA1 = new AimSegmentQuickSort(schemaA, classOf[BlockStorageLZ4])
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01")
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02")
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03")
    sA1.appendRecord("12322cfb-a29e-42c3-a3d9-12d32850e103", "www.xyz.com", "2014-10-10 12:01:02")
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
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true")
    sC1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    sC1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false")
    sC1.close
    val partitionC1 = new AimPartition(schemaC, 1000)
    partitionC1.add(sC1)

    val eventsJoin = new OuterJoinScanner(
      new MergeScanner(schemaA, "user_uid,url,timestamp", "url contains 'travel.com'", partitionA1.segments)
      ,new MergeScanner(schemaB, "user_uid,url,timestamp,conversion", "*", partitionB1.segments)
    )

    val tsetJoin = new EquiJoinScanner(
      new MergeScanner(partitionC1.schema, "user_uid", "value='true' and flag='quizzed' or flag='cc'", partitionC1.segments),
      eventsJoin
    )

    /**
     * Expected result contains only first and third user, combined schema from pageviews and conversions
     * and table C is used only for filteing users who have either quizzed or cc flags.
     *
     * user_uid(UUID:BYTEARRAY[16])         | url(STRING)                           | conversion(STRING)    |
     * =====================================+=======================================+=======================+
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers                 | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers/holiday         | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.bank.com                          | check                 |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers                 | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers/holiday/book    | buy                   |
     * a7b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers                 | -                     |
     * =====================================+=======================================+=======================|
     */

    tsetJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 12:01:02\t"+Aim.EMPTY)
    tsetJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday\t2014-10-10 12:01:03\t"+Aim.EMPTY)
    tsetJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com/myaccunt\t2014-10-10 13:59:01\tcheck")
    tsetJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday/book\t2014-10-10 13:01:03\tbuy")
    tsetJoin.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 13:01:03\t"+Aim.EMPTY)
    an[EOFException] must be thrownBy tsetJoin.nextLine

  }
}