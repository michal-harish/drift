package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.partition.InnerJoinScanner
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.partition.OuterJoinScanner
import java.io.EOFException
import net.imagini.aim.types.Aim

class InnerOuterJoinScannerTest extends FlatSpec with Matchers {
    "Inner vs Outer Join " should "return different sets" in {
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

      val outerJoin = new OuterJoinScanner(
          new MergeScanner(partitionA1.schema, "user_uid, url, timestamp", "*", partitionA1.segments), 
          new MergeScanner(partitionB1.schema, "user_uid, url, timestamp, conversion", "*", partitionB1.segments)
      )
      outerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.auto.com/mycar\t2014-10-10 11:59:01\t" + Aim.EMPTY)
      outerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 12:01:02\t" + Aim.EMPTY)
      outerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday\t2014-10-10 12:01:03\t" + Aim.EMPTY)
      outerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com/myaccunt\t2014-10-10 13:59:01\tcheck")
      outerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday/book\t2014-10-10 13:01:03\tbuy")
      outerJoin.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com/myaccunt\t2014-10-10 13:59:01\t" + Aim.EMPTY)
      outerJoin.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 13:01:03\t" + Aim.EMPTY)
      an[EOFException] must be thrownBy outerJoin.nextLine

      val innerJoin = new InnerJoinScanner(
          new MergeScanner(partitionA1.schema, "user_uid, url, timestamp", "*", partitionA1.segments), 
          new MergeScanner(partitionB1.schema, "user_uid, url, timestamp, conversion", "*", partitionB1.segments)
      )

      innerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.auto.com/mycar\t2014-10-10 11:59:01\t" + Aim.EMPTY)
      innerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 12:01:02\t" + Aim.EMPTY)
      innerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday\t2014-10-10 12:01:03\t" + Aim.EMPTY)
      innerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com/myaccunt\t2014-10-10 13:59:01\tcheck")
      innerJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday/book\t2014-10-10 13:01:03\tbuy")
      an[EOFException] must be thrownBy innerJoin.nextLine
    }


}