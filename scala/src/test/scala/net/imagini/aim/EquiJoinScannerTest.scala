package net.imagini.aim

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.region.AimRegion
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.region.EquiJoinScanner
import java.io.EOFException

class EquiJoinScannerTest extends FlatSpec with Matchers {
  "Usecase2-keyspace import" should "should filter one region by co-partition from another table supllying flags" in {
    //Keyspace:at, table: vdna_syncs
    val AS = AimSchema.fromString("at_id(STRING), user_uid(UUID:BYTEARRAY[16])")
    val AS1 = new AimRegion(AS, 1000)
    AS1.add(new AimSegmentQuickSort(AS, classOf[BlockStorageMEMLZ4])
      .appendRecord("AT1234", "37b22cfb-a29e-42c3-a3d9-12d32850e103")
      .appendRecord("AT5656", "a7b22cfb-a29e-42c3-a3d9-12d32850e234")
      .appendRecord("AT7888", "89777987-a29e-42c3-a3d9-12d32850e234")
    )

    //Keyspace:at, table: pageviews
    val AP = AimSchema.fromString("at_id(STRING), url(STRING), timestamp(TIME:LONG)")
    val AP1 = new AimRegion(AP, 1000)
    AP1.add(new AimSegmentQuickSort(AP, classOf[BlockStorageMEMLZ4])
      .appendRecord("AT1234", "www.tv.com", "2014-10-10 13:59:01")
      .appendRecord("AT5656", "www.auto.com", "2014-10-10 14:00:01")
      .appendRecord("AT1234", "www.auto.com/offers", "2014-10-10 15:00:01")
      .appendRecord("AT1234", "www.travel.com", "2014-10-10 16:00:01")
      .appendRecord("AT5656", "www.marvel.com", "2014-10-10 17:00:01")
      .appendRecord("AT1234", "www.bank.com", "2014-10-10 18:00:01")
      )

    val joinScan = new EquiJoinScanner(
        new MergeScanner("user_uid", "*", AS1.segments), 
        new MergeScanner("url, timestamp", "timestamp > '2014-10-10 16:00:00' ", AP1.segments) //at_id, 
    )
    joinScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com\t2014-10-10 16:00:01")
    joinScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com\t2014-10-10 18:00:01")
    joinScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.marvel.com\t2014-10-10 17:00:01")
    joinScan.next should equal(false)
    an[EOFException] must be thrownBy joinScan.nextLine
    an[EOFException] must be thrownBy joinScan.nextLine

    val joinScan2 = new EquiJoinScanner(
        new MergeScanner("user_uid", "*", AS1.segments),  
        new MergeScanner("url, timestamp", "*", AP1.segments)
    )
    joinScan2.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.tv.com\t2014-10-10 13:59:01")
    joinScan2.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.auto.com/offers\t2014-10-10 15:00:01")
    joinScan2.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com\t2014-10-10 16:00:01")
    joinScan2.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com\t2014-10-10 18:00:01")
    joinScan2.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.auto.com\t2014-10-10 14:00:01")
    joinScan2.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.marvel.com\t2014-10-10 17:00:01")
    joinScan2.next should equal(false)
    an[EOFException] must be thrownBy joinScan2.nextLine
    an[EOFException] must be thrownBy joinScan2.nextLine
  }

}