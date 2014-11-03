package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.partition.EquiJoinScanner
import net.imagini.aim.segment.MergeScanner
import java.io.EOFException

class Usecase2KeyspaceImport extends FlatSpec with Matchers {
  "Usecase2-keyspace import " should "be possible to do by scan transformation" in {

    val schemaATPageviews = AimSchema.fromString("at_id(STRING), url(STRING), timestamp(TIME:LONG)")
    val schemaATSyncs = AimSchema.fromString("at_id(STRING), user_uid(UUID:BYTEARRAY[16])")
    val schemaVDNAPageviews = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),url(STRING),timestamp(TIME:LONG)")
    val partitionVDNAPageviews1 = new AimPartition(schemaVDNAPageviews, 1000)
    val existingVDNAPageviewsSegment = new AimSegmentQuickSort(schemaVDNAPageviews, classOf[BlockStorageLZ4])
    existingVDNAPageviewsSegment.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e234", "www.work.com", "2014-10-10 08:59:01")
    existingVDNAPageviewsSegment.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e234", "www.work2.com", "2014-10-10 08:59:01")
    existingVDNAPageviewsSegment.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.cafe.com", "2014-10-10 10:59:01")
    partitionVDNAPageviews1.add(existingVDNAPageviewsSegment.close)

    //Keyspace AT normal load
    val AS1 = new AimPartition(schemaATSyncs, 1000)
    AS1.add(new AimSegmentQuickSort(schemaATSyncs, classOf[BlockStorageLZ4])
      .appendRecord("AT1234", "37b22cfb-a29e-42c3-a3d9-12d32850e103")
      .appendRecord("AT5656", "a7b22cfb-a29e-42c3-a3d9-12d32850e234")
      .appendRecord("AT7888", "89777987-a29e-42c3-a3d9-12d32850e234")
      .close)

    val AP1 = new AimPartition(schemaATPageviews, 1000)
    AP1.add(new AimSegmentQuickSort(schemaATPageviews, classOf[BlockStorageLZ4])
      .appendRecord("AT1234", "www.tv.com", "2014-10-10 13:59:01")
      .appendRecord("AT5656", "www.auto.com", "2014-10-10 14:00:01")
      .appendRecord("AT1234", "www.auto.com/offers", "2014-10-10 15:00:01")
      .appendRecord("AT1234", "www.travel.com", "2014-10-10 16:00:01")
      .appendRecord("AT5656", "www.marvel.com", "2014-10-10 17:00:01")
      .appendRecord("AT1234", "www.bank.com", "2014-10-10 18:00:01")
      .close)

    //Scan join transformation of AT pageviews into VDNA Pageviews 
    val joinScan = new EquiJoinScanner(
      Array("user_uid","url","timestamp"),
      new MergeScanner(schemaATSyncs, "user_uid, at_id", "*", AS1.segments),
      new MergeScanner(schemaATPageviews, "at_id, url, timestamp", "timestamp > '2014-10-10 16:00:00' ", AP1.segments))
    val newVDNAPageviewsSegment = new AimSegmentQuickSort(schemaVDNAPageviews, classOf[BlockStorageLZ4])

    while (joinScan.next) {
      newVDNAPageviewsSegment.appendRecord(joinScan.selectRow)
    }
    partitionVDNAPageviews1.add(newVDNAPageviewsSegment.close)
    //TODO joinScan.close

    //scan VDNA Pageviews which should contain previous pageviews with the ones imported from AT
    val vdnaPageviewScan = new MergeScanner(schemaVDNAPageviews, "user_uid,url,timestamp", "*", partitionVDNAPageviews1.segments)
    vdnaPageviewScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com\t2014-10-10 16:00:01")
    vdnaPageviewScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com\t2014-10-10 18:00:01")
    vdnaPageviewScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.cafe.com\t2014-10-10 10:59:01")
    vdnaPageviewScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.marvel.com\t2014-10-10 17:00:01")
    vdnaPageviewScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.work.com\t2014-10-10 8:59:01")
    vdnaPageviewScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.work2.com\t2014-10-10 8:59:01")
    an[EOFException] must be thrownBy vdnaPageviewScan.nextLine

  }
}