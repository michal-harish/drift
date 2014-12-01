package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.region.AimRegion
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.region.EquiJoinScanner
import net.imagini.aim.segment.MergeScanner
import java.io.EOFException
import net.imagini.aim.types.AimTableDescriptor
import net.imagini.aim.types.SortType
import net.imagini.aim.segment.AimSegment
import java.util.ArrayList
import net.imagini.aim.utils.View
import scala.collection.mutable.ListBuffer

class Usecase2KeyspaceImport extends FlatSpec with Matchers {
  "Usecase2-keyspace import " should "be possible to do by scan transformation" in {

    val schemaATPageviews = AimSchema.fromString("at_id(STRING), url(STRING), timestamp(TIME)")
    val schemaATSyncs = AimSchema.fromString("at_id(STRING), user_uid(UUID)")
    val schemaVDNAPageviews = AimSchema.fromString("user_uid(UUID),url(STRING),timestamp(TIME)")
    val pageviews = new AimTableDescriptor(schemaVDNAPageviews, 1000, classOf[BlockStorageMEMLZ4], SortType.QUICK_SORT)
    val syncs = new AimTableDescriptor(schemaATSyncs, 1000, classOf[BlockStorageMEMLZ4], SortType.QUICK_SORT)
    val view = new AimTableDescriptor(schemaATPageviews, 1000, classOf[BlockStorageMEMLZ4], SortType.QUICK_SORT)
    val regionVDNAPageviews1 = new AimRegion("vdna.pageviews", pageviews)
    regionVDNAPageviews1.addTestRecords(
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e234", "www.work.com", "2014-10-10 08:59:01"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e234", "www.work2.com", "2014-10-10 08:59:01"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.cafe.com", "2014-10-10 10:59:01"))
    regionVDNAPageviews1.compact

    //Keyspace AT normal load
    val AS1 = new AimRegion("addthis.syncs", syncs)
    AS1.addTestRecords(
      Seq("AT1234", "37b22cfb-a29e-42c3-a3d9-12d32850e103"),
      Seq("AT5656", "a7b22cfb-a29e-42c3-a3d9-12d32850e234"),
      Seq("AT7888", "89777987-a29e-42c3-a3d9-12d32850e234"))
    AS1.compact

    val AP1 = new AimRegion("addthis.views", view)
    AP1.addTestRecords(
      Seq("AT1234", "www.tv.com", "2014-10-10 13:59:01"),
      Seq("AT5656", "www.auto.com", "2014-10-10 14:00:01"),
      Seq("AT1234", "www.auto.com/offers", "2014-10-10 15:00:01"),
      Seq("AT1234", "www.travel.com", "2014-10-10 16:00:01"),
      Seq("AT5656", "www.marvel.com", "2014-10-10 17:00:01"),
      Seq("AT1234", "www.bank.com", "2014-10-10 18:00:01"))
    AP1.compact

    //Scan join transformation of AT pageviews into VDNA Pageviews 
    val joinScan = new EquiJoinScanner(
      new MergeScanner("user_uid", "*", AS1.segments),
      new MergeScanner("url, timestamp", "timestamp > '2014-10-10 16:00:00' ", AP1.segments))

    val records = new ListBuffer[Seq[String]]
    while (joinScan.next) {
      records += joinScan.schema.asStrings(joinScan.selectRow)
    }
    regionVDNAPageviews1.addTestRecords(records:_*)
    joinScan.close
    regionVDNAPageviews1.compact

    //scan VDNA Pageviews which should contain previous pageviews with the ones imported from AT
    val vdnaPageviewScan = new MergeScanner("user_uid,url,timestamp", "*", regionVDNAPageviews1.segments)
    vdnaPageviewScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.cafe.com\t2014-10-10 10:59:01")
    vdnaPageviewScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com\t2014-10-10 18:00:01")
    vdnaPageviewScan.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com\t2014-10-10 16:00:01")
    vdnaPageviewScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.work.com\t2014-10-10 08:59:01")
    vdnaPageviewScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.work2.com\t2014-10-10 08:59:01")
    vdnaPageviewScan.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e234\twww.marvel.com\t2014-10-10 17:00:01")
    an[EOFException] must be thrownBy vdnaPageviewScan.nextLine

  }
}