package net.imagini.drift

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.drift.types.AimSchema
import net.imagini.drift.utils.BlockStorageMEMLZ4
import net.imagini.drift.region.AimRegion
import net.imagini.drift.region.IntersectionJoinScanner
import net.imagini.drift.segment.MergeScanner
import net.imagini.drift.region.UnionJoinScanner
import java.io.EOFException
import net.imagini.drift.types.Aim
import net.imagini.drift.types.AimTableDescriptor
import net.imagini.drift.types.SortType

class UnionVsIntersectionScannerTest extends FlatSpec with Matchers {
  "Union and Intersection Join " should "return different sets" in {
    //PAGEVIEWS
    val pageviewsDescriptor = new AimTableDescriptor(
      AimSchema.fromString("user_uid(UUID),url(STRING),timestamp(TIME)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionA1 = new AimRegion("vdna.pageviews", pageviewsDescriptor)
    regionA1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03"))

    regionA1.addTestRecords(
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 13:01:03"))

    regionA1.compact

    //CONVERSIONS
    val conversionsDescriptor = new AimTableDescriptor(
      AimSchema.fromString("user_uid(UUID),conversion(STRING),url(STRING),timestamp(TIME)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionB1 = new AimRegion("vdna.conversions", conversionsDescriptor)
    regionB1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "check", "www.bank.com/myaccunt", "2014-10-10 13:59:01"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "buy", "www.travel.com/offers/holiday/book", "2014-10-10 13:01:03"))
    regionB1.compact

    val unionJoin = new UnionJoinScanner(
      new MergeScanner("user_uid, url, timestamp", "*", regionA1.segments),
      new MergeScanner("user_uid, url, timestamp, conversion", "*", regionB1.segments))
    unionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.auto.com/mycar\t2014-10-10 11:59:01\t" + Aim.EMPTY)
    unionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 12:01:02\t" + Aim.EMPTY)
    unionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday\t2014-10-10 12:01:03\t" + Aim.EMPTY)
    unionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday/book\t2014-10-10 13:01:03\tbuy")
    unionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com/myaccunt\t2014-10-10 13:59:01\tcheck")
    unionJoin.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com/myaccunt\t2014-10-10 13:59:01\t" + Aim.EMPTY)
    unionJoin.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 13:01:03\t" + Aim.EMPTY)
    unionJoin.next should be(false)
    an[EOFException] must be thrownBy unionJoin.nextLine
    an[EOFException] must be thrownBy unionJoin.nextLine

    val intersectionJoin = new IntersectionJoinScanner(
      new MergeScanner("user_uid, url, timestamp", "*", regionA1.segments),
      new MergeScanner("user_uid, url, timestamp, conversion", "*", regionB1.segments))
    intersectionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.auto.com/mycar\t2014-10-10 11:59:01\t" + Aim.EMPTY)
    intersectionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 12:01:02\t" + Aim.EMPTY)
    intersectionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday\t2014-10-10 12:01:03\t" + Aim.EMPTY)
    intersectionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday/book\t2014-10-10 13:01:03\tbuy")
    intersectionJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com/myaccunt\t2014-10-10 13:59:01\tcheck")
    intersectionJoin.next should be(false)
    an[EOFException] must be thrownBy intersectionJoin.nextLine
    an[EOFException] must be thrownBy intersectionJoin.nextLine
  }

}