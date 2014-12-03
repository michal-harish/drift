package net.imagini.drift

import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.drift.region.DriftRegion
import net.imagini.drift.region.QueryParser
import net.imagini.drift.segment.MergeScanner
import net.imagini.drift.types.Drift
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.utils.BlockStorageMEMLZ4
import net.imagini.drift.types.DriftTableDescriptor
import net.imagini.drift.types.SortType

class Usecase1RetroTrainingSet extends FlatSpec with Matchers {

  "Usecase1-retroactive measured set " should "be possible to do by scanning joins" in {

    //PAGEVIEWS //TODO ttl = 10
    val pageviewsDescriptor = new DriftTableDescriptor(
      DriftSchema.fromString("user_uid(UUID),url(STRING),timestamp(TIME)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionPageviews1 = new DriftRegion("vdna.pageviews", pageviewsDescriptor)
    regionPageviews1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03"),
      Seq("12322cfb-a29e-42c3-a3d9-12d32850e103", "www.xyz.com", "2014-10-10 12:01:02"))
    regionPageviews1.addTestRecords(
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 13:01:03"))
    regionPageviews1.compact

    //CONVERSIONS //TODO ttl = 10
    val conversionsDescriptor = new DriftTableDescriptor(
      DriftSchema.fromString("user_uid(UUID),conversion(STRING),url(STRING),timestamp(TIME)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionConversions1 = new DriftRegion("vdna.conversions", conversionsDescriptor)
    regionConversions1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "check", "www.bank.com/myaccunt", "2014-10-10 13:59:01"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "buy", "www.travel.com/offers/holiday/book", "2014-10-10 13:01:03"))
    regionConversions1.compact

    //USERFLAGS //TODO ttl = -1
    val userFlagsDescriptor = new DriftTableDescriptor(
      DriftSchema.fromString("user_uid(UUID),flag(STRING),value(BOOL)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionUserFlags1 = new DriftRegion("vdna.flags", userFlagsDescriptor)
    regionUserFlags1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true"))
    regionUserFlags1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false"))
    regionUserFlags1.compact

    //PARTION
    val regions = Map[String, DriftRegion](
      "vdna.pageviews" -> regionPageviews1,
      "vdna.conversions" -> regionConversions1,
      "vdna.flags" -> regionUserFlags1)
    val parser = new QueryParser(regions)
    val tsetJoin = parser.parse("select user_uid from vdna.flags where value='true' and flag='quizzed' or flag='cc' join " +
      "(select user_uid,url,timestamp from vdna.pageviews where url contains 'travel.com' union "
      + "select user_uid,url,timestamp,conversion from vdna.conversions)");

    /**
     * Expected result contains only first and third user, combined schema from pageviews and conversions
     * and table C is used only for filteing users who have either quizzed or cc flags.
     *
     * user_uid(UUID)                       | url(STRING)                           | conversion(STRING)    |
     * =====================================+=======================================+=======================+
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers                 | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers/holiday         | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.bank.com                          | check                 |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers                 | -                     |
     * 37b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers/holiday/book    | buy                   |
     * a7b22cfb-a29e-42c3-a3d9-12d32850e103 | www.travel.com/offers                 | -                     |
     * =====================================+=======================================+=======================|
     */

    tsetJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 12:01:02\t" + Drift.EMPTY)
    tsetJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday\t2014-10-10 12:01:03\t" + Drift.EMPTY)
    tsetJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers/holiday/book\t2014-10-10 13:01:03\tbuy")
    tsetJoin.nextLine should be("37b22cfb-a29e-42c3-a3d9-12d32850e103\twww.bank.com/myaccunt\t2014-10-10 13:59:01\tcheck")
    tsetJoin.nextLine should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103\twww.travel.com/offers\t2014-10-10 13:01:03\t" + Drift.EMPTY)
    an[EOFException] must be thrownBy tsetJoin.nextLine

  }
}