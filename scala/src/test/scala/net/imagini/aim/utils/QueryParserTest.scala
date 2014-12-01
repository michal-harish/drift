package net.imagini.aim.utils

import org.scalatest.Matchers
import net.imagini.aim.region.QueryParser
import net.imagini.aim.region.AimRegion
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.region.QueryParser
import net.imagini.aim.region.PSelect
import net.imagini.aim.region.PTable
import net.imagini.aim.region.PWildcard
import net.imagini.aim.region.PEquiJoin
import net.imagini.aim.region.PVar
import net.imagini.aim.region.PUnionJoin
import net.imagini.aim.types.AimTableDescriptor
import net.imagini.aim.types.SortType

class QueryParserTest extends FlatSpec with Matchers {

  val regions = Map[String, AimRegion](
    "vdna.pageviews" -> pageviews,
    "vdna.conversions" -> conversions,
    "vdna.flags" -> flags)
  val parser = new QueryParser(regions)

  parser.frame("select * from vdna.pageviews where url contains 'travel'") should be(
    PSelect(PTable("vdna", "pageviews"), "url contains 'travel'", PWildcard(PTable("vdna", "pageviews"))))

  parser.frame("SELECT user_uid,url,timestamp FROM vdna.pageviews WHERE timestamp > '2014-10-09 13:59:01' UNION SELECT * FROM vdna.conversions") should be(
    PUnionJoin(
      PSelect(PTable("vdna", "pageviews"), "timestamp > '2014-10-09 13:59:01'", PVar("user_uid"), PVar("url"), PVar("timestamp")),
      PSelect(PTable("vdna", "conversions"), "*", PWildcard(PTable("vdna", "conversions"))),
      PVar("user_uid"), PVar("url"), PVar("timestamp"), PWildcard(PTable("vdna", "conversions"))))

  parser.frame("select user_uid from vdna.flags where value='true' and flag='quizzed' or flag='cc' JOIN SELECT * FROM vdna.conversions") should be(
    PEquiJoin(
      PSelect(PTable("vdna", "flags"), "value = 'true' and flag = 'quizzed' or flag = 'cc'", PVar("user_uid")),
      PSelect(PTable("vdna", "conversions"), "*", PWildcard(PTable("vdna", "conversions"))),
      PVar("user_uid"), PWildcard(PTable("vdna", "conversions"))))

  parser.frame("select user_uid from vdna.flags where value='true' and flag='quizzed' or flag='cc' "
    + "JOIN (SELECT user_uid,url,timestamp FROM vdna.pageviews WHERE timestamp > '2014-10-09 13:59:01' UNION SELECT * FROM vdna.conversions)") should be(
    PEquiJoin(
      PSelect(PTable("vdna", "flags"), "value = 'true' and flag = 'quizzed' or flag = 'cc'", PVar("user_uid")),
      PUnionJoin(
        PSelect(PTable("vdna", "pageviews"), "timestamp > '2014-10-09 13:59:01'", PVar("user_uid"), PVar("url"), PVar("timestamp")),
        PSelect(PTable("vdna", "conversions"), "*", PWildcard(PTable("vdna", "conversions"))),
        PVar("user_uid"), PVar("url"), PVar("timestamp"), PWildcard(PTable("vdna", "conversions"))),
      PVar("user_uid"), PVar("user_uid"), PVar("url"), PVar("timestamp"), PWildcard(PTable("vdna", "conversions"))))

  private def pageviews: AimRegion = {
    val pageviewsDescriptor = new AimTableDescriptor(
      AimSchema.fromString("user_uid(UUID),url(STRING),timestamp(TIME)"),
      10000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionPageviews = new AimRegion("vdna.pageviews", pageviewsDescriptor)
    //segment 1
    regionPageviews.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03"),
      Seq("12322cfb-a29e-42c3-a3d9-12d32850e103", "www.xyz.com", "2014-10-10 12:01:02"))
    //segment 2
    regionPageviews.addTestRecords(
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 13:01:03"))

    regionPageviews.compact
    regionPageviews

  }

  private def conversions: AimRegion = {
    //CONVERSIONS //TODO ttl = 10
    val conversionsDescriptor = new AimTableDescriptor(
      AimSchema.fromString("user_uid(UUID),conversion(STRING),url(STRING),timestamp(TIME)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionConversions1 = new AimRegion("vdna.conversions", conversionsDescriptor)
    regionConversions1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "check", "www.bank.com/myaccunt", "2014-10-10 13:59:01"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "buy", "www.travel.com/offers/holiday/book", "2014-10-10 13:01:03"))

    regionConversions1.compact
    regionConversions1
  }

  private def flags: AimRegion = {
    //USERFLAGS //TODO ttl = -1
    val userFlagsDescriptor = new AimTableDescriptor(
      AimSchema.fromString("user_uid(UUID),flag(STRING),value(BOOL)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionUserFlags1 = new AimRegion("vdna.flags", userFlagsDescriptor)
    //segment 1
    regionUserFlags1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true"))
    //segment 2
    regionUserFlags1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false"))
    regionUserFlags1.compact
    regionUserFlags1
  }

}