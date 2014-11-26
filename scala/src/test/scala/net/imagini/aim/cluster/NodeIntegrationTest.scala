package net.imagini.aim.cluster

import java.io.EOFException
import scala.Array.canBuildFrom
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.client.DriftClient
import net.imagini.aim.client.DriftLoader
import net.imagini.aim.region.AimRegion
import net.imagini.aim.region.QueryParser
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.segment.CountScanner
import net.imagini.aim.types.AimTableDescriptor

class NodeIntegrationTest extends FlatSpec with Matchers {
  val host = "localhost"
  val port = 9999
  val manager = new DriftManagerLocal(1)

  def fixutreNode: AimNode = {
    manager.createTable("vdna", "events", "user_uid(UUID),timestamp(LONG),column(STRING),value(STRING)")
    val node = new AimNode(1, host + ":" + port, manager)
    node
  }
  def fixutreLoadDataSyncs = {
    val loader = new DriftLoader(manager, "vdna", "events", '\t', this.getClass.getResourceAsStream("datasync.csv"), false)
    loader.streamInput should be(3)
  }
  def fixutreLoadPageviews = {
    val loader = new DriftLoader(manager, "vdna", "events", '\t', this.getClass.getResourceAsStream("pageviews.csv"), false)
    loader.streamInput should be(5)
  }

  def newClient: DriftClient = {
    val client = new DriftClient(host, port)
    client
  }
  def fetchAll(client: DriftClient): Array[String] = {
    var records = Array[String]()
    while (client.hasNext) {
      records :+= client.fetchRecordLine
    }
    records
  }

  private val regions = Map[String, AimRegion](
    "vdna.pageviews" -> pageviews,
    "vdna.conversions" -> conversions,
    "vdna.flags" -> flags)

  private def pageviews: AimRegion = {
    val pageviewsDescriptor = new AimTableDescriptor(
        AimSchema.fromString("user_uid(UUID),url(STRING),timestamp(TIME)"),
        10000,
        classOf[BlockStorageMEMLZ4],
        classOf[AimSegmentQuickSort])
    val regionPageviews = new AimRegion("vdna.pageviews", pageviewsDescriptor)
    val sA1 = regionPageviews.newSegment
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01") //0  1
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02") //16 1
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03") //32 1
    sA1.appendRecord("12322cfb-a29e-42c3-a3d9-12d32850e103", "www.xyz.com", "2014-10-10 12:01:02") //48 2
    val sA2 = regionPageviews.newSegment
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 13:01:03")

    regionPageviews.add(sA1)
    regionPageviews.add(sA2)
    regionPageviews.compact
    regionPageviews

  }

  private def conversions: AimRegion = {
    //CONVERSIONS //TODO ttl = 10
    val conversionsDescriptor = new AimTableDescriptor(
        AimSchema.fromString("user_uid(UUID),conversion(STRING),url(STRING),timestamp(TIME)"),
        1000,
        classOf[BlockStorageMEMLZ4],
        classOf[AimSegmentQuickSort])
    val regionConversions1 = new AimRegion("vdna.conversions", conversionsDescriptor)
    val sB1 = regionConversions1.newSegment
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "check", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "buy", "www.travel.com/offers/holiday/book", "2014-10-10 13:01:03")

    regionConversions1.add(sB1)
    regionConversions1.compact
    regionConversions1
  }

  private def flags: AimRegion = {
    //USERFLAGS //TODO ttl = -1
    val userFlagsDescriptor = new AimTableDescriptor(
        AimSchema.fromString("user_uid(UUID),flag(STRING),value(BOOL)"),
        1000,
        classOf[BlockStorageMEMLZ4],
        classOf[AimSegmentQuickSort])
    val regionUserFlags1 = new AimRegion("vdna.flags", userFlagsDescriptor)
    val sC1 = regionUserFlags1.newSegment
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true")
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    val sC2 = regionUserFlags1.newSegment
    sC2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false")
    regionUserFlags1.add(sC1)
    regionUserFlags1.add(sC2)
    regionUserFlags1.compact
    regionUserFlags1
  }

  "Multiple loaders" should "be merged and sorted" in {
    val node = fixutreNode
    fixutreLoadDataSyncs
    fixutreLoadPageviews
    val client = newClient
    if (client.query("select * from vdna.events") != None) {
      client.hasNext should be(true)
      client.fetchRecordStrings(0) should equal("04732d65-d530-4b18-a583-53799838731a")
      client.hasNext should be(true)
      client.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
      client.hasNext should be(true)
      client.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
      client.hasNext should be(true)
      client.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
      client.hasNext should be(true)
      client.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
      client.hasNext should be(true)
      client.fetchRecordStrings(0) should equal("69a82e00-3f54-b96a-8fe0-8d2a51f80a86")
      client.hasNext should be(true)
      client.fetchRecordStrings(0) should equal("883f5b55-a5bf-480e-9983-8bb117437eac")
      client.hasNext should be(true)
      client.fetchRecordStrings(0) should equal("d1d284b7-b04e-442a-b52a-ea74bc6466c5")
      client.hasNext should be(false)
      an[EOFException] must be thrownBy (client.fetchRecordStrings)
    }
    node.manager.down
  }

//  "Region with 1 segment" should "should return all records after selecting loaded test data" in {
//
//    val node = fixutreNode
//    fixutreLoadDataSyncs
//
//    val client = newClient
//    if (client.query("select * from vdna.events") != None) {
//      client.hasNext should be(true)
//      client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
//      client.hasNext should be(true)
//      client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
//      client.hasNext should be(true)
//      client.fetchRecordStrings should be(Array("d1d284b7-b04e-442a-b52a-ea74bc6466c5", "1413143748080", "ydx_vdna_id", "e9dd0b85-e3e8-f0c4-c42a-fdb0bf6cd28c"))
//      client.hasNext should be(false)
//      an[EOFException] must be thrownBy (client.fetchRecordStrings)
//      node.manager.down
//
//    }
//  }
//
//  "Filter over muitlple loaders" should "return subset from all sources" in {
//    val node = fixutreNode
//    val client = newClient
//
//    //  TODO EmptyTableScanner
//    //    client.query("SELECT * from vdna.events") match {
//    //      case None ⇒ throw new IllegalArgumentException
//    //      case Some(schema) ⇒ {
//    //        client.hasNext should be(false)
//    //        an[EOFException] must be thrownBy (client.fetchRecordStrings)
//    //      }
//    //    }
//    //    node.query("COUNT vdna.events").asInstanceOf[CountScanner].count should be(0)
//    //
//    //    client.query("COUNT vdna.events") should be(None)
//    //    client.getCount should be(0)
//
//    fixutreLoadDataSyncs
//    fixutreLoadPageviews
//
//    client.query("select * from vdna.events where user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'") match {
//      case None ⇒ throw new IllegalArgumentException
//      case Some(schema) ⇒ {
//        client.hasNext should be(true)
//        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
//        client.hasNext should be(true)
//        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
//        client.hasNext should be(true)
//        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413061544595", "VDNAUserPageview", "http://zh.pad.wikia.com/wiki/Puzzle_%26_Dragons_%E4%B8%AD%E6%96%87WIKI"))
//        client.hasNext should be(true)
//        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413061544605", "VDNAUserPageview", "http://www.lincolnshireecho.co.uk/news"))
//        client.hasNext should be(false)
//        an[EOFException] must be thrownBy (client.fetchRecordStrings)
//      }
//    }
//
//    client.query("select * from vdna.events where column contains 'x'") match {
//      case None ⇒ throw new IllegalArgumentException
//      case Some(schema) ⇒ {
//        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
//        client.fetchRecordStrings should be(Array("d1d284b7-b04e-442a-b52a-ea74bc6466c5", "1413143748080", "ydx_vdna_id", "e9dd0b85-e3e8-f0c4-c42a-fdb0bf6cd28c"))
//        an[EOFException] must be thrownBy (client.fetchRecordStrings)
//      }
//    }
//
//    client.close
//    node.manager.down
//  }
//
//  "Region " should "understand simple query" in {
//    val parser = new QueryParser(regions)
//    val scanner = parser.parse("select * from vdna.pageviews where url contains 'travel'")
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 12:01:02")
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday,2014-10-10 12:01:03")
//    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 13:01:03")
//    scanner.next should be(false);
//
//    val scanner2 = parser.parse("select * from vdna.pageviews where user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'")
//    scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.auto.com/mycar,2014-10-10 11:59:01")
//    scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 12:01:02")
//    scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday,2014-10-10 12:01:03")
//    scanner2.next should be(false);
//
//    val scanner3 = parser.parse("count vdna.pageviews where user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'")
//    scanner3.count should be(3L)
//  }
//
//  "Region " should "understand complex join query" in {
//    val parser = new QueryParser(regions)
//    val scanner = parser.parse("select user_uid from vdna.flags where value='true' and flag='quizzed' or flag='cc' "
//      + "JOIN (SELECT user_uid,url,timestamp FROM vdna.pageviews WHERE timestamp > '2014-10-10 11:59:01' UNION SELECT * FROM vdna.conversions)")
//
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103, ,www.travel.com/offers,2014-10-10 12:01:02")
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103, ,www.travel.com/offers/holiday,2014-10-10 12:01:03")
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,check,www.bank.com/myaccunt,2014-10-10 13:59:01")
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,buy,www.travel.com/offers/holiday/book,2014-10-10 13:01:03")
//    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103, ,www.bank.com/myaccunt,2014-10-10 13:59:01")
//    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103, ,www.travel.com/offers,2014-10-10 13:01:03")
//    scanner.next should be(false)
//    an[EOFException] must be thrownBy (scanner.selectLine(","))
//    val scanner3 = parser.parse("count (SELECT user_uid,url,timestamp FROM vdna.pageviews WHERE timestamp > '2014-10-10 11:59:01' UNION SELECT * FROM vdna.conversions)")
//    scanner3.count should be(7L)
//
//  }
//
//  "Region " should "understand complex join query with subquery" in {
//    val parser = new QueryParser(regions)
//
//    val scanner = parser.parse("SELECT user_uid,url,conversion FROM ( select user_uid from vdna.flags where value='true' and flag='quizzed' or flag='cc' "
//      + "JOIN ( SELECT user_uid,url,timestamp FROM vdna.pageviews UNION SELECT * FROM vdna.conversions) ) WHERE timestamp > '2014-10-10 11:59:01'")
//
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers, ")
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday, ")
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.bank.com/myaccunt,check")
//    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday/book,buy")
//    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,www.bank.com/myaccunt, ")
//    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers, ")
//    scanner.next should be(false)
//    an[EOFException] must be thrownBy (scanner.selectLine(","))
//
//  }

}