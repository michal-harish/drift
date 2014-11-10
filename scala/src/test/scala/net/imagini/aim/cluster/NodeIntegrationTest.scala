package net.imagini.aim.cluster

import java.io.EOFException
import scala.Array.canBuildFrom
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.client.AimClient
import net.imagini.aim.client.Loader
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.partition.QueryParser
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.tools.CountScanner

class NodeIntegrationTest extends FlatSpec with Matchers {
  val host = "localhost"
  val port = 9999

  def fixutreNode: AimNode = {
    val manager = new DriftManagerLocal(1)
    manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
    val node = new AimNode(1, host + ":" + port, manager)
    node
  }
  def fixutreLoadDataSyncs = {
    val loader = new Loader(host, port,Protocol.LOADER_USER, "vdna", "events", "\n", this.getClass.getResourceAsStream("datasync.csv"), false)
    loader.streamInput should be (3)
  }
  def fixutreLoadPageviews = {
    val loader = new Loader(host, port, Protocol.LOADER_USER, "vdna", "events", "\n", this.getClass.getResourceAsStream("pageviews.csv"), false)
    loader.streamInput should be (5)
  }

  def newClient: AimClient = {
    val client = new AimClient(host, port)
    client
  }
  def fetchAll(client: AimClient): Array[String] = {
    var records = Array[String]()
    while (client.hasNext) {
      records :+= client.fetchRecordLine
    }
    records
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
    node.shutdown
  }

  "Partition with 1 segment" should "should return all records after selecting loaded test data" in {

    val node = fixutreNode
    fixutreLoadDataSyncs

    val client = newClient
    if (client.query("select * from vdna.events") != None) {
      client.hasNext should be(true)
      client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
      client.hasNext should be(true)
      client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
      client.hasNext should be(true)
      client.fetchRecordStrings should be(Array("d1d284b7-b04e-442a-b52a-ea74bc6466c5", "1413143748080", "ydx_vdna_id", "e9dd0b85-e3e8-f0c4-c42a-fdb0bf6cd28c"))
      client.hasNext should be(false)
      an[EOFException] must be thrownBy (client.fetchRecordStrings)
      node.shutdown

    }
  }

  "Filter over muitlple loaders" should "return subset from all sources" in {
    val node = fixutreNode
    val client = newClient

//  TODO EmptyTableScanner
//    client.query("SELECT * from vdna.events") match {
//      case None ⇒ throw new IllegalArgumentException
//      case Some(schema) ⇒ {
//        client.hasNext should be(false)
//        an[EOFException] must be thrownBy (client.fetchRecordStrings)
//      }
//    }
//    node.query("COUNT vdna.events").asInstanceOf[CountScanner].count should be(0)
//
//    client.query("COUNT vdna.events") should be(None)
//    client.getCount should be(0)

    fixutreLoadDataSyncs
    fixutreLoadPageviews

    client.query("select * from vdna.events where user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'") match {
      case None ⇒ throw new IllegalArgumentException
      case Some(schema) ⇒ {
        client.hasNext should be(true)
        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413061544595", "VDNAUserPageview", "http://zh.pad.wikia.com/wiki/Puzzle_%26_Dragons_%E4%B8%AD%E6%96%87WIKI"))
        client.hasNext should be(true)
        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413061544605", "VDNAUserPageview", "http://www.lincolnshireecho.co.uk/news"))
        client.hasNext should be(true)
        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
        client.hasNext should be(true)
        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
        client.hasNext should be(false)
        an[EOFException] must be thrownBy (client.fetchRecordStrings)
      }
    }

    client.query("select * from vdna.events where column contains 'x'") match {
      case None ⇒ throw new IllegalArgumentException
      case Some(schema) ⇒ {
        client.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
        client.fetchRecordStrings should be(Array("d1d284b7-b04e-442a-b52a-ea74bc6466c5", "1413143748080", "ydx_vdna_id", "e9dd0b85-e3e8-f0c4-c42a-fdb0bf6cd28c"))
        an[EOFException] must be thrownBy (client.fetchRecordStrings)
      }
    }

    client.close
    node.shutdown
  }

  "Partition " should "understand simple query" in {
    val parser = new QueryParser(regions)
    val scanner = parser.parse("select * from vdna.pageviews where url contains 'travel'")
    scanner.next should be(true);
    scanner.next should be(true);
    scanner.next should be(true);
    scanner.next should be(false);
    scanner.rewind
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 12:01:02")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday,2014-10-10 12:01:03")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 13:01:03")
    scanner.next should be(false);

    val scanner2 = parser.parse("select * from vdna.pageviews where user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'")
    scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.auto.com/mycar,2014-10-10 11:59:01")
    scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 12:01:02")
    scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday,2014-10-10 12:01:03")
    scanner2.next should be(false);

    val scanner3 = parser.parse("count vdna.pageviews where user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'")
    scanner3.count should be(3L)
  }

  "Partition " should "understand complex join query" in {
    val parser = new QueryParser(regions)
    val scanner = parser.parse("select user_uid from vdna.flags where value='true' and flag='quizzed' or flag='cc' "
      + "JOIN (SELECT user_uid,url,timestamp FROM vdna.pageviews WHERE timestamp > '2014-10-10 11:59:01' UNION SELECT * FROM vdna.conversions)")

    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103, ,www.travel.com/offers,2014-10-10 12:01:02")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103, ,www.travel.com/offers/holiday,2014-10-10 12:01:03")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,buy,www.travel.com/offers/holiday/book,2014-10-10 13:01:03")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,check,www.bank.com/myaccunt,2014-10-10 13:59:01")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103, ,www.bank.com/myaccunt,2014-10-10 13:59:01")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103, ,www.travel.com/offers,2014-10-10 13:01:03")
    scanner.next should be(false)
    an[EOFException] must be thrownBy (scanner.selectLine(","))
    val scanner3 = parser.parse("count (SELECT user_uid,url,timestamp FROM vdna.pageviews WHERE timestamp > '2014-10-10 11:59:01' UNION SELECT * FROM vdna.conversions)")
    scanner3.count should be(7L)

  }

  "Partition " should "understand complex join query with subquery" in {
    
    val parser = new QueryParser(regions)

    val scanner = parser.parse("SELECT user_uid,url,conversion FROM ( select user_uid from vdna.flags where value='true' and flag='quizzed' or flag='cc' "
      + "JOIN ( SELECT user_uid,url,timestamp FROM vdna.pageviews UNION SELECT * FROM vdna.conversions) ) WHERE timestamp > '2014-10-10 11:59:01'")

    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers, ")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday, ")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday/book,buy")
    scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.bank.com/myaccunt,check")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,www.bank.com/myaccunt, ")
    scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers, ")
    scanner.next should be(false)
    an[EOFException] must be thrownBy (scanner.selectLine(","))

  }
  private val regions = Map[String, AimPartition](
      "vdna.pageviews" -> pageviews,
      "vdna.conversions" -> conversions,
      "vdna.flags" -> flags)

  private def pageviews: AimPartition = {
    val schemaPageviews = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),url(STRING),timestamp(TIME:LONG)")
    val sA1 = new AimSegmentQuickSort(schemaPageviews, classOf[BlockStorageLZ4])
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01") //0  1
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02") //16 1
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03") //32 1
    sA1.appendRecord("12322cfb-a29e-42c3-a3d9-12d32850e103", "www.xyz.com", "2014-10-10 12:01:02") //48 2
    val sA2 = new AimSegmentQuickSort(schemaPageviews, classOf[BlockStorageLZ4])
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 13:01:03")
    val partitionPageviews = new AimPartition(schemaPageviews, 10000)
    partitionPageviews.add(sA1)
    partitionPageviews.add(sA2)
    partitionPageviews

  }

  private def conversions: AimPartition = {
    //CONVERSIONS //TODO ttl = 10
    val schemaConversions = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),conversion(STRING),url(STRING),timestamp(TIME:LONG)")
    val sB1 = new AimSegmentQuickSort(schemaConversions, classOf[BlockStorageLZ4])
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "check", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "buy", "www.travel.com/offers/holiday/book", "2014-10-10 13:01:03")
    val partitionConversions1 = new AimPartition(schemaConversions, 1000)
    partitionConversions1.add(sB1)
    partitionConversions1
  }

  private def flags: AimPartition = {
    //USERFLAGS //TODO ttl = -1
    val schemaUserFlags = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),flag(STRING),value(BOOL)")
    val sC1 = new AimSegmentQuickSort(schemaUserFlags, classOf[BlockStorageLZ4])
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true")
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    val sC2 = new AimSegmentQuickSort(schemaUserFlags, classOf[BlockStorageLZ4])
    sC2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false")
    val partitionUserFlags1 = new AimPartition(schemaUserFlags, 1000)
    partitionUserFlags1.add(sC1)
    partitionUserFlags1.add(sC2)
    partitionUserFlags1
  }

}