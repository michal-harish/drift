package net.imagini.aim

import scala.Array.canBuildFrom
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.client.AimClient
import net.imagini.aim.client.AimResult
import net.imagini.aim.cluster.AimNode
import net.imagini.aim.cluster.DriftManagerLocal
import net.imagini.aim.cluster.Loader
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.types.AimSchema
import java.io.EOFException
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.partition.QueryParser

class NodeIntegrationTest extends FlatSpec with Matchers {
  val host = "localhost"
  val port = 9999

  def fixutreNode: AimNode = {
    val manager = new DriftManagerLocal
    manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)", true)
    val node = new AimNode(1, host + ":" + port, manager)
    node
  }
  def fixutreLoadDataSyncs = {
    val loader = new Loader(host, port, "vdna", "events", "\n", this.getClass.getResourceAsStream("datasync.csv"), false)
    loader.processInput should equal(3)
  }
  def fixutreLoadPageviews = {
    val loader = new Loader(host, port, "vdna", "events", "\n", this.getClass.getResourceAsStream("pageviews.csv"), false)
    loader.processInput should equal(5)
  }

  def newClient: AimClient = {
    val client = new AimClient(host, port)
    client.query("use vdna")
    client
  }
  def fetchAll(result: AimResult): Array[String] = {
    var records = Array[String]()
    while (result.hasNext) {
      records :+= result.fetchRecordLine
    }
    records
  }

  "Multiple loaders" should "be merged and sorted" in {
    val node = fixutreNode
    fixutreLoadDataSyncs
    fixutreLoadPageviews
    val client = newClient
    client.query("select * from events") match {
      case None ⇒ throw new IllegalArgumentException
      case Some(result) ⇒ {
        result.hasNext should be(true)
        result.fetchRecordStrings(0) should equal("04732d65-d530-4b18-a583-53799838731a")
        result.hasNext should be(true)
        result.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
        result.hasNext should be(true)
        result.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
        result.hasNext should be(true)
        result.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
        result.hasNext should be(true)
        result.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
        result.hasNext should be(true)
        result.fetchRecordStrings(0) should equal("69a82e00-3f54-b96a-8fe0-8d2a51f80a86")
        result.hasNext should be(true)
        result.fetchRecordStrings(0) should equal("883f5b55-a5bf-480e-9983-8bb117437eac")
        result.hasNext should be(true)
        result.fetchRecordStrings(0) should equal("d1d284b7-b04e-442a-b52a-ea74bc6466c5")
        result.hasNext should be(false)
        result.count should equal(8)
      }
    }
    node.shutdown
  }

  "Partition with 1 segment" should "should return all records after selecting loaded test data" in {

    val node = fixutreNode
    fixutreLoadDataSyncs

    val client = newClient
    val result = client.query("select * from events") match {
      case None ⇒ throw new IllegalArgumentException
      case Some(result) ⇒ {
        result.hasNext should be(true)
        result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
        result.hasNext should be(true)
        result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
        result.hasNext should be(true)
        result.fetchRecordStrings should be(Array("d1d284b7-b04e-442a-b52a-ea74bc6466c5", "1413143748080", "ydx_vdna_id", "e9dd0b85-e3e8-f0c4-c42a-fdb0bf6cd28c"))
        result.hasNext should be(false)
        result.count should be(3L)
        node.shutdown
      }
    }
  }

  "Filter over muitlple loaders" should "return subset from all sources" in {
    val node = fixutreNode
    fixutreLoadDataSyncs
    fixutreLoadPageviews
    val client = newClient
    client.query("select * from events where user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'") match {
      case None ⇒ throw new IllegalArgumentException
      case Some(result) ⇒ {
        result.hasNext should be(true)
        result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413061544595", "VDNAUserPageview", "http://zh.pad.wikia.com/wiki/Puzzle_%26_Dragons_%E4%B8%AD%E6%96%87WIKI"))
        result.hasNext should be(true)
        result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413061544605", "VDNAUserPageview", "http://www.lincolnshireecho.co.uk/news"))
        result.hasNext should be(true)
        result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
        result.hasNext should be(true)
        result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
        result.hasNext should be(false)
        result.count should be(4L)
        an[EOFException] must be thrownBy (result.fetchRecordStrings)
      }
    }

    client.query("select * from events where column contains 'x'") match {
      case None ⇒ throw new IllegalArgumentException
      case Some(result) ⇒ {
        println(result.fetchRecordLine)
        //val records2 = fetchAll(result2)
        //records2.size should equal(2)
      }
    }

    client.close
    node.shutdown
  }

  "Partition " should "understand complete query" in {
    val partition = pageviewsPartition
    val regions = Map[String, AimPartition](
      "pageviews" -> partition)
    val parser = new QueryParser(regions)
  }

  private def pageviewsPartition: AimPartition = {
    val schemaPageviews = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),url(STRING),timestamp(TIME:LONG)")
    val sA1 = new AimSegmentQuickSort(schemaPageviews, classOf[BlockStorageLZ4])
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01") //0  1
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02") //16 1
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03") //32 1
    sA1.appendRecord("12322cfb-a29e-42c3-a3d9-12d32850e103", "www.xyz.com", "2014-10-10 12:01:02") //48 2
    sA1.close
    val sA2 = new AimSegmentQuickSort(schemaPageviews, classOf[BlockStorageLZ4])
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 13:01:03")
    sA2.close
    val partitionPageviews = new AimPartition(schemaPageviews, 10000)
    partitionPageviews.add(sA1)
    partitionPageviews.add(sA2)
    partitionPageviews

  }
}