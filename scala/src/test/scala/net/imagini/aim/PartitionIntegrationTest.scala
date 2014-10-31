package net.imagini.aim

import net.imagini.aim.client.AimResult
import net.imagini.aim.client.AimClient
import org.scalatest.Matchers
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.cluster.AimPartitionServer
import net.imagini.aim.types.AimSchema
import org.scalatest.FlatSpec
import net.imagini.aim.types.SortOrder
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.cluster.Loader

class PartitionIntegrationTest extends FlatSpec with Matchers {
  val host = "localhost"
  val port = 9999
  val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")

  def fixutreServer: AimPartitionServer = {
    val partition = new AimPartition(schema, 8192)
    val server = new AimPartitionServer(partition, port)
    server
  }
  def fixutreLoadDataSyncs = {
    val loader = new Loader(host, port, schema, "\n", this.getClass.getResourceAsStream("datasync.csv"), false)
    loader.processInput should equal(3)
  }
  def fixutreLoadPageviews = {
    val loader = new Loader(host, port, schema, "\n", this.getClass.getResourceAsStream("pageviews.csv"), false)
    loader.processInput should equal(5)
  }

  def select(filter: String): AimResult = {
    val client = new AimClient(host, port)
    val response = client.select(filter)
    response shouldBe a[Some[AimResult]]
    response.get
  }
  def fetchAll(result: AimResult): Array[String] = {
    var records = Array[String]()
    while (result.hasNext) {
      records :+= result.fetchRecordLine
    }
    result.close
    records
  }

  "Multiple loaders" should "be merged and sorted" in {
    val server = fixutreServer
    fixutreLoadDataSyncs
    fixutreLoadPageviews
    val result = select("*")

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

    fetchAll(result).foreach(println(_))
    result.count should equal(8)

    server.close
  }

  "Partition with 1 segment" should "should return all records after selecting loaded test data" in {

    val server = fixutreServer
    fixutreLoadDataSyncs

    val result = select("*")
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("d1d284b7-b04e-442a-b52a-ea74bc6466c5", "1413143748080", "ydx_vdna_id", "e9dd0b85-e3e8-f0c4-c42a-fdb0bf6cd28c"))
    result.hasNext should be(false)
    result.count should be(3L)
    result.filteredCount should be(3L)
    result.close
    server.close

  }

  "Filter over muitlple loaders" should "return subset from all sources" in {
    val server = fixutreServer
    fixutreLoadDataSyncs
    fixutreLoadPageviews
    val result = select("filter user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'")
    val records = fetchAll(result)
    records.size should equal(4)
    records.foreach(println(_))

    val result2 = select("filter column contains 'x'")
    val records2 = fetchAll(result2)
    records2.size should equal(2)
    records2.foreach(println(_))

    server.close
  }
}