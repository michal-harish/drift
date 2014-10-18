package net.imagini.aim

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.cluster.AimServer
import net.imagini.aim.cluster.StandardLoader
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder
import net.imagini.aim.cluster.AimClient
import net.imagini.aim.cluster.AimResult

class IntegrationTest extends FlatSpec with Matchers {

  val host = "localhost"
  val port = 9999
  val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")

  def fixutreServer: AimServer = {
    val partition = new AimPartition(schema, 8192, schema.name(0), SortOrder.ASC)
    val server = new AimServer(partition, port)
    server.start
    server
  }
  def fixutreLoadDataSyncs = {
    val loader = new StandardLoader(schema, host, port, "\n", this.getClass.getResourceAsStream("datasync.csv"), false)
    loader.processInput should equal(3)
  }
    def fixutreLoadPageviews = {
    val loader = new StandardLoader(schema, host, port, "\n", this.getClass.getResourceAsStream("pageviews.csv"), false)
    loader.processInput should equal(5)
  }

  def selectAll(): AimResult = {
    val client = new AimClient(host, port)
    val response = client.select()
    response shouldBe a[Some[AimResult]]
    response.get
  }
  def select(filter:String): AimResult = {
    val client = new AimClient(host, port)
    val response = client.select(filter)
    response shouldBe a[Some[AimResult]]
    response.get
  }
  def fetchAll(result:AimResult): Array[String] = {
    var records = Array[String]()
    while(result.hasNext) {
      records :+= result.fetchRecordLine
    }
    records
  }

  "Multiple loaders" should "be merged and sorted" in {
     val server = fixutreServer
     fixutreLoadDataSyncs
     fixutreLoadPageviews
     val records = fetchAll(selectAll())
     records.size should equal(8)
     records.foreach(println(_))
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
     server.close
  }

  "Partition with 1 segment" should "should return all records after selecting loaded test data" in {

    val server = fixutreServer
    fixutreLoadDataSyncs

    val result = selectAll()
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("d1d284b7-b04e-442a-b52a-ea74bc6466c5", "1413143748080", "ydx_vdna_id", "e9dd0b85-e3e8-f0c4-c42a-fdb0bf6cd28c"))
    result.hasNext should be(false)
    result.count should be(3L)
    result.filteredCount should be(3L)

    server.close

  }

}