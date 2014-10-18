package net.imagini.aim

import net.imagini.aim.cluster.AimResult
import net.imagini.aim.cluster.AimClient
import org.scalatest.Matchers
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.partition.AimPartitionServer
import net.imagini.aim.types.AimSchema
import org.scalatest.FlatSpec
import net.imagini.aim.types.SortOrder
import net.imagini.aim.partition.AimPartitionLoader

class PartitionIntegrationTest extends FlatSpec with Matchers {
  val host = "localhost"
  val port = 9999
  val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")

  def fixutreServer: AimPartitionServer = {
    val partition = new AimPartition(schema, 8192, schema.name(0), SortOrder.ASC)
    val server = new AimPartitionServer(partition, port)
    server.start
    server
  }
  def fixutreLoadDataSyncs = {
    val loader = new AimPartitionLoader(host, port, schema, "\n", this.getClass.getResourceAsStream("datasync.csv"), false)
    loader.processInput should equal(3)
  }
    def fixutreLoadPageviews = {
    val loader = new AimPartitionLoader(host, port, schema, "\n", this.getClass.getResourceAsStream("pageviews.csv"), false)
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
    result.close
    records
  }

}