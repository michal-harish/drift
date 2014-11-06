package net.imagini.aim.cluster

import net.imagini.aim.client.Loader
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.client.AimClient

class DistributedLoaderTests  extends FlatSpec with Matchers {
  val manager = new DriftManagerLocal(4)
  manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
  val node1 = new AimNode(1, "localhost:9998", manager)
  val node2 = new AimNode(2, "localhost:9997", manager)
  val node3 = new AimNode(3, "localhost:9996", manager)
  val node4 = new AimNode(4, "localhost:9995", manager)

  new Loader("localhost", 9998, Protocol.LOADER_USER, "vdna", "events", "\n", this.getClass.getResourceAsStream("datasync_big.csv"), false).streamInput should be(139)
  node1.keyspace("vdna")("events").getCount should be(39)
  node2.keyspace("vdna")("events").getCount should be(19)
  node3.keyspace("vdna")("events").getCount should be(38)
  node4.keyspace("vdna")("events").getCount should be(43)
  (39 + 19 + 38 +43) should be(139)

  val client = new AimClient("localhost", 9997)
  client.query("USE vdna")
  client.query("STATS") 
  client.printResult

  node1.shutdown
  node2.shutdown
  node3.shutdown
  node4.shutdown
  manager.close

}