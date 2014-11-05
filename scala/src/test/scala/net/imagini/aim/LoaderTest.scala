package net.imagini.aim

import net.imagini.aim.cluster.AimNode
import net.imagini.aim.cluster.DriftManagerLocal
import net.imagini.aim.client.Loader
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class LoaderTest extends FlatSpec with Matchers {
  val manager = new DriftManagerLocal
  manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)", true)
  val node = new AimNode(1, "localhost:9998", manager)

  new Loader("localhost", 9998, "vdna", "events", "\n", this.getClass.getResourceAsStream("datasync.csv.gz"), true).streamInput should be (3)
  node.keyspace("vdna")("events").getCount should be(3L)

  new Loader("localhost", 9998, "vdna", "events", "\n", this.getClass.getResourceAsStream("datasync.csv"), false).streamInput should be (3)
  node.keyspace("vdna")("events").getCount should be(6L)

  node.shutdown

}