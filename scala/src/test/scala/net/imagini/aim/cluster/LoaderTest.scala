package net.imagini.aim.cluster

import net.imagini.aim.client.Loader
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class LoaderTest extends FlatSpec with Matchers {
  "Loader" should " load csv and gz" in {
    val manager = new DriftManagerLocal(1)
    manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
    val node = new AimNode(1, "localhost:9998", manager)

    new Loader("localhost", 9998, Protocol.LOADER_USER, "vdna", "events", "\n", this.getClass.getResourceAsStream("datasync.csv"), false).streamInput should be(3)
    node.regions("vdna.events").getCount should be(3L)

    new Loader("localhost", 9998, Protocol.LOADER_USER, "vdna", "events", "\n", this.getClass.getResourceAsStream("datasync.csv.gz"), true).streamInput should be(3)
    node.regions("vdna.events").getCount should be(6L)

    node.shutdown
  }
}