package net.imagini.aim.cluster

import net.imagini.aim.client.DriftLoader
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.client.DriftClient
import java.io.EOFException

class DistributedLoaderTests extends FlatSpec with Matchers {
  "distributed loader" should "result in a correct client output" in {
      val manager = new DriftManagerLocal(4)

      manager.createTable("vdna", "events", "user_uid(UUID),timestamp(LONG),column(STRING),value(STRING)")
      manager.createTable("addthis", "syncs", "at_id(STRING),user_uid(UUID),timestamp(LONG)")
      val node1 = new AimNode(1, "localhost:9998", manager)
      val node2 = new AimNode(2, "localhost:9997", manager)
      val node3 = new AimNode(3, "localhost:9996", manager)
      val node4 = new AimNode(4, "localhost:9995", manager)

      new DriftLoader(manager, "vdna", "events", '\t', this.getClass.getResourceAsStream("datasync_big.csv.gz"), true).streamInput should be(139)
      node1.query("count vdna.events").count should be(39)
      node2.query("count vdna.events").count should be(19)
      node3.query("count vdna.events").count should be(38)
      node4.query("count vdna.events").count should be(43)
      (39 + 19 + 38 + 43) should be(139)

      val client = new DriftClient("localhost", 9997)
      client.query("STATS vdna")
      client.hasNext should be(true); client.fetchRecordLine should be("events,1,1,1918") //,2322
      client.hasNext should be(true); client.fetchRecordLine should be("events,3,1,1586") //,2173
      client.hasNext should be(true); client.fetchRecordLine should be("events,4,1,2081") //,2604
      client.hasNext should be(true); client.fetchRecordLine should be("events,2,1,905") //,1050
      client.hasNext should be(false);
      an[EOFException] must be thrownBy (client.fetchRecordLine)

      new DriftLoader(manager, "addthis", "syncs", ' ', this.getClass.getResourceAsStream("datasync_string.csv"), false).streamInput should be(10)
      client.query("STATS addthis")
      client.hasNext should be(true); client.fetchRecordLine should be("syncs,1,1,93") //,88
      client.hasNext should be(true); client.fetchRecordLine should be("syncs,3,1,165") //,176
      client.hasNext should be(true); client.fetchRecordLine should be("syncs,4,1,123") //,132
      client.hasNext should be(true); client.fetchRecordLine should be("syncs,2,1,49") //,44
      client.hasNext should be(false);
      an[EOFException] must be thrownBy (client.fetchRecordLine)

      node1.manager.down
      manager.close
  }
}