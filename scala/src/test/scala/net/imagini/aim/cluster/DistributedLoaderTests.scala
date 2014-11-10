package net.imagini.aim.cluster

import net.imagini.aim.client.Loader
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.client.AimClient
import java.io.EOFException

class DistributedLoaderTests  extends FlatSpec with Matchers {
  val manager = new DriftManagerLocal(4)

  manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
  manager.createTable("addthis", "syncs", "at_id(STRING),user_uid(UUID:BYTEARRAY[16]),timestamp(LONG)")
  val node1 = new AimNode(1, "localhost:9998", manager)
  val node2 = new AimNode(2, "localhost:9997", manager)
  val node3 = new AimNode(3, "localhost:9996", manager)
  val node4 = new AimNode(4, "localhost:9995", manager)

  new Loader("localhost", 9998, Protocol.LOADER_USER, "vdna", "events", "\n", this.getClass.getResourceAsStream("datasync_big.csv.gz"), true).streamInput should be(139)
  node1.regions("vdna.events").getCount should be(39)
  node2.regions("vdna.events").getCount should be(19)
  node3.regions("vdna.events").getCount should be(38)
  node4.regions("vdna.events").getCount should be(43)
  (39 + 19 + 38 +43) should be(139)

  val client = new AimClient("localhost", 9997)
  client.query("STATS vdna") 
  client.hasNext should be(true); client.fetchRecordLine should be("events,1,1,39,39,1916,2322")
  client.hasNext should be(true); client.fetchRecordLine should be("events,3,1,38,38,1590,2173")
  client.hasNext should be(true); client.fetchRecordLine should be("events,4,1,43,43,2080,2604")
  client.hasNext should be(true); client.fetchRecordLine should be("events,2,1,19,19,905,1050")
  client.hasNext should be(false); 
  an[EOFException] must be thrownBy(client.fetchRecordLine)

  
  new Loader("localhost", 9998, Protocol.LOADER_USER, "addthis", "syncs", "    ", this.getClass.getResourceAsStream("datasync_string.csv"), false).streamInput should be(10)
  client.query("STATS addthis") 
  while(client.hasNext) {
    println(client.fetchRecordLine)
  }
  
  node1.shutdown
  node2.shutdown
  node3.shutdown
  node4.shutdown
  manager.close

}