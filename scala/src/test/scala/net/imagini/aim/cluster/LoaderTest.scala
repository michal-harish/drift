package net.imagini.aim.cluster

import net.imagini.aim.client.DriftLoader
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import java.io.EOFException

class LoaderTest extends FlatSpec with Matchers {
  "ClientLoader" should " load csv and gz" in {
    val manager = new DriftManagerLocal(1)
    manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
    val node = new AimNode(1, "localhost:9998", manager)

    new DriftLoader(manager, "vdna", "events", '\t', this.getClass.getResourceAsStream("datasync.csv"), false).streamInput should be(3)
    node.query("count vdna.events").count should be(3L)

    new DriftLoader(manager, "vdna", "events", '\t', this.getClass.getResourceAsStream("datasync.csv.gz"), true).streamInput should be(3)
    node.query("count vdna.events").count should be(6L)

    manager.down
  }

  "ServerLoader" should " load from record view" in {
    val manager = new DriftManagerLocal(1)
    manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
    val node = new AimNode(1, "localhost:9998", manager)
    val loader = new AimNodeLoader(manager, "vdna", "events")
    loader.insert("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413061544595", "VDNAUserPageview", "http://zh.pad.wikia.com/wiki/Puzzle_%26_Dragons_%E4%B8%AD%E6%96%87WIKI")
    loader.insert("04732d65-d530-4b18-a583-53799838731a", "1413061544599", "VDNAUserPageview", "http://www.gumtree.com/flats-and-houses-for-rent/caerphilly")
    loader.finish should be(2)
    val scanner = node.query("select * from vdna.events")
    scanner.next should be(true); scanner.selectLine(" ") should be("04732d65-d530-4b18-a583-53799838731a 1413061544599 VDNAUserPageview http://www.gumtree.com/flats-and-houses-for-rent/caerphilly")
    scanner.next should be(true); scanner.selectLine(" ") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 1413061544595 VDNAUserPageview http://zh.pad.wikia.com/wiki/Puzzle_%26_Dragons_%E4%B8%AD%E6%96%87WIKI")
    scanner.next should be(false); an[EOFException] must be thrownBy (scanner.selectLine(" "))
    manager.down

  }
}