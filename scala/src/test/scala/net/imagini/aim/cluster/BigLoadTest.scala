package net.imagini.aim.cluster

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.client.Loader
import java.io.EOFException
import net.imagini.aim.segment.SegmentScanner
import net.imagini.aim.segment.MergeScanner

class BigLoadTest extends FlatSpec with Matchers {

  private def getNode: AimNode = {
    val manager = new DriftManagerLocal(1)
    val storageType = classOf[BlockStorageLZ4]
    val node = new AimNode(1, "localhost:9998", manager)
    manager.createTable("addthis", "views", "at_id(STRING), url(STRING), timestamp(LONG)", 5000000, storageType)
    new Loader("localhost", 9998, Protocol.LOADER_USER, "addthis", "views", "\t", this.getClass.getResourceAsStream("views_big.csv"), false).streamInput should be(5730)
    val partition = node.regions("addthis.views")
    partition.getCount should be(5730)
    partition.segments.size should be(1)
    node
  }

  "SegmentScanner" should "yield same for all" in {
    var total = 0
    var filtered = 0
    val node = getNode
    val partition = node.regions("addthis.views")
    val segment = partition.segments(0)
    val segmentScanner = new SegmentScanner("*", "*", segment)
    while (segmentScanner.next) {
      total += 1
      if (segmentScanner.selectLine(" ").contains("http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets")) {
        filtered += 1
      }
    }
    total should be(5730)
    filtered should be(158)
    an[EOFException] must be thrownBy (segmentScanner.nextLine)
    node.manager.down
  }

  "SegmentScanner for all" should "yield same as grep" in {
    var total = 0
    var filtered = 0
    val node = getNode
    val partition = node.regions("addthis.views")
    val segment = partition.segments(0)
    val mergeScanner = new MergeScanner("*", "*", partition.segments)
    while (mergeScanner.next) {
      total += 1
      if (mergeScanner.selectLine(" ").contains("http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets")) {
        filtered += 1
      }
    }
    total should be(5730)
    filtered should be(158)
    an[EOFException] must be thrownBy (mergeScanner.nextLine)
    node.manager.down
  }

  "SegmentScanner with filter" should "yield same as grep" in {
    var filtered = 0
    val node = getNode
    val partition = node.regions("addthis.views")
    val segment = partition.segments(0)
    val segmentScanner = new SegmentScanner("*", "url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'", segment)
    while (segmentScanner.next) {
      filtered += 1
      val line = segmentScanner.selectLine(" ")
      if (!line.contains("http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets")) {
        System.err.println(line)
      }
      line.contains("http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets") should be(true)
    }
    filtered should be(158)
    an[EOFException] must be thrownBy (segmentScanner.nextLine)
    node.manager.down
  }

  "MergeScanner for filtered" should "yield same as grep" in {
    var total = 0
    var filtered = 0
    val node = getNode
    val allScanner = node.query("select * from addthis.views")
    while (allScanner.next) {
      total += 1
      if (allScanner.selectLine(" ").contains("http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets")) {
        filtered += 1
      }
    }
    total should be(5730)
    filtered should be(158)
    an[EOFException] must be thrownBy (allScanner.nextLine)
    node.manager.down
  }

  "Partition for all" should "yield all loaded records" in {

    //  val scanner = node.query("select * from addthis.views where url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'")
    //  var count = 0

    //    while (scanner.next) {
    //      System.err.println(scanner.nextLine)
    //      count += 1
    //    }
  }
}