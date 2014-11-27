package net.imagini.aim.cluster

import java.io.EOFException
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.client.DriftLoader
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.segment.SegmentScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageMEMLZ4

class BigLoadTest extends FlatSpec with Matchers {

  val schema = AimSchema.fromString("at_id(STRING), url(STRING), timestamp(LONG)")

  private def getNode: AimNode = {
    val manager = new DriftManagerLocal(1)
    val storageType = classOf[BlockStorageMEMLZ4]
    val node = new AimNode(1, "localhost:9998", manager)
    manager.createTable("addthis", "views", schema.toString, 5000000, storageType)
    new DriftLoader(manager, "addthis", "views", '\t', this.getClass.getResourceAsStream("views_big.csv"), false).streamInput should be(5730)
    val region = node.regions("addthis.views")
    node.query("count addthis.views").count should be(5730)
    region.segments.size should be(1)
    node
  }

  "SegmentScanner" should "yield same for all" in {
    var total = 0
    var filtered = 0
    val node = getNode
    val region = node.regions("addthis.views")
    val segment = region.segments(0)
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

  "MergeScanner for all" should "yield same as grep" in {
    var total = 0
    var filtered = 0
    val node = getNode
    val region = node.regions("addthis.views")
    val segment = region.segments(0)
    val mergeScanner = new MergeScanner("*", "*", region.segments)
    while (mergeScanner.next) {
      try {
        val line = mergeScanner.selectLine(" ")
        total += 1
        if (line.contains("http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets")) {
          filtered += 1
        }
      } catch {
        case e: Throwable ⇒ {
          e.printStackTrace
          throw e
        }
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
    val region = node.regions("addthis.views")
    val segment = region.segments(0)
    val segmentScanner = new SegmentScanner("*", "url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'", segment)
    while (segmentScanner.next) {
      filtered += 1
      val line = segmentScanner.selectLine(" ")
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

  "StreamMergeScanner for filtered" should "yield same as grep" in {
    var filtered = 0
    val node = getNode
    val mergeScanner = node.query("select * from addthis.views where url contains 'http://www.toysrus.co.uk/Toys-R-Us/Toys/Cars-and-Trains/Cars-and-Playsets'")
    val streamScanner = new ScannerInputStream(mergeScanner)
    try {
      while (true) {
        mergeScanner.schema.fields.map(field ⇒ StreamUtils.read(streamScanner, field))
        filtered += 1
      }
    } catch {
      case e: EOFException ⇒ {}
    }
    filtered should be(158)
    node.manager.down
  }

}