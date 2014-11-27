package net.imagini.aim

import java.io.EOFException
import java.io.InputStream
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import net.imagini.aim.cluster.ScannerInputStream
import net.imagini.aim.cluster.StreamUtils
import net.imagini.aim.region.AimRegion
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimTableDescriptor
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.utils.View

class ScannerInputStreamTest extends FlatSpec with Matchers {

  val schema = AimSchema.fromString("user_uid(UUID),url(STRING),timestamp(TIME)")

  private def readLine(in: InputStream) = {
    schema.get(0).asString(StreamUtils.read(in, schema.get(0))) + " " + schema.get(1).asString(StreamUtils.read(in, schema.get(1)))
  }

  "Region select " should " use ScannerInputStream correctly" in {
    val d = new AimTableDescriptor( schema, 10000, classOf[BlockStorageMEMLZ4], classOf[AimSegmentQuickSort])
    val p = new AimRegion("vdna.pageviews", d)
    val sA1 = p.newSegment
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01")
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02")
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03")
    sA1.appendRecord("12322cfb-a29e-42c3-a3d9-12d32850e103", "www.xyz.com", "2014-10-10 12:01:02")
    p.add(sA1)
    p.compact

    val in = new ScannerInputStream(new MergeScanner("user_uid,url", "*", p.segments))
    readLine(in) should be("12322cfb-a29e-42c3-a3d9-12d32850e103 www.xyz.com")
    readLine(in) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 www.auto.com/mycar")
    readLine(in) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 www.travel.com/offers")
    readLine(in) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 www.travel.com/offers/holiday")
    in.close
  }

  "ScannerInputStream " should "be able to read can read MergeScanner byte by byte" in {

    val schemaATSyncs = AimSchema.fromString("at_id(STRING), user_uid(UUID)")
    val dAtSyncs = new AimTableDescriptor(schemaATSyncs, 1000, classOf[BlockStorageMEMLZ4], classOf[AimSegmentQuickSort])
    val AS1 = new AimRegion("addthis.syncs", dAtSyncs)
    AS1.add(AS1.newSegment
      .appendRecord("AT5656", "a7b22cfb-a29e-42c3-a3d9-12d32850e234")
      .appendRecord("AT1234", "37b22cfb-a29e-42c3-a3d9-12d32850e103")
      .appendRecord("AT7888", "89777987-a29e-42c3-a3d9-12d32850e234")
      )
    AS1.compact

    val scanner = new MergeScanner("at_id", "*", AS1.segments)
    scanner.nextLine should be("AT1234")
    scanner.nextLine should be("AT5656")
    scanner.nextLine should be("AT7888")
    an[EOFException] must be thrownBy scanner.nextLine
    an[EOFException] must be thrownBy scanner.nextLine

    val scanner2 = new MergeScanner("at_id", "*", AS1.segments)

    val t = schemaATSyncs.field("at_id")
    val scannerStream = new ScannerInputStream(scanner2)
    t.asString(StreamUtils.read(scannerStream, t)) should be("AT1234")
    t.asString(StreamUtils.read(scannerStream, t)) should be("AT5656")
    t.asString(StreamUtils.read(scannerStream, t)) should be("AT7888")
    scannerStream.read should be(-1)
    an[EOFException] must be thrownBy StreamUtils.read(scannerStream, t)

    scannerStream.close

  }

}