package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.types.SortOrder
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.segment.RowFilter
import net.imagini.aim.cluster.StreamMerger
import java.io.EOFException
import net.imagini.aim.region.AimRegion
import net.imagini.aim.cluster.StreamUtils
import net.imagini.aim.cluster.ScannerInputStream
import net.imagini.aim.segment.MergeScanner
import java.io.InputStream
import net.imagini.aim.types.AimTableDescriptor
import net.imagini.aim.utils.View
import net.imagini.aim.types.SortType

class StreamMergeIntegrationTest extends FlatSpec with Matchers {

  private def readRecord(schema: AimSchema, stream: InputStream) = {
    schema.fields.map(t ⇒ { t.asString(StreamUtils.read(stream, t)) }).foldLeft("")(_ + _ + " ")
  }
  "2 sorted segments" should "yield correct groups when merge-sorted" in {
    val schema = AimSchema.fromString("user_uid(UUID),column(STRING),value(STRING)")
    val d = new AimTableDescriptor(schema, 10000, classOf[BlockStorageMEMLZ4], SortType.QUICK_SORT)
    val p1 = new AimRegion("vdna.events", d)
    p1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}"),
      Seq("17b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}"))

    val p2 = new AimRegion("vdna.events", d)
    p2.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876"),
      Seq("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}"))

    val subschema = schema.subset(Array("user_uid", "value"))

    val stream1 = new ScannerInputStream(new MergeScanner("user_uid,value", "column='pageview'", p1.segments))

    val stream2 = new ScannerInputStream(new MergeScanner("user_uid,value", "column='pageview'", p2.segments))

    val mergeStream = new StreamMerger(subschema, 1, Array(stream1, stream2))

    readRecord(subschema, mergeStream) should be("17b22cfb-a29e-42c3-a3d9-12d32850e103 {www.music.com} ")
    readRecord(subschema, mergeStream) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 {www.auto.com} ")
    readRecord(subschema, mergeStream) should be("37b22cfb-a29e-42c3-a3d9-12d32850e103 {www.ebay.com} ")
    readRecord(subschema, mergeStream) should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103 {www.travel.com} ")

    an[EOFException] must be thrownBy readRecord(subschema, mergeStream)

    mergeStream.close
  }

}