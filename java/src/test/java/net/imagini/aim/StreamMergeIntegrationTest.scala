package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.types.SortOrder
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.segment.AimFilter
import scala.collection.JavaConverters._
import net.imagini.aim.tools.PipeUtils
import net.imagini.aim.tools.StreamMerger

class StreamMergeIntegrationTest extends FlatSpec with Matchers {
  
  "2 sorted segments" should "yield correct groups when merge-sorted" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),column(STRING),value(STRING)")
    val s1 = new AimSegmentQuickSort(schema, "user_uid", SortOrder.ASC, classOf[BlockStorageLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103","pageview",  "{www.auto.com}")
    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103","addthis_id","AT1234")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103","pageview",  "{www.travel.com}")
    s1.close

    val s2 = new AimSegmentQuickSort(schema, "user_uid", SortOrder.ASC, classOf[BlockStorageLZ4])
    s2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103","pageview",  "{www.ebay.com}")
    s2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103","addthis_id","AT9876")
    s2.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103","pageview",  "{www.music.com}")
    s2.close

    val subschema = schema.subset(List("user_uid","value").asJava)
    val filter = AimFilter.fromString(schema, "column='pageview'")
    val mergeSort = new StreamMerger(
        subschema, Seq(
            s1.select(filter, subschema.names), 
            s2.select(filter, subschema.names)
        )
    )

    System.err.println(schema.fields.map(t => t.convert(PipeUtils.read(mergeSort, t.getDataType)).foldLeft("")(_ + _ + " ")))
    System.err.println(schema.fields.map(t => t.convert(PipeUtils.read(mergeSort, t.getDataType)).foldLeft("")(_ + _ + " ")))
    System.err.println(schema.fields.map(t => t.convert(PipeUtils.read(mergeSort, t.getDataType)).foldLeft("")(_ + _ + " ")))
    System.err.println(schema.fields.map(t => t.convert(PipeUtils.read(mergeSort, t.getDataType)).foldLeft("")(_ + _ + " ")))

    mergeSort.close
  }

}