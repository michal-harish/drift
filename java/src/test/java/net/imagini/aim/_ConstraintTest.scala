package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.tools.StreamMerger
import net.imagini.aim.tools.AimFilter
import net.imagini.aim.tools.PipeUtils
import java.io.EOFException

class ConstraintTest extends FlatSpec with Matchers {
  "simple group filter " should "yield correct group" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),column(STRING),value(STRING)")
    val s1 = new AimSegmentQuickSort(schema, classOf[BlockStorageLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}")
    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234")
    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "ddp_id", "1093847234")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}")
    s1.close
    val s2 = new AimSegmentQuickSort(schema, classOf[BlockStorageLZ4])
    s2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}")
    s2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876")
    s2.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}")
    s2.close

    /*
     * select user_uid, column filter column='pageview' constraint column == 'ddp_id' or 
     * select user_uid, column filter column='pageview' constraint column == 'addthis_id' and value=='AT1234'
     */
    val filter = AimFilter.fromString(schema, "column='pageview' or column='ddp_id'")
    val select = schema.subset(Array("user_uid", "value"))
    val mergeSort = new StreamMerger(select, 2, Array(
      s1.select(filter, select.names),
      s2.select(filter, select.names)))

    try {
      while (true) {
        println(select.fields.map(t ⇒ { t.convert(PipeUtils.read(mergeSort, t.getDataType)) }).foldLeft("")(_ + _ + " "))
      }
    } catch {
      case e: EOFException ⇒ {
        println("EOF")
        mergeSort.close
      }
    }

  }

}