package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageLZ4

class SegmentTest extends FlatSpec with Matchers {

  "Segemnt select" should "give an input stream" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
    val s1 = new AimSegmentUnsorted(schema, classOf[BlockStorageLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748041","a","6571796330792743131")
    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748041","a","6571796330792743131")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748041","a","6571796330792743131")
    s1.close
//    s1.select(AimFilter.fromString("column='a'"), columns)
  }
}