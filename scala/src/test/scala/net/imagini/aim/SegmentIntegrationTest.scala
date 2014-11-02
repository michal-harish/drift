package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.tools.RowFilter
import java.io.InputStream
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.types.SortOrder
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.tools.StreamUtils

class SegmentIntegration extends FlatSpec with Matchers {

  "Unsorted segemnt select" should "give an input stream with the same order of records" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
    val p1 = new AimPartition(schema, 10000)
    val s1 = new AimSegmentUnsorted(schema, classOf[BlockStorageLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748041","a","6571796330792743131")
    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748042","a","6571796330792743131")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748043","a","6571796330792743131")
    p1.add(s1.close)

    s1.count should be(3)

    val in: InputStream  = p1.select("*", "column='a'")
    schema.fields.map(t => t.convert(StreamUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("37b22cfb-a29e-42c3-a3d9-12d32850e1031413143748041a6571796330792743131")
    schema.fields.map(t => t.convert(StreamUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("17b22cfb-a29e-42c3-a3d9-12d32850e1031413143748042a6571796330792743131")
    schema.fields.map(t => t.convert(StreamUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("a7b22cfb-a29e-42c3-a3d9-12d32850e1031413143748043a6571796330792743131")
    in.close

    p1.count("column=a") should be(3)
    p1.count("timestamp>1413143748041") should be(2)

  }

  "QuickSorted ASC segemnt select" should "give an input stream with the correct order" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
    val p1 = new AimPartition(schema, 10000)
    val s1 = new AimSegmentQuickSort(schema, classOf[BlockStorageLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748041","a","6571796330792743131")
    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748042","a","6571796330792743131")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748043","a","6571796330792743131")
    p1.add(s1.close)
    s1.count should be(3)
    p1.count("column=a") should be(3)
    p1.count("timestamp>1413143748041") should be(2)
    val in: InputStream  = p1.select("*", "column='a'")
    schema.fields.map(t => t.convert(StreamUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("17b22cfb-a29e-42c3-a3d9-12d32850e1031413143748042a6571796330792743131")
    schema.fields.map(t => t.convert(StreamUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("37b22cfb-a29e-42c3-a3d9-12d32850e1031413143748041a6571796330792743131")
    schema.fields.map(t => t.convert(StreamUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("a7b22cfb-a29e-42c3-a3d9-12d32850e1031413143748043a6571796330792743131")
    in.close


  }

//  "QuickSorted DESC  segemnt select" should "give an input stream with the correct order" in {
//    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),timestamp(LONG),column(STRING),value(STRING)")
//    val s1 = new AimSegmentQuickSort(schema, "user_uid", SortOrder.DESC, classOf[BlockStorageLZ4])
//    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748041","a","6571796330792743131")
//    s1.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748042","a","6571796330792743131")
//    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103","1413143748043","a","6571796330792743131")
//    s1.close
//    s1.count(RowFilter.emptyFilter) should be(3)
//    s1.count(RowFilter.fromString(schema, "column=a")) should be(3)
//    s1.count(RowFilter.fromString(schema, "timestamp>1413143748041")) should be(2)
//    val in: InputStream  = s1.select(RowFilter.fromString(schema, "column='a'"), schema.names)
//    schema.fields.map(t => t.convert(PipeUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("a7b22cfb-a29e-42c3-a3d9-12d32850e1031413143748043a6571796330792743131")
//    schema.fields.map(t => t.convert(PipeUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("37b22cfb-a29e-42c3-a3d9-12d32850e1031413143748041a6571796330792743131")
//    schema.fields.map(t => t.convert(PipeUtils.read(in, t.getDataType))).foldLeft("")(_ + _) should be ("17b22cfb-a29e-42c3-a3d9-12d32850e1031413143748042a6571796330792743131")
//    in.close
//  }

}