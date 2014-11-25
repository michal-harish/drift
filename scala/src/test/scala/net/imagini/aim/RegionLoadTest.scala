package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.region.AimRegion
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.types.TypeUtils
import net.imagini.aim.utils.ByteUtils
import java.util.UUID
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.utils.View
import net.imagini.aim.utils.BlockStorageMEM
import net.imagini.aim.segment.AimSegmentUnsorted
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.segment.SegmentScanner
import net.imagini.aim.types.AimTableDescriptor

class RegionLoadTest extends FlatSpec with Matchers {
  //unsorted
  "Single-segment region with unsorted raw storage" should "keep consistent state" in {
    runLoadTestUnsorted(1, classOf[BlockStorageMEM])
  }
  "Single-segment region  with unsorted lz4 storage" should "keep consistent state" in {
    runLoadTestUnsorted(1, classOf[BlockStorageMEMLZ4])
  }
  "5-segment region  with unsorted raw storage" should "keep consistent state" in {
    runLoadTestUnsorted(5, classOf[BlockStorageMEM])
  }
  "5-segment region with unsorted lz4 storage" should "keep consistent state" in {
    runLoadTestUnsorted(5, classOf[BlockStorageMEMLZ4])
  }
  //quick-sorted
  "Single block segment with quick-sorted raw storage" should "keep consistent state" in {
    runLoadTestQuickSorted(1, classOf[BlockStorageMEM])
  }
  "5-segment region with with quick-sorted raw storage" should "keep consistent state" in {
    runLoadTestQuickSorted(5, classOf[BlockStorageMEM])
  }
  "Single block segment with quick-sorted LZ4 storage" should "keep consistent state" in {
    runLoadTestQuickSorted(1, classOf[BlockStorageMEMLZ4])
  }
  "5-segment region with with quick-sorted LZ4 storage" should "keep consistent state" in {
    runLoadTestQuickSorted(5, classOf[BlockStorageMEMLZ4])
  }

  private def runLoadTestUnsorted(numSegments: Int, storageType: Class[_ <: BlockStorage]) {
    val schema = AimSchema.fromString("uid(UUID:BYTEARRAY[16]),i(INT),s(STRING)")
    val recordSize = 16 + 4 + 14
    val recordsPerSegment = 3000
    val segmentSize = recordSize * recordsPerSegment
    val numRecords = recordsPerSegment * numSegments
    val ids: Array[String] = Array("0dc56198-975d-4cf9-9b3f-a52581dee886", "32c07e66-0824-4e1c-b126-bd0a2e586bae")

    val descriptor = new AimTableDescriptor(schema, segmentSize, storageType, classOf[AimSegmentUnsorted])
    val region = new AimRegion("test.data", descriptor)

    var segment = region.newSegment
    val recordView = new Array[View](schema.size)
    for (r ← (1 to numRecords)) {

      recordView(0) = new View(schema.get(0).convert(ids(r % ids.size)))
      schema.get(0).asString(recordView(0)) should equal(ids(r % ids.size))

      recordView(1) = new View(schema.get(1).convert(r.toString))
      schema.get(1).asString(recordView(1)) should equal(r.toString)
      ByteUtils.asIntValue(recordView(1).array, recordView(1).offset) should equal(r)

      val s = r.toString.padTo(10, '0')
      recordView(2) = new View(schema.get(2).convert(s))
      schema.get(2).asString(recordView(2)) should equal(s)

      segment = region.appendRecord(segment, recordView)
    }
    region.add(segment)
    region.compact
    region.getNumSegments should equal(numRecords * (16 + 4 + 14) / segmentSize)

    for (s ← (0 to region.segments.length - 1)) {
      val scanner = new SegmentScanner("*", "*", region.segments(s))
      for (r ← (s * recordsPerSegment + 1 to (s + 1) * recordsPerSegment)) {
        scanner.next should equal(true)
        val row = scanner.selectRow
        schema.get(0).asString(row(0)) should equal(ids(r % ids.size))
        ByteUtils.asIntValue(row(1).array, row(1).offset) should equal(r)
        schema.get(1).asString(row(1)) should equal(r.toString)
        schema.get(2).asString(row(2)) should equal(r.toString.padTo(10, '0'))
      }
    }
  }

  private def runLoadTestQuickSorted(numSegments: Int, storageType: Class[_ <: BlockStorage]) {
    val schema = AimSchema.fromString("uid(UUID:BYTEARRAY[16]),i(INT),s(STRING)")
    val recordSize = 16 + 4 + 14
    val recordsPerSegment = 3000
    val segmentSize = recordSize * recordsPerSegment
    val numRecords = recordsPerSegment * numSegments
    val ids: Array[String] = Array("0dc56198-975d-4cf9-9b3f-a52581dee886", "32c07e66-0824-4e1c-b126-bd0a2e586bae")

    val descriptor = new AimTableDescriptor(schema, segmentSize, storageType, classOf[AimSegmentQuickSort])
    val region = new AimRegion("test.data", descriptor)

    var segment = region.newSegment
    val recordView = new Array[View](schema.size)
    for (r ← (1 to numRecords)) {
      recordView(0) = new View(schema.get(0).convert(ids(r % ids.size)))
      schema.get(0).asString(recordView(0)) should equal(ids(r % ids.size))

      recordView(1) = new View(schema.get(1).convert(r.toString))
      schema.get(1).asString(recordView(1)) should equal(r.toString)
      ByteUtils.asIntValue(recordView(1).array, recordView(1).offset) should equal(r)

      val s = r.toString.padTo(10, '0')
      recordView(2) = new View(schema.get(2).convert(s))
      schema.get(2).asString(recordView(2)) should equal(s)

      segment = region.appendRecord(segment, recordView)
    }
    region.add(segment)
    region.compact
    //TODO scan count region.getCount should equal(numRecords)
    region.getNumSegments should equal(numRecords * (16 + 4 + 14) / segmentSize)

    for (s ← (0 to region.segments.length - 1)) {
      val scanner = new SegmentScanner("*", "*", region.segments(s))
      for (r ← (1 to recordsPerSegment)) {
        val expectedId = ids(if (r <= recordsPerSegment / 2) 0 else 1)
        //System.err.println(s + ": " + r + " expecting " + expectedId)
        scanner.next should equal(true)
        val row = scanner.selectRow
        schema.get(0).asString(row(0)) should equal(expectedId)
        val i = ByteUtils.asIntValue(row(1).array, row(1).offset)
        (i > s * recordsPerSegment && i <= s * recordsPerSegment + recordsPerSegment) should equal(true)
        //          schema.get(1).asString(row(1)) should equal(r.toString)
        //          schema.get(2).asString(row(2)) should equal(r.toString.padTo(10, '0'))
      }
    }
  }
}