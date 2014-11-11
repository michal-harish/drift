package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.partition.StatScanner
import java.io.EOFException

class PartitionTest extends FlatSpec with Matchers {
  "Stat partition " should " behave as a normal table" in {
    val schema = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),column(STRING),value(STRING)")
    val s1 = new AimSegmentQuickSort(schema, classOf[BlockStorageMEMLZ4])
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}")
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}")
    val s2 = new AimSegmentQuickSort(schema, classOf[BlockStorageMEMLZ4])
    s2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}")
    s2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876")
    s2.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}")
    val partition1 = new AimPartition(schema, 1000)
    partition1.add(s1)
    partition1.add(s2)

    //USERFLAGS //TODO ttl = -1
    val schemaUserFlags = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),flag(STRING),value(BOOL)")
    val sC1 = new AimSegmentQuickSort(schemaUserFlags, classOf[BlockStorageMEMLZ4])
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true")
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    val sC2 = new AimSegmentQuickSort(schemaUserFlags, classOf[BlockStorageMEMLZ4])
    sC2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false")
    val partitionUserFlags1 = new AimPartition(schemaUserFlags, 1000)
    partitionUserFlags1.add(sC1)
    partitionUserFlags1.add(sC2)

    val scanner = new StatScanner(1, Map("pageviews" -> partition1, "flags" -> partitionUserFlags1))
    scanner.nextLine should be("flags\t1\t2\t5\t5\t121\t141")
    scanner.nextLine should be("pageviews\t1\t2\t6\t6\t218\t267")
    an[EOFException] must be thrownBy(scanner.nextLine)
  }

}