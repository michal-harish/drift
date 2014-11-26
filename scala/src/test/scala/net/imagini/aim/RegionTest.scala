package net.imagini.aim

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.region.AimRegion
import net.imagini.aim.region.StatScanner
import java.io.EOFException
import net.imagini.aim.types.AimTableDescriptor

class RegionTest extends FlatSpec with Matchers {
  "Stat region " should " behave as a normal table" in {
    val descriptor = new AimTableDescriptor(
      AimSchema.fromString("user_uid(UUID),column(STRING),value(STRING)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      classOf[AimSegmentQuickSort])
    val region1 = new AimRegion("vdna.events", descriptor)
    val s1 = region1.newSegment
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}")
    s1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234")
    s1.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}")
    val s2 = region1.newSegment
    s2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}")
    s2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876")
    s2.appendRecord("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}")

    region1.add(s1)
    region1.add(s2)
    region1.compact

    //USERFLAGS //TODO ttl = -1
    val userFlags = new AimTableDescriptor(
      AimSchema.fromString("user_uid(UUID),flag(STRING),value(BOOL)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      classOf[AimSegmentQuickSort])
    val regionUserFlags1 = new AimRegion("vdna.flags", userFlags)
    val sC1 = regionUserFlags1.newSegment
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true")
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    val sC2 = regionUserFlags1.newSegment
    sC2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false")

    regionUserFlags1.add(sC1)
    regionUserFlags1.add(sC2)
    regionUserFlags1.compact

    val scanner = new StatScanner(1, Map("pageviews" -> region1, "flags" -> regionUserFlags1))
    scanner.nextLine should be("flags\t1\t2\t121") //\t141
    scanner.nextLine should be("pageviews\t1\t2\t218") //\t267
    an[EOFException] must be thrownBy (scanner.nextLine)
  }

}