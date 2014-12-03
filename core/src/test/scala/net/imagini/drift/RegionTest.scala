package net.imagini.drift

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.utils.BlockStorageMEMLZ4
import net.imagini.drift.region.DriftRegion
import net.imagini.drift.region.StatScanner
import java.io.EOFException
import net.imagini.drift.types.DriftTableDescriptor
import net.imagini.drift.types.SortType

class RegionTest extends FlatSpec with Matchers {
  "Stat region " should " behave as a normal table" in {
    val descriptor = new DriftTableDescriptor(
      DriftSchema.fromString("user_uid(UUID),column(STRING),value(STRING)"),
      1000, classOf[BlockStorageMEMLZ4], SortType.QUICK_SORT)
    val region1 = new DriftRegion("vdna.events", descriptor)
    region1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.auto.com}"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT1234"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.travel.com}"))
    region1.compact
    region1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.ebay.com}"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "addthis_id", "AT9876"),
      Seq("17b22cfb-a29e-42c3-a3d9-12d32850e103", "pageview", "{www.music.com}"))
    region1.compact

    //USERFLAGS //TODO ttl = -1
    val userFlags = new DriftTableDescriptor(
      DriftSchema.fromString("user_uid(UUID),flag(STRING),value(BOOL)"),
      1000,
      classOf[BlockStorageMEMLZ4],
      SortType.QUICK_SORT)
    val regionUserFlags1 = new DriftRegion("vdna.flags", userFlags)
    regionUserFlags1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true"),
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true"))
    regionUserFlags1.addTestRecords(
      Seq("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true"),
      Seq("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false"))
    regionUserFlags1.compact

    val scanner = new StatScanner(1, Map("pageviews" -> region1, "flags" -> regionUserFlags1))
    scanner.nextLine should be("flags\t1\t2\t121") //\t141
    scanner.nextLine should be("pageviews\t1\t2\t218") //\t267
    an[EOFException] must be thrownBy (scanner.nextLine)
  }

}