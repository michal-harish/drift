package net.imagini.aim

import org.scalatest.Matchers
import net.imagini.aim.partition.QueryParser
import net.imagini.aim.partition.AimPartition
import org.scalatest.FlatSpec
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.partition.QueryParser
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.partition.PSelect
import net.imagini.aim.partition.PTable
import scala.collection.mutable.ListBuffer
import net.imagini.aim.partition.PWildcard
import net.imagini.aim.partition.PEquiJoin
import net.imagini.aim.partition.PVar
import net.imagini.aim.partition.PUnionJoin

class QueryParserTest extends FlatSpec with Matchers {

  val regions = Map[String, AimPartition](
    "pageviews" -> pageviews,
    "conversions" -> conversions,
    "flags" -> flags)
  val parser = new QueryParser(regions)

  parser.frame("select * from pageviews where url contains 'travel'") should be(
    PSelect(PTable("pageviews"), "url contains 'travel'", PWildcard(PTable("pageviews"))))

  parser.frame("SELECT user_uid,url,timestamp FROM pageviews WHERE timestamp > '2014-10-09 13:59:01' UNION SELECT * FROM conversions") should be(
    PUnionJoin(
      PSelect(PTable("pageviews"), "timestamp > '2014-10-09 13:59:01'", PVar("user_uid"), PVar("url"), PVar("timestamp")),
      PSelect(PTable("conversions"), "*", PWildcard(PTable("conversions"))),
      PVar("user_uid"), PVar("url"), PVar("timestamp"), PWildcard(PTable("conversions"))))

  parser.frame("select user_uid from flags where value='true' and flag='quizzed' or flag='cc' JOIN SELECT * FROM conversions") should be(
    PEquiJoin(
      PSelect(PTable("flags"), "value = 'true' and flag = 'quizzed' or flag = 'cc'", PVar("user_uid")),
      PSelect(PTable("conversions"), "*", PWildcard(PTable("conversions"))),
      PVar("user_uid"), PWildcard(PTable("conversions"))))


  parser.frame("select user_uid from flags where value='true' and flag='quizzed' or flag='cc' "
    + "JOIN (SELECT user_uid,url,timestamp FROM pageviews WHERE timestamp > '2014-10-09 13:59:01' UNION SELECT * FROM conversions)") should be(
    PEquiJoin(
      PSelect(PTable("flags"), "value = 'true' and flag = 'quizzed' or flag = 'cc'", PVar("user_uid")),
      PUnionJoin(
        PSelect(PTable("pageviews"), "timestamp > '2014-10-09 13:59:01'", PVar("user_uid"), PVar("url"), PVar("timestamp")),
        PSelect(PTable("conversions"), "*", PWildcard(PTable("conversions"))), 
        PVar("user_uid"), PVar("url"), PVar("timestamp"), PWildcard(PTable("conversions"))
      ),
      PVar("user_uid"), PVar("user_uid"), PVar("url"), PVar("timestamp"), PWildcard(PTable("conversions"))
    ))

  private def pageviews: AimPartition = {
    val schemaPageviews = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),url(STRING),timestamp(TIME:LONG)")
    val sA1 = new AimSegmentQuickSort(schemaPageviews, classOf[BlockStorageLZ4])
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.auto.com/mycar", "2014-10-10 11:59:01") //0  1
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 12:01:02") //16 1
    sA1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers/holiday", "2014-10-10 12:01:03") //32 1
    sA1.appendRecord("12322cfb-a29e-42c3-a3d9-12d32850e103", "www.xyz.com", "2014-10-10 12:01:02") //48 2
    sA1.close
    val sA2 = new AimSegmentQuickSort(schemaPageviews, classOf[BlockStorageLZ4])
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sA2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "www.travel.com/offers", "2014-10-10 13:01:03")
    sA2.close
    val partitionPageviews = new AimPartition(schemaPageviews, 10000)
    partitionPageviews.add(sA1)
    partitionPageviews.add(sA2)
    partitionPageviews

  }

  private def conversions: AimPartition = {
    //CONVERSIONS //TODO ttl = 10
    val schemaConversions = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),conversion(STRING),url(STRING),timestamp(TIME:LONG)")
    val sB1 = new AimSegmentQuickSort(schemaConversions, classOf[BlockStorageLZ4])
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "check", "www.bank.com/myaccunt", "2014-10-10 13:59:01")
    sB1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "buy", "www.travel.com/offers/holiday/book", "2014-10-10 13:01:03")
    sB1.close
    val partitionConversions1 = new AimPartition(schemaConversions, 1000)
    partitionConversions1.add(sB1)
    partitionConversions1
  }

  private def flags: AimPartition = {
    //USERFLAGS //TODO ttl = -1
    val schemaUserFlags = AimSchema.fromString("user_uid(UUID:BYTEARRAY[16]),flag(STRING),value(BOOL)")
    val sC1 = new AimSegmentQuickSort(schemaUserFlags, classOf[BlockStorageLZ4])
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "true")
    sC1.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    sC1.close
    val sC2 = new AimSegmentQuickSort(schemaUserFlags, classOf[BlockStorageLZ4])
    sC2.appendRecord("37b22cfb-a29e-42c3-a3d9-12d32850e103", "opt_out_targetting", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "cc", "true")
    sC2.appendRecord("a7b22cfb-a29e-42c3-a3d9-12d32850e103", "quizzed", "false")
    sC2.close
    val partitionUserFlags1 = new AimPartition(schemaUserFlags, 1000)
    partitionUserFlags1.add(sC1)
    partitionUserFlags1.add(sC2)
    partitionUserFlags1
  }

}