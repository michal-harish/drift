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

class QueryParserTest extends FlatSpec with Matchers {

  val partition = pageviewsPartition
  val regions = Map[String, AimPartition](
    "pageviews" -> partition)
  val parser = new QueryParser(regions)

  val frame = parser.frame("select * from pageviews  where url contains 'travel'")
 println(frame)
  

  val scanner = parser.parse("select * from pageviews  where url contains 'travel'")
  scanner.next should be(true);
  scanner.next should be(true);
  scanner.next should be(true);
  scanner.next should be(false);
  scanner.rewind
  scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 12:01:02")
  scanner.next should be(true); scanner.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday,2014-10-10 12:01:03")
  scanner.next should be(true); scanner.selectLine(",") should be("a7b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 13:01:03")
  scanner.next should be(false);

  val scanner2 = parser.parse("select * from pageviews where user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'")
  scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.auto.com/mycar,2014-10-10 11:59:01")
  scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers,2014-10-10 12:01:02")
  scanner2.next should be(true); scanner2.selectLine(",") should be("37b22cfb-a29e-42c3-a3d9-12d32850e103,www.travel.com/offers/holiday,2014-10-10 12:01:03")
  scanner2.next should be(false);

  //TODO parse("select 'user_uid' from flags where value='true' and flag='quizzed' or flag='cc' JOIN (SELECT user_uid,url,timestamp FROM pageviews WHERE time > 1000 UNION SELECT * FROM converesions)")

  private def pageviewsPartition: AimPartition = {
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
}