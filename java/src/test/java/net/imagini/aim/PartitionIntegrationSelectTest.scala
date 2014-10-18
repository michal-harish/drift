package net.imagini.aim

class PartitionIntegrationSelectTest extends PartitionIntegrationTest {

  "Multiple loaders" should "be merged and sorted" in {
     val server = fixutreServer
     fixutreLoadDataSyncs
     fixutreLoadPageviews
     val result = selectAll

     result.hasNext should be(true)
     result.fetchRecordStrings(0) should equal("04732d65-d530-4b18-a583-53799838731a")
     result.hasNext should be(true)
     result.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
     result.hasNext should be(true)
     result.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
     result.hasNext should be(true)
     result.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
     result.hasNext should be(true)
     result.fetchRecordStrings(0) should equal("37b22cfb-a29e-42c3-a3d9-12d32850e103")
     result.hasNext should be(true)
     result.fetchRecordStrings(0) should equal("69a82e00-3f54-b96a-8fe0-8d2a51f80a86")
     result.hasNext should be(true)
     result.fetchRecordStrings(0) should equal("883f5b55-a5bf-480e-9983-8bb117437eac")
     result.hasNext should be(true)
     result.fetchRecordStrings(0) should equal("d1d284b7-b04e-442a-b52a-ea74bc6466c5")

     fetchAll(result).foreach(println(_))
     result.count should equal(8)

     server.close
  }

  "Partition with 1 segment" should "should return all records after selecting loaded test data" in {

    val server = fixutreServer
    fixutreLoadDataSyncs

    val result = selectAll()
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748041", "x", "ad36ec72-5b66-44f0-89be-668882c08ca5"))
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("37b22cfb-a29e-42c3-a3d9-12d32850e103", "1413143748052", "a", "6571796330792743131"))
    result.hasNext should be(true)
    result.fetchRecordStrings should be(Array("d1d284b7-b04e-442a-b52a-ea74bc6466c5", "1413143748080", "ydx_vdna_id", "e9dd0b85-e3e8-f0c4-c42a-fdb0bf6cd28c"))
    result.hasNext should be(false)
    result.count should be(3L)
    result.filteredCount should be(3L)
    result.close
    server.close

  }

}