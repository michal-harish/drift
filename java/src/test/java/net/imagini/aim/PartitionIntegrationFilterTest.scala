package net.imagini.aim

import net.imagini.aim.cluster.AimResult
import net.imagini.aim.cluster.AimClient
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder

class PartitionIntegrationFilterTest extends PartitionIntegrationTest {
  "Filter over muitlple loaders" should "return subset from all sources" in {
    val server = fixutreServer
    fixutreLoadDataSyncs
    fixutreLoadPageviews
    val result = select("filter user_uid='37b22cfb-a29e-42c3-a3d9-12d32850e103'")
    val records = fetchAll(result)
    records.size should equal(4)
    records.foreach(println(_))

    val result2 = select("filter column contains 'x'")
    val records2 = fetchAll(result2)
    records2.size should equal(2)
    records2.foreach(println(_))

    server.close
  }

}