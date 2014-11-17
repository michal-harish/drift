package net.imagini.aim.cluster

import net.imagini.aim.client.DriftLoader
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.utils.View
import grizzled.slf4j.Logger
import net.imagini.aim.types.TypeUtils

class AimNodeLoader(val keyspace: String, val table: String, val node: AimNode) {
  val log = Logger[this.type]
  val totalNodes = node.manager.expectedNumNodes
  val region = node.regions(keyspace + "." + table)
  val schema = region.schema
  val keyType = schema.get(0)
  val peerLoaders: Map[Int, DriftLoader] = node.peers.map(peer ⇒ {
    peer._1 -> new DriftLoader(peer._2.getHost, peer._2.getPort, Protocol.LOADER_INTERNAL, keyspace, table, "", null, false)
  })

  private var localSegment: AimSegment = region.newSegment
  private var count: Long = 0

  def insert(record: String*) {
    insert((schema.fields, record).zipped.map((f, r) ⇒ f.convert(r)))
  }

  def insert(record: Array[Array[Byte]]) {
    val targetNode = keyType.partition(new View(record(0)), totalNodes) + 1
    if (targetNode == node.id) {
      localSegment = region.appendRecord(localSegment, record)
      count += 1
    } else {
      var r = 0; while (r < record.length) {
        val dataType = schema.dataType(r)
        val value = record(r)
        peerLoaders(targetNode).pipe.getOutputStream.write(value, 0, TypeUtils.sizeOf(dataType, value))
        r+=1
      }
    }
  }
  def insert(record: Array[View]) {
    val targetNode = keyType.partition(record(0), totalNodes) + 1
    if (targetNode == node.id) {
      localSegment = region.appendRecord(localSegment, record)
      count += 1
    } else {
      var r = 0; while (r < record.length) {
        val dataType = schema.dataType(r)
        val view = record(r)
        peerLoaders(targetNode).pipe.getOutputStream.write(view.array, view.offset, dataType.sizeOf(view))
        r+=1
      }
    }
  }

  def finish: Long = {
    region.add(localSegment)
    peerLoaders.values.foreach(peer ⇒ count += peer.ackLoadedCount)
    count
  }

}