package net.imagini.aim.cluster

import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.segment.AimSegment
import java.io.EOFException
import net.imagini.aim.segment.AimSegmentQuickSort
import java.io.IOException
import net.imagini.aim.utils.BlockStorageLZ4
import grizzled.slf4j.Logger
import java.io.InputStreamReader
import java.io.BufferedReader
import net.imagini.aim.types.TypeUtils
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.client.Loader
import net.imagini.aim.tools.StreamUtils
import net.imagini.aim.types.AimQueryException
import net.imagini.aim.utils.View
import java.nio.ByteBuffer

class AimNodeLoaderSession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {

  val keyspace = pipe.readHeader
  val table = pipe.readHeader
  val partition = node.regions(keyspace+"." + table)
  val separator = pipe.readHeader
  val schema = partition.schema
  val keyType = schema.get(0)
  pipe.writeHeader(schema.toString)
  log.info("LOADING INTO " + keyspace + "." + table + " " + schema.toString)

  val startTime = System.currentTimeMillis
  val COLUMN_BUFFER_SIZE = 2048
  val record = ByteBuffer.allocate(COLUMN_BUFFER_SIZE * schema.size)
  val recordView = new View(record)
  var localSegment = partition.createNewSegment
  var count = 0

  override def accept = {
    try {
      pipe.protocol match {
        case Protocol.LOADER_USER     ⇒ loadUnparsedStream
        case Protocol.LOADER_INTERNAL ⇒ loadPartitionedStream
        case _                        ⇒ throw new AimQueryException("Invalid loader protocol " + pipe.protocol);
      }
    } finally {
      partition.add(localSegment)
      log.info("load(EOF) records: " + count + " time(ms): " + (System.currentTimeMillis - startTime) + ", total:" + partition.getCount)
      pipe.writeInt(count)
      pipe.flush
    }
  }

  private def loadPartitionedStream = {
    val in = pipe.getInputStream
    while (true) {
      record.rewind
      for (t ← schema.fields) StreamUtils.read(in, t.getDataType, record)
      record.rewind
      localSegment = partition.appendRecord(localSegment, record)
      count += 1
    }
  }

  private def loadUnparsedStream = {
    val peerLoaders: Map[Int, Loader] = node.peers.map(peer ⇒ {
      peer._1 -> new Loader(peer._2.getHost, peer._2.getPort, Protocol.LOADER_INTERNAL, keyspace, table, "", null, false)
    })
    try {
      val source = scala.io.Source.fromInputStream(pipe.getInputStream)
      val lines = source.getLines
      val values: Array[String] = new Array[String](schema.size)
      var line: String = ""
      while (true) {
        record.clear
        var fields: Int = 0
        while (fields < schema.size) {
          if (!lines.hasNext) {
            throw new EOFException
          } else {
            val line = lines.next
            for (value ← line.split(separator)) {
              values(fields) = value
              fields += 1
            }
          }
        }
        try {
          for ((t, value) ← (schema.fields zip values)) {
            val bytes = t.convert(value)
            TypeUtils.copy(bytes , 0, t.getDataType, record)
          }
          record.flip
          val targetNode = keyType.partition(recordView, node.expectedNumNodes) + 1
          if (targetNode == node.id) {
            localSegment = partition.appendRecord(localSegment, record)
            count += 1
          } else {
            StreamUtils.write(record, peerLoaders(targetNode).pipe.getOutputStream)
          }
        } catch {
          case e: IOException ⇒ {
            log.error(count + ":" + values, e);
            throw e
          }
          case e: Exception ⇒ {
            log.error(count + ":" + values, e);
          }
        }
      }
    } catch {
      case e: EOFException ⇒ {
        peerLoaders.values.map(peer ⇒ count += peer.ackLoadedCount)
        throw e;
      }
    }
  }
}