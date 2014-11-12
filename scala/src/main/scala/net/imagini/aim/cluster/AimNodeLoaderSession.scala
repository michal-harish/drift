package net.imagini.aim.cluster

import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.segment.AimSegment
import java.io.EOFException
import net.imagini.aim.segment.AimSegmentQuickSort
import java.io.IOException
import net.imagini.aim.utils.BlockStorageMEMLZ4
import grizzled.slf4j.Logger
import java.io.InputStreamReader
import java.io.BufferedReader
import net.imagini.aim.types.TypeUtils
import net.imagini.aim.region.AimRegion
import net.imagini.aim.client.DriftLoader
import net.imagini.aim.tools.StreamUtils
import net.imagini.aim.types.AimQueryException
import net.imagini.aim.utils.View
import java.nio.ByteBuffer

class AimNodeLoaderSession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {

  val keyspace = pipe.readHeader
  val table = pipe.readHeader
  val region = node.regions(keyspace + "." + table)
  val separator = pipe.readHeader
  val schema = region.schema
  val keyType = schema.get(0)
  pipe.writeHeader(schema.toString)

  log.info("LOADING INTO " + keyspace + "." + table + " " + schema.toString)
  private val startTime = System.currentTimeMillis
  private var localSegment = region.createNewSegment
  private var count: Long = 0

  override def accept = {
    try {
      pipe.protocol match {
        case Protocol.LOADER_USER     ⇒ loadUnparsedStream
        case Protocol.LOADER_INTERNAL ⇒ loadPartitionedStream
        case _                        ⇒ throw new AimQueryException("Invalid loader protocol " + pipe.protocol);
      }
    } finally {
      region.add(localSegment)
      log.info(Protocol.LOADER_USER + "/EOF records: " + count + " time(ms): " + (System.currentTimeMillis - startTime))
      pipe.writeInt(count.toInt)
      pipe.flush
    }
  }

  private def loadPartitionedStream = {
    val in = pipe.getInputStream
    while (!node.isSuspended) {
      val record = new Array[Array[Byte]](schema.size)
      var c = 0; while (c < schema.size) {
        record(c) = StreamUtils.read(in, schema.dataType(c))
        c += 1
      }
      localSegment = region.appendRecord(localSegment, record)
      count += 1
    }
  }

  private def loadUnparsedStream = {
    val loader = new AimNodeLoader(keyspace, table, node)
    try {
      val source = scala.io.Source.fromInputStream(pipe.getInputStream)
      val lines = source.getLines
      val values: Array[String] = new Array[String](schema.size)
      var line: String = ""
      val totalNodes = node.manager.expectedNumNodes
      while (!node.isSuspended) {
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
          loader.insert(values: _*)
        } catch {
          case e: Exception ⇒ {
            log.error(count + ":" + values.mkString(","), e);
            throw e
          }
        }
      }
    } catch {
      case e: EOFException ⇒ {
        count += loader.finish
        throw e;
      }
    }
  }
}