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
import java.nio.ByteBuffer

class AimNodeLoaderSession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {
  private var count: Integer = 0
  private var currentSegment: AimSegment = null

  //TODO optimized parsing
  //  var position = -1
  //  var limit = -1
  //  var mark = -1
  //  val buf = new Array[Byte](1000)
  //  private def readColumn: String = {
  //    var result: String = ""
  //    while (true) {
  //      position += 1
  //      if (position >= limit) {
  //        if (mark >= 0) {
  //          result += new String(buf, 0, limit)
  //        }
  //        position = 0
  //        mark = 0
  //        limit = pipe.getInputStream.read(buf, 0, buf.length)
  //      }
  //      if (limit == -1) {
  //        return result;
  //      }
  //    }
  //    throw new EOFException
  //  }
  override def accept = {
    val keyspace = pipe.readHeader
    val table = pipe.readHeader
    val partition = node.keyspace(keyspace)(table)
    val separator = pipe.readHeader
    pipe.writeHeader(partition.schema.toString)
    log.info("LOADING INTO " + keyspace + "." + table + " " + partition.schema.toString)
    val startTime = System.currentTimeMillis
    val COLUMN_BUFFER_SIZE = 2048
    val record = ByteUtils.createBuffer(COLUMN_BUFFER_SIZE * partition.schema.size)
    var segment = partition.createNewSegment
    val source = scala.io.Source.fromInputStream(pipe.getInputStream)
    val lines = source.getLines
    try {
      val values: Array[String] = new Array[String](partition.schema.size)
      var line: String = ""
      while (true) {
        record.clear
        var fields: Int = 0
        while (fields < partition.schema.size) {
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
          for ((t, value) ← (partition.schema.fields zip values)) {
            val bytes = t.convert(value)
            TypeUtils.copy(bytes, t.getDataType, record)
          }
          record.flip
          segment = partition.appendRecord(segment, record)
          count += 1
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
    } finally {
      partition.add(segment)
      log.info("load(EOF) records: " + count + " time(ms): " + (System.currentTimeMillis - startTime))
      pipe.writeInt(count)
      pipe.flush
    }
  }

  

}