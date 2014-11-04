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

class AimNodeLoaderSession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {
  val keyspace = pipe.read
  val table = pipe.read
  val partition = node.keyspace(keyspace)(table)
  val separator = pipe.read
  log.info("LOADING INTO " + keyspace + "." + table + " " + partition.schema.toString)
  pipe.write(partition.schema.toString)
  pipe.flush

  val startTime = System.currentTimeMillis
  private var count: Integer = 0
  private var currentSegment: AimSegment = null
  /* Zero-copy support
     * TODO COLUMN_BUFFER_SIZE should be configurable for different schemas
     */
  private val COLUMN_BUFFER_SIZE = 2048
  private val record = ByteUtils.createBuffer(COLUMN_BUFFER_SIZE * partition.schema.size)

  createNewSegmentIfNull

  override def accept = {

    val reader = new InputStreamReader(pipe.getInputStream)
    try {
      val lineReader = new BufferedReader(reader)
      val values: Array[String] = new Array[String](partition.schema.size)
      var line: String = ""
      while (true) {
        record.clear
        var fields: Int = 0
        while (fields < partition.schema.size) {
          val line = lineReader.readLine
          if (line == null) {
            throw new EOFException
          } else {
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
          currentSegment.appendRecord(record);
          count += 1

          if (currentSegment.getOriginalSize > partition.segmentSizeBytes) {
            addCurrentSegment
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

    } finally {
      addCurrentSegment
      log.info("load(EOF) records: " + count + " time(ms): " + (System.currentTimeMillis - startTime))
      pipe.writeInt(count)
      pipe.flush
      reader.close
    }
  }

  private def addCurrentSegment = {
    try {
      currentSegment.close
      if (currentSegment.getOriginalSize > 0) {
        partition.add(currentSegment);
        currentSegment = null
      }
      createNewSegmentIfNull
    } catch {
      case e: Throwable ⇒ {
        throw new IOException(e);
      }
    }
  }

  private def createNewSegmentIfNull = {
    if (currentSegment == null) {
      currentSegment = new AimSegmentQuickSort(partition.schema, classOf[BlockStorageLZ4]);
      //TODO enalbe once schema contains keyField and sort order
      //            if (partition.keyField == null) {
      //                currentSegment = new AimSegmentUnsorted(partition.schema(), BlockStorageLZ4.class);
      //            } else {
      //                currentSegment = new AimSegmentQuickSort(partition.schema(), partition.keyField(), partition.sortOrder, BlockStorageLZ4.class);
      //            }
    }
  }

}