package net.imagini.aim.cluster

import java.io.EOFException
import java.io.InputStream
import scala.collection.mutable.HashMap
import scala.collection.mutable.MultiMap
import net.imagini.aim.types.AimQueryException
import net.imagini.aim.utils.ByteKey
import net.imagini.aim.utils.View
import java.util.Collections
import net.imagini.aim.types.SortOrder

class AimNodeLoaderSession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {

  val keyspace = pipe.readHeader
  val table = pipe.readHeader
  val region = node.regions(keyspace + "." + table)
  val separator: Char = pipe.readHeader()(0)
  val schema = region.schema
  val keyType = schema.get(0)
  pipe.writeHeader(schema.toString)

  log.info("LOADING INTO " + keyspace + "." + table + " " + schema.toString)
  private val startTime = System.currentTimeMillis
  private var count: Long = 0
  private val loadBuffer = new Array[Byte](region.segmentSizeBytes + 65535)
  private val loadView = new View(loadBuffer)
  private val recordList = new java.util.ArrayList[View]

  override def accept = {
    try {
      pipe.protocol match {
        case Protocol.LOADER_USER     ⇒ loadUnparsedStream
        case Protocol.LOADER_INTERNAL ⇒ loadPartitionedStream
        case _                        ⇒ throw new AimQueryException("Invalid loader protocol " + pipe.protocol);
      }
    } catch {
      case e: EOFException ⇒ {}
      case e: Throwable    ⇒ log.error("!", e)
    } finally {
      region.compact
      log.info(Protocol.LOADER_USER + "/EOF records: " + count + " time(ms): " + (System.currentTimeMillis - startTime))
      pipe.writeInt(count.toInt)
      pipe.flush
      throw new EOFException
    }
  }

  private def loadUnparsedStream = {
    val loader = new AimNodeLoader(node.manager, keyspace, table)
    count = loader.loadUnparsedStream(pipe.getInputStream, separator)
  }

  private def loadPartitionedStream = {
    val in = pipe.getInputStream
    try {
      while (!node.isSuspended) {
        loadRecord(in)
        if (loadView.offset >= region.segmentSizeBytes) {
          submitSegmentToRegion
        }
        count += 1
      }
    } catch {
      case e: EOFException ⇒ {
        submitSegmentToRegion
        throw e
      }
    }
  }

  private def loadRecord(in: InputStream) = {
    //TODO - maybe more optimisation, too many views created 
    val recordView = new View(loadView)
    var c = 0; while (c < schema.size) {
      val valueOffset = loadView.offset
      val valueSize = StreamUtils.read(in, schema.get(c), loadView)
      recordView.limit = valueOffset
      c += 1
    }
    recordList.add(recordView)
  }

  private def submitSegmentToRegion() {
    try {
      region.addRecords(recordList)
    } finally {
      loadView.limit = loadView.size - 1
      loadView.offset = 0
      recordList.clear
    }
  }

}