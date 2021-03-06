package net.imagini.drift.cluster

import java.util.concurrent.TimeUnit
import scala.Array.canBuildFrom
import grizzled.slf4j.Logger
import net.imagini.drift.client.DriftLoader
import net.imagini.drift.segment.DriftSegment
import net.imagini.drift.utils.View
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import net.imagini.drift.region.DriftRegion
import java.net.URI
import net.imagini.drift.types.DriftSchema
import java.io.EOFException
import java.io.InputStream
import java.net.Socket
import java.net.InetAddress
import scala.collection.mutable.LinkedList
import scala.collection.mutable.Queue
import net.imagini.drift.utils.CSVStreamParser
import net.imagini.drift.utils.ByteUtils
import java.util.concurrent.locks.ReentrantLock
import net.imagini.drift.types.DriftTableDescriptor

class DriftNodeLoader(val manager: DriftManager, val keyspace: String, val table: String) {
  val log = Logger[this.type]
  val totalNodes = manager.expectedNumNodes
  val descriptor = manager.getDescriptor(keyspace, table)
  val workers = manager.getNodeConnectors.map(c ⇒ c._1 -> new DriftNodeLoaderWorker(c._1, keyspace, table, c._2)).toMap
  val executor = Executors.newFixedThreadPool(workers.size)
  workers.values.foreach(executor.submit(_))

  def loadUnparsedStream(in: InputStream, separator: Char): Long = {
    try {
      val csv = new CSVStreamParser(in, separator)
      val parseBuffer = new Array[Byte](65535)
      val record: Array[View] = descriptor.schema.fields.map(field ⇒ new View(parseBuffer))
      val totalNodes = manager.expectedNumNodes
      var lineNumber = 0
      while (!manager.clusterIsSuspended) {
        var f = 0
        var parserBufferPosition = 0
        var recordWithIllegalArgument = false
        lineNumber += 1
        try {
          while (f < descriptor.schema.size) {
            val field = descriptor.schema.get(f)
            record(f).offset = parserBufferPosition
            val valueToParse = csv.nextValue
            try {
              val parsedLength = field.parse(valueToParse, parseBuffer, parserBufferPosition)
              record(f).limit = parserBufferPosition + parsedLength - 1
              parserBufferPosition += parsedLength
              f += 1
            } catch {
              case e: IllegalArgumentException ⇒
                {
                  recordWithIllegalArgument = true
                  if (valueToParse.limit - valueToParse.offset + 1 < 3) {
                    log.error(valueToParse.offset + "::" + valueToParse.limit)
                  }
                  log.warn(lineNumber + ": " + field + " " + new String(valueToParse.array, valueToParse.offset, valueToParse.limit - valueToParse.offset + 1) + " " + e)
                }
                throw e
            }
          }
        } catch {
          case e: IllegalArgumentException ⇒ csv.skipLine
        }
        if (!recordWithIllegalArgument) try {
          insert(record)
        } catch {
          case e: Exception ⇒ {
            log.error(e)
            throw e
          }
        }
      }
    } catch {
      case e: EOFException ⇒ {
        return finish
      }
    }
    0L
  }

  def insert(record: Array[View]) {
    val targetNode = descriptor.keyType.partition(record(0), totalNodes) + 1
    workers(targetNode).process(record)
  }
  def finish: Long = {
    try {
      workers.values.map(_.finish)
      executor.shutdown
      executor.awaitTermination(100, TimeUnit.SECONDS)
      val totalLoadedCount = workers.values.map(_.ackLoadedCount).fold(0L)(_ + _)
      totalLoadedCount
    } catch {
      case e: Throwable ⇒ {
        e.printStackTrace
        throw e
      }
    }
  }

}

class DriftNodeLoaderWorker(
  val workerNodeId: Int,
  val keyspace: String,
  val table: String,
  val nodeURI: URI) extends Runnable {
  private var incomingBuffer = new Array[Byte](32767)
  private var incomingBufferPosition = 0
  private var outgoingBuffer: Array[Byte] = new Array[Byte](incomingBuffer.length)
  private var outgoingBufferLimit = 0
  val socket = new Socket(InetAddress.getByName(nodeURI.getHost), nodeURI.getPort)
  val pipe = Pipe.newLZ4Pipe(socket, Protocol.LOADER_INTERNAL)
  //handshake with the loader session handler
  pipe.writeHeader(keyspace)
  pipe.writeHeader(table)
  pipe.writeHeader("N/A")
  val schemaDeclaration = pipe.readHeader
  val schema = DriftSchema.fromString(schemaDeclaration)

  def process(record: Array[View]) = {
    var c = 0; while (c < record.length) {
      val view = record(c)
      val len = view.limit - view.offset + 1
      if (incomingBufferPosition + len >= incomingBuffer.length) {
        swapBuffers(false)
      }
      if (len > incomingBuffer.length) {
        throw new NotImplementedError("Value larger than loader buffer not allowed by this implementation")
      }
      ByteUtils.copy(view.array, view.offset, incomingBuffer, incomingBufferPosition, len)
      incomingBufferPosition += len
      c += 1
    }
  }

  def finish = swapBuffers(true)

  private def swapBuffers(finished: Boolean) = {
    synchronized {
      while (outgoingBufferLimit > 0) {
        wait
      }
      val swapBuffer = outgoingBuffer
      outgoingBuffer = incomingBuffer
      outgoingBufferLimit = incomingBufferPosition
      incomingBuffer = if (finished) null else swapBuffer
      incomingBufferPosition = if (finished) -1 else 0
      notify
    }
  }

  override def run = {
    do {
      synchronized {
        if (outgoingBufferLimit > 0) {
          pipe.getOutputStream.write(outgoingBuffer, 0, outgoingBufferLimit)
          outgoingBufferLimit = 0
          notify
        } else {
          wait
        }
      }
    } while (incomingBufferPosition >= 0 || outgoingBufferLimit > 0)

    pipe.flush
    pipe.finishOutput
  }

  def ackLoadedCount: Long = {
    val loadedCount: Int = pipe.readInt
    pipe.close
    loadedCount
  }

}