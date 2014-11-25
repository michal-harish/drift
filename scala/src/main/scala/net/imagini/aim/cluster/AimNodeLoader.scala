package net.imagini.aim.cluster

import java.util.concurrent.TimeUnit
import scala.Array.canBuildFrom
import grizzled.slf4j.Logger
import net.imagini.aim.client.DriftLoader
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.utils.View
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import net.imagini.aim.region.AimRegion
import java.net.URI
import net.imagini.aim.types.AimSchema
import java.io.EOFException
import java.io.InputStream
import java.net.Socket
import java.net.InetAddress
import scala.collection.mutable.LinkedList
import scala.collection.mutable.Queue
import net.imagini.aim.utils.CSVStreamParser
import net.imagini.aim.utils.ByteUtils
import java.util.concurrent.locks.ReentrantLock

class AimNodeLoader(val manager: DriftManager, val keyspace: String, val table: String) {
  val log = Logger[this.type]
  val totalNodes = manager.expectedNumNodes
  val descriptor = manager.getDescriptor(keyspace, table)
  val workers = manager.getNodeConnectors.map(c ⇒ c._1 -> new AimNodeLoaderWorker(c._1, keyspace, table, c._2)).toMap
  //val executor = Executors.newFixedThreadPool(workers.size)
  //workers.values.foreach(executor.submit(_))

  def loadUnparsedStream(in: InputStream, separator: Char): Long = {
    try {
      val csv = new CSVStreamParser(in, separator)
      val values: Array[String] = new Array[String](descriptor.schema.size)
      val totalNodes = manager.expectedNumNodes
      while (!manager.clusterIsSuspended) {
        var f = 0;
        while (f < descriptor.schema.size) {
          values(f) = csv.nextValue
          f += 1
        }
        try {
          insert(values: _*)
        } catch {
          case e: Exception ⇒ {
            log.error(values.mkString(","), e);
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

  def insert(record: String*) {
    try {
      var f = 0
      val recordView = new Array[View](descriptor.schema.size)
      while (f < descriptor.schema.size) {
        recordView(f) = new View(descriptor.schema.get(f).convert(record(f)))
        f += 1
      }
      val targetNode = descriptor.keyType.partition(recordView(0), totalNodes) + 1
      workers(targetNode).process(recordView)
    } catch {
      case e: NumberFormatException ⇒ log.warn(e)
    }
  }

  def insert(record: Array[View]) {
    val targetNode = descriptor.keyType.partition(record(0), totalNodes) + 1
    workers(targetNode).process(record)
  }

  def finish: Long = {
    try {
      //      executor.shutdown
      val totalLoadedCount = workers.values.map(_.finish).fold(0L)(_ + _)
      //      executor.awaitTermination(10, TimeUnit.SECONDS)
      totalLoadedCount
    } catch {
      case e: Throwable ⇒ {
        e.printStackTrace
        throw e
      }
    }
  }

}

class AimNodeLoaderWorker(
  val workerNodeId: Int,
  val keyspace: String,
  val table: String,
  val nodeURI: URI) extends Runnable {
  private var incomingBuffer = new Array[Byte](65535)
  private var incomingBufferPosition = 0
  //  private var outgoingBuffer: Array[Byte] = new Array[Byte](65535)
  //  private var outgoingBufferLimit = 0
  val socket = new Socket(InetAddress.getByName(nodeURI.getHost), nodeURI.getPort)
  val pipe = Pipe.newLZ4Pipe(socket, Protocol.LOADER_INTERNAL)
  //handshake with the loader session handler
  pipe.writeHeader(keyspace)
  pipe.writeHeader(table)
  pipe.writeHeader("N/A")
  val schemaDeclaration = pipe.readHeader
  val schema = AimSchema.fromString(schemaDeclaration)
  //  val lock = new ReentrantLock
  @volatile private var finished = false

  private def sendBuffer(buffer: Array[Byte], limit: Int) {
    pipe.getOutputStream.write(buffer, 0, limit)
  }

  def process(record: Array[View]) = {
    var c = 0; while (c < record.length) {
      val view = record(c)
      val len = view.limit - view.offset + 1
      if (incomingBufferPosition + len >= incomingBuffer.length) {
        //        swapBuffers
        sendBuffer(incomingBuffer, incomingBufferPosition)
        incomingBufferPosition = 0
      }
      ByteUtils.copy(view.array, view.offset, incomingBuffer, incomingBufferPosition, len)
      incomingBufferPosition += len
      c += 1
    }

  }

  def finish: Long = {
    //    lock.lock
    //    try {
    //      finished = true
    //      swapBuffers
    //      flushOutgoingBuffer
    //      pipe.flush
    //      pipe.finishOutput
    //    } finally {
    //      lock.unlock
    //    }
    sendBuffer(incomingBuffer, incomingBufferPosition)
    pipe.flush
    pipe.finishOutput
    val loadedCount: Int = pipe.readInt
    pipe.close
    loadedCount
  }

  //  private def flushOutgoingBuffer = {
  //    lock.lock
  //    try {
  //      if (outgoingBufferLimit > 0) {
  //        pipe.getOutputStream.write(outgoingBuffer, 0, outgoingBufferLimit)
  //        outgoingBufferLimit = 0
  //      }
  //    } finally {
  //      lock.unlock
  //    }
  //  }

  //  private def swapBuffers = {
  //    lock.lock
  //    try {
  //      val swapBuffer = outgoingBuffer
  //      outgoingBuffer = incomingBuffer
  //      outgoingBufferLimit = incomingBufferPosition
  //      incomingBuffer = swapBuffer
  //      incomingBufferPosition = 0
  //      lock.synchronized {
  //        lock.notify
  //      }
  //    } finally {
  //      lock.unlock
  //    }
  //  }

  override def run = {
    //    while (!finished) {
    //      if (outgoingBufferLimit > 0) {
    //        flushOutgoingBuffer
    //      } else lock.synchronized {
    //        lock.wait
    //      }
    //    }
  }

}