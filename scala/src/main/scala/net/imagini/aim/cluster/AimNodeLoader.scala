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

class AimNodeLoader(val manager: DriftManager, val keyspace: String, val table: String) {
  val log = Logger[this.type]
  val totalNodes = manager.expectedNumNodes
  val schema = manager.getSchema(keyspace, table)
  val keyType = schema.get(0)
  val workers = manager.getNodeConnectors.map(c ⇒ c._1 -> new AimNodeLoaderWorker(c._1, keyspace, table, c._2)).toMap
  val executor = Executors.newFixedThreadPool(workers.size)
  workers.values.foreach(executor.submit(_))

  def loadUnparsedStream(in: InputStream, separator: String): Long = {
    try {
      val source = scala.io.Source.fromInputStream(in)
      val lines = source.getLines
      val values: Array[String] = new Array[String](schema.size)
      var line: String = ""
      val totalNodes = manager.expectedNumNodes
      while (!manager.clusterIsSuspended) {
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
      insert((schema.fields, record).zipped.map((f, r) ⇒ new View(f.convert(r))))
    } catch {
      case e: NumberFormatException ⇒ log.warn(e)
    }
  }

  def insert(record: Array[View]) {
    val targetNode = keyType.partition(record(0), totalNodes) + 1
    workers(targetNode).process(record)
  }

  def finish: Long = {
    try {
      executor.shutdown
      val totalLoadedCount = workers.values.map(_.finish).fold(0L)(_ + _)
      executor.awaitTermination(10, TimeUnit.SECONDS)
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
  private var incomingQueue = new Queue[Array[View]]
  private var outgoingQueue: Queue[Array[View]] = null
  val socket = new Socket(InetAddress.getByName(nodeURI.getHost), nodeURI.getPort)
  val pipe = Pipe.newLZ4Pipe(socket, Protocol.LOADER_INTERNAL)
  //handshake
  pipe.writeHeader(keyspace)
  pipe.writeHeader(table)
  pipe.writeHeader("N/A")
  val schemaDeclaration = pipe.readHeader
  val schema = AimSchema.fromString(schemaDeclaration)
  val lock = new Object
  @volatile private var finished = false

  def process(record: Array[View]) = {
    if (incomingQueue.size >= 10000) {
      lock.synchronized {
        outgoingQueue = incomingQueue
        incomingQueue = new Queue[Array[View]]
        lock.notify
      }
    }
    incomingQueue.enqueue(record)
  }

  def finish: Long = {
    lock.synchronized {
      finished = true
      outgoingQueue = incomingQueue
      incomingQueue = null
      lock.notify
    }
    val loadedCount: Int = pipe.readInt
    pipe.close
    loadedCount

  }

  override def run = {
    while (!finished) {
      lock.synchronized {
        //TOTO use reentrant lock instead
        lock.wait
        while (!outgoingQueue.isEmpty) {
          val record = outgoingQueue.dequeue
          pipe.getOutputStream.write(1)
          var r = 0; while (r < record.length) {
            val dataType = schema.dataType(r)
            val view = record(r)
            pipe.getOutputStream.write(view.array, view.offset, dataType.sizeOf(view))
            r += 1
          }
        }
      }
    }
    pipe.getOutputStream.write(-1)
    pipe.flush
    pipe.finishOutput
  }

}