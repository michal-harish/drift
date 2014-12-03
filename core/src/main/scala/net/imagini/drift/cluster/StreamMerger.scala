package net.imagini.drift.cluster

import java.io.EOFException
import java.io.InputStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.LinkedBlockingQueue
import net.imagini.drift.utils.ByteKey
import net.imagini.drift.types.AimSchema
import net.imagini.drift.types.SortOrder
import net.imagini.drift.types.SortOrder._
import scala.Array.canBuildFrom
import net.imagini.drift.types.TypeUtils
import scala.Array.fallbackCanBuildFrom
import java.util.TreeMap

/**
 * Concurrent streaming merge tool which executes the filter over each input stream
 * in parallel. It uses  LinkedBlockingQueue for each individual input stream and ConcurrentSkipListMap
 * to for the output which automatically does the merge sort. These 2 structures keep
 * each other in balance, since ConcurrentSkipListMap has a O(log(n)) insertion complexity, if the
 * queues advance too fast compared to client consumption, they will get slower and slower. In case the
 * client is a very slow filter, the queueSize applied to each input stream queue is a safety net.
 *
 */

class StreamMerger(val schema: AimSchema, val queueSize: Int, val inputStreams: Array[InputStream]) extends InputStream {

  //TODO this must become part of the schema including key, but it must be easy to create variants of schemas with switching key columns and sort order
  val sortOrder = SortOrder.ASC
  val keyColumn: Int = 0
  val keyField: String = schema.name(keyColumn)
  val keyType = schema.get(0)
  val sortQueue = new TreeMap[ByteKey, (Int, Array[Array[Byte]])]

  val executor: ExecutorService = Executors.newFixedThreadPool(inputStreams.length)
  val fetchers: Seq[Fetcher] = inputStreams.map(in ⇒ new Fetcher(schema, in, executor, queueSize))

  override def read: Int = if (position >= 0 && checkNextByte) record(field)(position) & 0xff else -1
  private var record: Array[Array[Byte]] = null
  private var field = schema.size - 1
  private var position = 0
  var currentKey: Array[Byte] = null
  private def checkNextByte: Boolean = {
    try {
      position += 1
      if (record == null || position == record(field).length) {
        field += 1
        position = 0
        if (field == schema.size) {
          record = nextRecord
          field = 0
        }
      }
      true
    } catch {
      case (e: EOFException) ⇒ position = -1; false
    }
  }

  private def takeNext(f: Int) {
    val fetcher = fetchers(f)
    if (!fetcher.closed) fetcher.take match {
      case None ⇒ {}
      case Some(record) ⇒ {
        val byteKey = new ByteKey(record(0), 0, TypeUtils.sizeOf(schema.get(0), record(0)), f)
        /**
         * FIXME assuming that the first field of schema is always the key - e.g. whatever provides
         * the underlying input stream must provide or transform the first field as the key
         */
        sortQueue.put(byteKey, (f, record))
      }
    }

  }
  private def nextRecord: Array[Array[Byte]] = {
    if (currentKey == null) {
      var f = 0
      while (f < fetchers.size) {
        takeNext(f)
        f += 1
      }
    }

    val entry = sortOrder match {
      case DESC ⇒ sortQueue.pollLastEntry
      case ASC  ⇒ sortQueue.pollFirstEntry
    }
    if (entry == null) throw new EOFException else {
      val tuple = entry.getValue
      currentKey = tuple._2(0)
      takeNext(tuple._1)
      tuple._2
    }
  }

  override def close = inputStreams.foreach(_.close)

}

class Fetcher(val schema: AimSchema, val in: InputStream, val executor: ExecutorService, val queueSize: Int) extends Runnable {
  @volatile private var hasMoreData = true
  def closed: Boolean = !hasMoreData && ready.size == 0
  private val ready = new LinkedBlockingQueue[Option[Array[Array[Byte]]]](queueSize)
  executor.submit(this)

  override def run = {
    while (hasMoreData) try {
      //TODO optimise as buffered read instead of byte allocation for each read 
      val record = schema.fields.map(t ⇒ StreamUtils.read(in, t))
      ready.put(Some(record))
    } catch {
      case e: EOFException ⇒ hasMoreData = false; ready.offer(None)
    }
  }
  def take: Option[Array[Array[Byte]]] = ready.take

}
