package net.imagini.aim.tools

import java.io.EOFException
import java.io.InputStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ConcurrentSkipListMap
import net.imagini.aim.utils.ByteKey
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder
import net.imagini.aim.types.SortOrder._
import scala.Array.canBuildFrom
import net.imagini.aim.types.TypeUtils

class MergeQueue extends ConcurrentSkipListMap[ByteKey, Array[Array[Byte]]]
class InputStreamQueue(limit: Int) extends LinkedBlockingQueue[Option[Array[Array[Byte]]]](limit)

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
  val sortQueue = new MergeQueue

  val executor: ExecutorService = Executors.newFixedThreadPool(inputStreams.length)
  val fetchers: Seq[Fetcher] = inputStreams.map(in ⇒ new Fetcher(schema, in, executor, queueSize))
  val counter = new AtomicInteger(0)

  override def read: Int = if (position >= 0 && checkNextByte) record(field)(position) & 0xff else -1
  private var record: Array[Array[Byte]] = null
  private var field = schema.size - 1
  private var position = 0
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

  private def nextRecord: Array[Array[Byte]] = {
    //TODO optimise foreach
    fetchers.filter(!_.closed).foreach(fetcher ⇒ {
      fetcher.take match {
        case None ⇒ {}
        case Some(record) ⇒ {
          val byteKey = new ByteKey(record(0), 0, TypeUtils.sizeOf(schema.dataType(0), record(0)), counter.incrementAndGet)
          /**
           * FIXME assuming that the first field of schema is always the key - e.g. whatever provides
           * the underlying input stream must provide or transform the first field as the key
           */
          sortQueue.put(byteKey, record)
        }
      }
    })

    val entry = sortOrder match {
      case DESC ⇒ sortQueue.pollLastEntry
      case ASC  ⇒ sortQueue.pollFirstEntry
    }
    if (entry == null) throw new EOFException else {
      entry.getValue
    }
  }

  override def close = inputStreams.foreach(_.close)

}

class Fetcher(val schema: AimSchema, val in: InputStream, val executor: ExecutorService, val queueSize: Int) extends Runnable {
  val keyDataType = schema.dataType(0)
  @volatile private var hasMoreData = true
  def closed: Boolean = !hasMoreData && ready.size == 0
  private val ready = new InputStreamQueue(queueSize)
  executor.submit(this)

  override def run = {
    while (hasMoreData) try {
      //TODO optimise this map into loop
      val record = schema.fields.map(t ⇒ StreamUtils.read(in, t.getDataType))
      ready.put(Some(record))
    } catch {
      case e: EOFException ⇒ hasMoreData = false; ready.offer(None)
    }
  }
  def take: Option[Array[Array[Byte]]] = ready.take

}
