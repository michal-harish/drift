package net.imagini.aim.tools

import java.io.EOFException
import java.io.InputStream
import java.util.TreeMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import net.imagini.aim.types.AimDataType
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.ByteKey
import java.util.concurrent.Callable
import java.util.concurrent.Future
import net.imagini.aim.types.SortOrder
import net.imagini.aim.types.SortOrder._

class MergeQueue extends TreeMap[ByteKey, Array[Array[Byte]]]

class StreamMerger(val schema: AimSchema, val inputStreams: Seq[InputStream]) extends InputStream {

  //TODO this must become part of the schema including key, but it must be easy to create variants of schemas with switching key columns and sort order
  val sortOrder = SortOrder.ASC
  val keyColumn: Int = 0
  val keyField: String = schema.name(keyColumn)
  val keyType = schema.get(0)
  val sortQueue = new MergeQueue

  val executor: ExecutorService = Executors.newFixedThreadPool(inputStreams.length)
  val fetchers: Seq[Fetcher] = inputStreams.map(in ⇒ new Fetcher(schema, in, executor))

  var record: Array[Array[Byte]] = null
  var field = schema.size - 1
  var position = -1
  override def read: Int = if (checkNextByte) record(field)(position) & 0xff else -1
  private def checkNextByte: Boolean = {
    try {
      position += 1
      if (record == null || position == record(field).length) {
        field += 1
        if (field == schema.size) {
          //FIXME this doesn't pass-on the meta-bytes
          record = nextRecord
          field = 0
          position = 0
        }
      }
      true
    } catch {
      case (e: EOFException) ⇒ false
    }

  }
  private def nextRecord: Array[Array[Byte]] = {
    for (fetcher ← fetchers) if (!fetcher.inSync) {
      fetcher.next match {
        case Some(record) ⇒ {
          val byteKey = new ByteKey(record(0), fetcher.hashCode)
          /**
           * assuming that the first field of schema is always the key - e.g. whatever provides
           * the underlying input stream must provide or transform the first field as the key
           * - this also optimizes towards zero-copy
           */
          sortQueue.put(byteKey, record)
        }
        case None ⇒ {}
      }
    }
    val entry = sortOrder match {
      case DESC ⇒ sortQueue.pollLastEntry
      case ASC  ⇒ sortQueue.pollFirstEntry
    }
    if (entry == null) throw new EOFException else entry.getValue
  }

  override def close = inputStreams.foreach(_.close)

}

class Fetcher(val schema: AimSchema, val in: InputStream, val executor: ExecutorService) extends Callable[Option[Array[Array[Byte]]]] {
  val keyDataType = schema.dataType(0)
  @volatile private var hasMoreData = true
  @volatile private var future = executor.submit(this)
  def inSync: Boolean = !hasMoreData || future == null

  override def call: Option[Array[Array[Byte]]] = if (!hasMoreData) None else try {
    val record = schema.fields.map(t ⇒ PipeUtils.read(in, t.getDataType))
    Some(record)
  } catch {
    case e: EOFException ⇒ hasMoreData = false; future = null; None
  }

  def next: Option[Array[Array[Byte]]] = {
    val recordMaybe = future.get
    future = executor.submit(this)
    recordMaybe
  }

}
