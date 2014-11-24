package net.imagini.aim.cluster

import java.io.EOFException

import net.imagini.aim.types.AimQueryException

class AimNodeLoaderSession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {

  val keyspace = pipe.readHeader
  val table = pipe.readHeader
  val region = node.regions(keyspace + "." + table)
  val separator:Char = pipe.readHeader()(0)
  val schema = region.schema
  val keyType = schema.get(0)
  pipe.writeHeader(schema.toString)

  log.info("LOADING INTO " + keyspace + "." + table + " " + schema.toString)
  private val startTime = System.currentTimeMillis
  private var localSegment = region.newSegment
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
      region.compact
      log.info(Protocol.LOADER_USER + "/EOF records: " + count + " time(ms): " + (System.currentTimeMillis - startTime))
      pipe.writeInt(count.toInt)
      pipe.flush
      throw new EOFException
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
    val loader = new AimNodeLoader(node.manager, keyspace, table)
    count = loader.loadUnparsedStream(pipe.getInputStream, separator)
  }
}