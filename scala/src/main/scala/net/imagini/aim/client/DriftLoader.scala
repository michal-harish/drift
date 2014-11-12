package net.imagini.aim.client

import java.io.FileInputStream
import java.io.InputStream
import java.net.InetAddress
import java.net.Socket
import java.util.zip.GZIPOutputStream
import grizzled.slf4j.Logger
import net.imagini.aim.cluster.Pipe
import net.imagini.aim.cluster.Protocol
import net.imagini.aim.types.AimSchema
import java.nio.ByteBuffer
import net.imagini.aim.tools.StreamUtils

object DriftLoader extends App {
  var host: String = "localhost"
  var port: Int = 4000
  var keyspace: String = null
  var table: String = null
  var separator: String = "\n"
  var gzip = false
  var file: Option[String] = None
  val argsIterator = args.iterator
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--host"      ⇒ host = argsIterator.next
      case "--port"      ⇒ port = argsIterator.next.toInt
      case "--keyspace"  ⇒ keyspace = argsIterator.next
      case "--table"     ⇒ table = argsIterator.next
      case "--separator" ⇒ separator = argsIterator.next
      case "--file"      ⇒ file = Some(argsIterator.next)
      case "--gzip"      ⇒ gzip = true
      case arg: String   ⇒ println("Unknown argument " + arg)
    }
  }
  val loader = file match {
    case None           ⇒ new DriftLoader(host, port, Protocol.LOADER_USER, keyspace, table, separator, null, gzip)
    case Some(filename) ⇒ new DriftLoader(host, port, Protocol.LOADER_USER, keyspace, table, separator, new FileInputStream(filename), gzip)
  }
  loader.streamInput
}

class DriftLoader(host: String, port: Int, protocol: Protocol,
  val keyspace: String,
  val table: String,
  val separator: String,
  val fileinput: InputStream,
  val gzip: Boolean) {

  val log = Logger[this.type]
  val in: InputStream = if (fileinput == null) System.in else fileinput
  val socket = new Socket(InetAddress.getByName(host), port)
  val pipe = new Pipe(socket, protocol, if (gzip) 3 else 1) // 1-LZ4 2-GZIP 3-WRAPPED-GZIP

  //handshake
  pipe.writeHeader(keyspace)
  pipe.writeHeader(table)
  pipe.writeHeader(separator)
  val schemaDeclaration = pipe.readHeader
  val schema = AimSchema.fromString(schemaDeclaration)

  def streamInput: Int = {
    StreamUtils.copy(in, pipe.getOutputStream)
    ackLoadedCount
  }

  def ackLoadedCount: Int = {
    pipe.flush
    if (!gzip) {
      pipe.finishOutput
    }
    val loadedCount: Int = pipe.readInt
    pipe.close
    loadedCount
  }
}