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

object Loader extends App {
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
    case None           ⇒ new Loader(host, port, Protocol.LOADER_USER, keyspace, table, separator, null, gzip)
    case Some(filename) ⇒ new Loader(host, port, Protocol.LOADER_USER, keyspace, table, separator, new FileInputStream(filename), gzip)
  }
  loader.streamInput
}

class Loader(host: String, port: Int, protocol: Protocol, 
    val keyspace: String, 
    val table: String, 
    val separator: String, 
    val fileinput: InputStream, 
    val gzip: Boolean
) {

  val log = Logger[this.type]
  val in: InputStream = if (fileinput == null) System.in else fileinput
  val socket = new Socket(InetAddress.getByName(host), port)
  val pipe = new Pipe(socket, protocol, if (gzip) 3 else 2)

  //handshake
  pipe.writeHeader(keyspace)
  pipe.writeHeader(table)
  pipe.writeHeader(separator)
  val schemaDeclaration = pipe.readHeader
  val schema = AimSchema.fromString(schemaDeclaration)

  def streamInput: Int = {
    var count: Long = 0
    var n: Int = 0
    val buffer = new Array[Byte](65535)
    do {
      n = in.read(buffer)
      if (-1 != n) {
        pipe.getOutputStream.write(buffer, 0, n)
        count += n
      }
    } while (-1 != n)
    ackLoadedCount
  }

  def ackLoadedCount: Int = {
    pipe.flush
    if (!gzip) {
      pipe.getOutputStream.asInstanceOf[GZIPOutputStream].finish
    }
    val loadedCount: Int = pipe.readInt
    pipe.close
    loadedCount
  }
}