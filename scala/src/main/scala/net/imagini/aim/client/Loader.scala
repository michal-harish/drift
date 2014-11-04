package net.imagini.aim.client
import java.io.FileInputStream
import java.io.InputStream
import java.net.InetAddress
import java.net.Socket
import grizzled.slf4j.Logger
import net.imagini.aim.types.AimSchema
import net.imagini.aim.cluster.PipeGZIPLoader
import net.imagini.aim.cluster.Protocol

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
    case None           ⇒ new Loader(host, port, keyspace, table, separator, gzip)
    case Some(filename) ⇒ new Loader(host, port, keyspace, table, separator, filename, gzip)
  }
  loader.streamInput
}

class Loader(host: String, port: Int, val keyspace: String, val table: String, val separator: String, val fileinput: InputStream, val gzip: Boolean) {

  def this(host: String, port: Int, keyspace: String, table: String, separator: String, filename: String, gzip: Boolean) = this(host, port, keyspace, table, separator, if (filename == null) null else new FileInputStream(filename), gzip)

  def this(host: String, port: Int, keyspace: String, table: String, separator: String, gzip: Boolean) = this(host, port, keyspace, table, separator, null.asInstanceOf[InputStream], gzip)

  val log = Logger[this.type]
  val in: InputStream = if (fileinput == null) System.in else fileinput
  val socket = new Socket(InetAddress.getByName(host), port)
  val pipe = new PipeGZIPLoader(socket, Protocol.LOADER_LOCAL)

  //handshake
  pipe.writeDirect(keyspace)
  pipe.writeDirect(table)
  pipe.writeDirect(separator)
  if (gzip) {
    pipe.getDirectOutputStream.finish
  } else {
    pipe.getDirectOutputStream.flush
  }
  val schemaDeclaration = pipe.read
  val schema = AimSchema.fromString(schemaDeclaration)

  def streamInput: Int = {
    val out = if (gzip) pipe.getOutputStream else pipe.getDirectOutputStream
    var count: Long = 0
    var n: Int = 0
    val buffer = new Array[Byte](65535)
    do {
      n = in.read(buffer)
      if (-1 != n) {
        out.write(buffer, 0, n)
        if (gzip) {
          pipe.getOutputStream.flush
        } else {
          pipe.getDirectOutputStream.flush
        }
        count += n
      }
    } while (-1 != n)
    if (!gzip) pipe.getDirectOutputStream.finish
    pipe.getOutputStream().flush
    val loadedCount: Int = pipe.readInt
    pipe.close
    socket.close
    loadedCount
  }

}