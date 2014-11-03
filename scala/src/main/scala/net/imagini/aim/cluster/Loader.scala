package net.imagini.aim.cluster

import java.io.BufferedReader
import java.io.EOFException
import java.io.FileInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.net.InetAddress
import java.net.Socket
import java.util.zip.GZIPInputStream
import scala.Array.canBuildFrom
import grizzled.slf4j.Logger
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType
import java.io.IOException

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
  loader.processInput
  loader.close

}
class Loader(host: String, port: Int, val keyspace: String, val table: String, val separator: String, val fileinput: InputStream, val gzip: Boolean) {

  def this(host: String, port: Int, keyspace: String, table: String, separator: String, filename: String, gzip: Boolean) = this(host, port, keyspace, table, separator, if (filename == null) null else new FileInputStream(filename), gzip)

  def this(host: String, port: Int, keyspace: String, table: String, separator: String, gzip: Boolean) = this(host, port, keyspace, table, separator, null.asInstanceOf[InputStream], gzip)

  val log = Logger[this.type]
  val in: InputStream = if (fileinput == null) System.in else fileinput
  val socket = new Socket(InetAddress.getByName(host), port)
  val pipe = new PipeLZ4(socket, Protocol.LOADER_LOCAL)
  pipe.write(keyspace)
  pipe.write(table)
  pipe.flush
  val schemaDeclaration = pipe.read
  val schema = AimSchema.fromString(schemaDeclaration)

  def close() {
    pipe.close
    socket.close
  }

  def processInput: Int = {
    var count = 0
    val reader = new InputStreamReader(if (gzip) new GZIPInputStream(in) else in)
    try {
      val lineReader = new BufferedReader(reader)
      val values: Array[String] = new Array[String](schema.size)
      var line: String = ""
      var eof = false
      while (!eof) {
        var fields: Int = 0
        while (fields < schema.size && !eof) {
          val line = lineReader.readLine
          if (line == null) {
            eof = true
          } else {
            for (value ← line.split(separator)) {
              values(fields) = value
              fields += 1
            }
          }
        }
        if (!eof) {
          try {
            val record = createEmptyRecord
            for ((t, i) ← schema.fields zip (0 to record.size - 1)) {
              record(i) = t.convert(values(i))
            }
            storeLoadedRecord(record)
            count += 1
          } catch {
            case e: IOException ⇒ {
              log.error(count + ":" + values, e);
              eof = true
            }
            case e: Exception ⇒ {
              log.error(count + ":" + values, e);
            }
          }
        }
      }
    } finally {
      pipe.flush
      close
    }
    in.close
    count
  }
  def createEmptyRecord: Array[Array[Byte]] = new Array[Array[Byte]](schema.size)

  def storeLoadedRecord(record: Array[Array[Byte]]) = {
    try {
      var i = 0
      for (t: AimType ← schema.fields) {
        pipe.write(t.getDataType, record(i))
        i = i + 1
      }
    } catch {
      case e: EOFException ⇒ {
        pipe.flush
        throw e
      }
    }
  }
}