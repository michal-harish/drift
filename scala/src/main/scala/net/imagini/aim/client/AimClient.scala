package net.imagini.aim.client

import java.io.IOException
import java.net.InetAddress
import java.net.Socket
import scala.Array.canBuildFrom
import net.imagini.aim.types.AimSchema
import net.imagini.aim.cluster.Pipe
import net.imagini.aim.cluster.Protocol
import java.io.EOFException
import net.imagini.aim.types.AimQueryException
import net.imagini.aim.tools.StreamUtils
import net.imagini.aim.types.Aim
import net.imagini.aim.utils.ByteUtils
import java.nio.ByteBuffer

object AimClient extends App {

  var host: String = "localhost"
  var port: Int = 4000
  var keyspace: String = null
  var table: String = null
  val argsIterator = args.iterator
  var query: Option[String] = None
  var separator: String = "\t"
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--host"      ⇒ host = argsIterator.next
      case "--port"      ⇒ port = argsIterator.next.toInt
      case "--keyspace"  ⇒ keyspace = argsIterator.next
      case "--separator" ⇒ separator = argsIterator.next
      case arg: String   ⇒ query = Some(arg)
    }
  }
  query match {
    case None ⇒ println("Usage: java jar drift-client.jar --keyspace <name> [--host <localhost> --port <4000> --separator<\\t>] '<query>'")
    case Some(query) ⇒ {
      val client = new AimClient(host, port)
      client.query("USE " + keyspace)
      client.query(query) match {
        case None ⇒ println("Invalid DRFIT Query")
        case Some(schema) ⇒ {
          while (client.hasNext) {
            println(client.fetchRecordStrings.mkString(separator))
          }
        }
      }
      client.close
    }
  }
}

class AimClient(val host: String, val port: Int) {

  private var socket = new Socket(InetAddress.getByName(host), port)
  private var pipe = Pipe.newLZ4Pipe(socket, Protocol.QUERY_LOCAL)
  private var error: Option[String] = None
  private var hasData: Option[Boolean] = None
  private var schema: Option[AimSchema] = None
  private var count: Long = 0

  private def reconnect = {
    socket.close
    socket = new Socket(InetAddress.getByName(host), port)
    pipe = Pipe.newLZ4Pipe(socket, Protocol.QUERY_LOCAL)
  }

  def close = {
    pipe.write("CLOSE")
    pipe.flush
    try { pipe.close } catch { case e: IOException ⇒ {} }
  }

  def resultSchema: AimSchema = schema match {
    case None         ⇒ throw new IllegalStateException
    case Some(schema) ⇒ schema
  }

  def printResult = {
    val len: Array[Int] = resultSchema.names.map(n ⇒ (math.max(n.length, resultSchema.dataType(n).getSize) / 4 + 1) * 4 + 1).toArray
    var printedHeader = false
    while (hasNext) {
      val line = (0 to len.length - 1, fetchRecordStrings).zipped.map((i, v) ⇒ {
        len(i) = math.max(len(i), (v.length / 4 + 1) * 4 + 1)
        " " + v.padTo(len(i) - 1, ' ')
      }).mkString("|")
      if (!printedHeader) {
        val header = (0 to len.length - 1).map(i ⇒ " " + resultSchema.name(i).padTo(len(i) - 1, ' ')).mkString("|")
        println(header)
        println(len.map(l ⇒ "-" * l).mkString("+"))
        printedHeader = true
      }
      println(line)
    }
    println()
  }

  def query(query: String): Option[AimSchema] = {
    if (hasNext) {
      reconnect
    }
    count = 0
    pipe.write(query).flush
    if (processResponse(pipe)) {
      schema
    } else {
      None
    }
  }
  def getCount: Long = count

  def hasNext: Boolean = {
    hasData match {
      case Some(bool) ⇒ bool
      case None ⇒ {
        schema match {
          case None ⇒ false
          case Some(resultSchema) ⇒
            hasData = Some(pipe.readInt match {
              case i: Int if (i == resultSchema.size) ⇒ true
              case i: Int if (i <= 0) ⇒ {
                this.error = pipe.read match { case null ⇒ None case error: String ⇒ Some(error) }
                false
              }
              case i: Int ⇒ throw new IllegalStateException("Invalid column number in the result stream: " + i)
            })
            hasData.get
        }
      }
    }
  }

  def fetchRecordLine = fetchRecordStrings.mkString(",")

  def fetchRecordStrings: Array[String] = (schema.get.fields, fetchRecord).zipped.map((t, a) ⇒ t.convert(a))

  def fetchRecord: Array[Array[Byte]] = {
    if (schema == None) {
      throw new IllegalStateException
    } else if (!hasNext) {
      throw new EOFException
    } else {
      hasData = None
      count += 1
      schema.get.fields.map(field ⇒ pipe.read(field.getDataType))
    }
  }

  def fetchRow(buffer: ByteBuffer) = {
    if (schema == None) {
      throw new IllegalStateException
    } else if (!hasNext) {
      throw new EOFException
    } else {
      hasData = None
      count += 1
      var i = 0
      var fields = schema.get.size
      while (i < fields) {
        pipe.readInto(schema.get.dataType(i).getDataType, buffer)
        i += 1
      }
    }
  }

  private def processResponse(pipe: Pipe): Boolean = {
    pipe.read match {
      case "OK" ⇒ {
        schema = None
        hasData = Some(false)
        true
      }
      case "ERROR" ⇒ {
        schema = None
        hasData = Some(false)
        val error = pipe.read
        throw new AimQueryException(error)
      }
      case "COUNT" ⇒ {
        count = java.lang.Long.valueOf(pipe.read)
        false
      }
      case "RESULT" ⇒ {
        schema = Some(AimSchema.fromString(pipe.read))
        hasData = None
        true
      }
      case _ ⇒ {
        schema = None
        hasData = Some(false)
        reconnect
        throw new IOException("Stream is curroupt, closing..")
      }
    }
  }
}