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
import net.imagini.aim.cluster.AimNode

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
    }
  }
}

class AimClient(val host: String, val port: Int, val protocol: Protocol) {
  def this(host: String, port: Int) = this(host, port, Protocol.QUERY_USER)
  private var socket: Socket = null
  private var pipe: Pipe = null
  private var error: Option[String] = None
  private var schema: Option[AimSchema] = None
  private var next: Option[Array[Array[Byte]]] = None
  private var count: Long = 0
  def getCount: Long = count
  def getSchema: Option[AimSchema] = schema

  def query(query: String): Option[AimSchema] = {
    if (socket == null || hasNext) {
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

  def getInputStream = pipe.getInputStream

  def hasNext: Boolean = {
    schema match {
      case None ⇒ false
      case Some(resultSchema) ⇒ next match {
        case Some(rowData) ⇒ true
        case None ⇒ {
          try {
            count += 1
            loadNextRecord
          } catch {
            case e: EOFException ⇒ {
              count -= 1
              schema = None
              socket = null
              close
              return false
            }
          }
          true
        }
      }
    }
  }

  private def loadNextRecord = {
    if (schema == None) {
      throw new IllegalStateException
    } else {
      next = Some(schema.get.fields.map(field ⇒ pipe.read(field.getDataType)))
    }
  }

  private def reconnect = {
    if (socket != null) socket.close
    socket = new Socket(InetAddress.getByName(host), port)
    pipe = Pipe.newLZ4Pipe(socket, protocol)
  }

  def close = {
    if (pipe != null) {
      try {
        pipe.write("CLOSE")
        pipe.flush
        pipe.close
      } catch {
        case e: IOException ⇒ {}
      }
    }
  }

  def resultSchema: AimSchema = schema match {
    case None         ⇒ throw new IllegalStateException
    case Some(schema) ⇒ schema
  }

  def printResult = {
    val len: Array[Int] = resultSchema.names.map(n ⇒ (math.max(n.length, resultSchema.dataType(n).getLen) / 4 + 1) * 4 + 1).toArray
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

  def fetchRecordLine = fetchRecordStrings.mkString(",")

  def fetchRecordStrings: Array[String] = {
    if (schema == None) {
      throw new EOFException
    } else {
      (schema.get.fields, fetchRecord).zipped.map((t, a) ⇒ t.convert(a))
    }
  }

  def fetchRecord: Array[Array[Byte]] = {
    if (schema == None) {
      throw new EOFException
    } else if (next == None && !hasNext) {
      throw new EOFException
    } else {
      val record = next.get
      next = None
      record
    }
  }

  private def processResponse(pipe: Pipe): Boolean = {
    next = None
    count = -1
    pipe.read match {
      case "OK" ⇒ {
        schema = None
        true
      }
      case "ERROR" ⇒ {
        schema = None
        val error = pipe.read
        throw new AimQueryException(error)
      }
      case "COUNT" ⇒ {
        count = java.lang.Long.valueOf(pipe.read)
        false
      }
      case "RESULT" ⇒ {
        schema = Some(AimSchema.fromString(pipe.read))
        true
      }
      case _ ⇒ {
        schema = None
        reconnect
        throw new IOException("Stream is curroupt, closing..")
      }
    }
  }
}