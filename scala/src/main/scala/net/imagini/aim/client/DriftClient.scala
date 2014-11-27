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
import net.imagini.aim.cluster.StreamUtils
import net.imagini.aim.types.Aim
import net.imagini.aim.utils.ByteUtils
import java.nio.ByteBuffer
import net.imagini.aim.cluster.AimNode
import net.imagini.aim.utils.View

object DriftClient extends App {

  var host: String = "localhost"
  var port: Int = 4000
  var table: String = null
  val argsIterator = args.iterator
  var query: Option[String] = None
  var separator: String = "\t"
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--host"      ⇒ host = argsIterator.next
      case "--port"      ⇒ port = argsIterator.next.toInt
      case "--separator" ⇒ separator = argsIterator.next
      case arg: String   ⇒ query = Some(arg)
    }
  }
  query match {
    case None ⇒ println("Usage: java jar drift-client.jar [--host <localhost> --port <4000> --separator<\\t>] '<query>'")
    case Some(query) ⇒ {
      val client = new DriftClient(host, port)
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

class DriftClient(val host: String, val port: Int, val protocol: Protocol) {
  def this(host: String, port: Int) = this(host, port, Protocol.QUERY_USER)
  private var socket: Socket = null
  private var pipe: Pipe = null
  private var schema: Option[AimSchema] = None
  private var next: Option[Array[Array[Byte]]] = None
  private var info: Option[Seq[String]] = null
  private var numRecords: Option[Long] = null

  def getSchema: Option[AimSchema] = schema

  def getInfo: Seq[String] = info match {
    case null      ⇒ Seq()
    case Some(seq) ⇒ seq
    case None ⇒ {
      val numLines = pipe.readInt()
      info = Some((1 to numLines).map(_ ⇒ pipe.read))
      info.get
    }
  }
  def getCount: Long = numRecords match {
    case null        ⇒ 0
    case Some(count) ⇒ count
    case None ⇒ schema match {
      case None ⇒ {
        numRecords = Some(pipe.readInt)
        numRecords.get
      }
      case Some(schema) ⇒ throw new IllegalStateException
    }
  }

  def query(query: String): Option[AimSchema] = {
    if (socket == null || hasNext) {
      reconnect
    }
    try {
      pipe.write(query).flush
      prepareResponse(pipe)
      schema
    } catch {
      case e: IOException ⇒ {
        close
        throw e
      }
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
            numRecords = Some(numRecords.get + 1)
            loadNextRecord
          } catch {
            case e: EOFException ⇒ {
              numRecords = Some(numRecords.get - 1)
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
      next = Some(schema.get.fields.map(field ⇒ pipe.read(field)))
    }
  }

  private def reconnect = {
    if (socket != null) socket.close
    socket = new Socket(InetAddress.getByName(host), port)
    //pipe = Pipe.newLZ4Pipe(socket, protocol) //FIXME LZ4BlockOutputStream creates a lot of garbage via pipe.createOutputPipe
    pipe = Pipe.newGZIPPipe(socket, protocol)
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
    val len: Array[Int] = resultSchema.names.map(n ⇒ (math.max(n.length, resultSchema.field(n).getLen) / 4 + 1) * 4 + 1).toArray
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
      (schema.get.fields, fetchRecord).zipped.map((t, a) ⇒ {
        t.asString(a)
      })
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

  private def prepareResponse(pipe: Pipe) = {
    schema = None
    numRecords = null
    info = null
    pipe.read match {
      case "OK"    ⇒ info = None
      case "COUNT" ⇒ numRecords = None
      case "RESULT" ⇒ {
        numRecords = Some(0)
        schema = Some(AimSchema.fromString(pipe.read))
      }
      case "ERROR" ⇒ {
        throw new AimQueryException(pipe.read)
      }
      case any: String ⇒ {
        reconnect
        throw new IOException("Session stream is curroupt, closing.. `" + any + "`")
      }
    }
  }
}