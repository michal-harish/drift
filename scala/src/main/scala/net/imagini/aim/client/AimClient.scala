package net.imagini.aim.client

import java.io.IOException
import java.net.InetAddress
import java.net.Socket
import scala.Array.canBuildFrom
import net.imagini.aim.types.AimSchema
import net.imagini.aim.tools.Pipe
import net.imagini.aim.cluster.PipeLZ4
import net.imagini.aim.cluster.Protocol

case class AimResult(val schema: AimSchema, val pipe: Pipe) {
  var filteredCount = 0L
  var count = 0L
  var error: Option[String] = None
  def close = pipe.close
  def hasNext: Boolean = {
    pipe.readBool match {
      case true ⇒ true
      case false ⇒ {
        filteredCount = pipe.readLong
        count = pipe.readLong
        val error = pipe.read
        pipe.readBool
        this.error = if (error != null) Some(error) else None
        false
      }
    }
  }
  def fetchRecord: Array[Array[Byte]] = schema.fields.map(field ⇒ pipe.read(field.getDataType))
  def fetchRecordStrings: Array[String] = schema.fields.map(field ⇒ field.convert(pipe.read(field.getDataType)))
  def fetchRecordLine = fetchRecordStrings.foldLeft("")(_ + _ + ",")
}

class AimClient(val host: String, val port: Int) {

  private var socket = new Socket(InetAddress.getByName(host), port)
  private var pipe = new PipeLZ4(socket, Protocol.QUERY)
  private def reconnect = {
    socket.close
    socket = new Socket(InetAddress.getByName(host), port)
    pipe = new PipeLZ4(socket, Protocol.QUERY)
  }

  def close = pipe.close

  def query(query: String): Any = {
    pipe.write(query).flush
    processResponse(pipe)
  }
  def select(filter: String = "*"): Option[AimResult] = {
    if (!filter.equals("*")) {
      pipe.write(filter).flush
      processResponse(pipe) match {
        case s: Seq[_] ⇒ println(s.foldLeft("")(_ + _))
        case x: Any    ⇒ throw new Exception("Unknown response from the Aim Server " + x.toString)
      }
    }
    pipe.write("select").flush
    processResponse(pipe) match {
      case result: AimResult ⇒ Some(result)
      case s: Seq[_]         ⇒ throw new Exception("Unexpected response from the Aim Server" + s.foldLeft("")(_ + _))
      case x: Any            ⇒ throw new Exception("Unknown response from the Aim Server" + x.toString)
    }
  }

  private def processResponse(pipe: Pipe): Any = {
    if (!pipe.readBool) {
      "Empty response"
    } else {
      pipe.read match {
        case "ERROR" ⇒ {
          reconnect
          throw new Exception("AIM SERVER ERROR: " + pipe.read());
        }
        case "STATS" ⇒ {
          val lines = scala.collection.mutable.ListBuffer[String]()
          while (pipe.readBool) {
            val line = pipe.read
            lines += line
          }
          lines.toSeq
          //          val schema = pipe.read
          //          val numRecords = pipe.readLong
          //          val numSegments = pipe.readInt
          //          val size: Long = pipe.readLong
          //          val originalSize: Long = pipe.readLong
          //          pipe.readBool
          //          Seq(
          //            "Schema: " + schema,
          //            "Total records: " + numRecords,
          //            "Total segments: " + numSegments,
          //            "Total compressed/original size: " + (size / 1024 / 1024) + " Mb / " + (originalSize / 1024 / 1024),
          //            if (originalSize > 0) " Mb = " + (size * 100 / originalSize) + "%" else "")
        }
        case "COUNT" ⇒ {
          val filter = pipe.read
          val count = pipe.readLong
          val totalCount: Long = pipe.readLong
          pipe.readBool
          Seq(filter, count + "/" + totalCount)
        }
        case "RESULT" ⇒ {
          val schema = AimSchema.fromString(pipe.read)
          val filter = pipe.read
          new AimResult(schema, pipe)
        }
        case _ ⇒ {
          reconnect
          throw new IOException("Stream is curroupt, closing..")
        }
      }
    }
  }
}