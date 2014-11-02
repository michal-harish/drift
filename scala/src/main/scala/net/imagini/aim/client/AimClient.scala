package net.imagini.aim.client

import java.io.IOException
import java.net.InetAddress
import java.net.Socket
import scala.Array.canBuildFrom
import net.imagini.aim.types.AimSchema
import net.imagini.aim.cluster.Pipe
import net.imagini.aim.cluster.PipeLZ4
import net.imagini.aim.cluster.Protocol
import java.io.EOFException

case class AimResult(val schema: AimSchema, val pipe: Pipe) {
  var eof = false
  var count = 0L
  var error: Option[String] = None
  def hasNext: Boolean = {
    pipe.readBool match {
      case true ⇒ true
      case false ⇒ {
        eof = true
        this.error = pipe.read match { case null ⇒ None case error: String ⇒ Some(error) }
        false
      }
    }
  }
  def fetchRecord: Array[Array[Byte]] = if (eof) throw new EOFException else {
    count+=1
    schema.fields.map(field ⇒ pipe.read(field.getDataType))
  }
  def fetchRecordStrings: Array[String] = if (eof) throw new EOFException else {
    count+=1
    schema.fields.map(field ⇒ {
      System.err.println(field)
      val value = field.convert(pipe.read(field.getDataType)) 
      System.err.println(value)
      value
    })
  }
  def fetchRecordLine = fetchRecordStrings.foldLeft("")(_ + _ + ",")
}

class AimClient(val host: String, val port: Int) {

  private var socket = new Socket(InetAddress.getByName(host), port)
  private var pipe = new PipeLZ4(socket, Protocol.QUERY_LOCAL)
  private def reconnect = {
    socket.close
    socket = new Socket(InetAddress.getByName(host), port)
    pipe = new PipeLZ4(socket, Protocol.QUERY_LOCAL)
  }

  def close = pipe.close

  def query(query: String): Option[AimResult] = {
    pipe.write(query).flush
    System.err.println(query)
    processResponse(pipe)
  }

  private def processResponse(pipe: Pipe): Option[AimResult] = {
    pipe.read match {
      case "OK" ⇒ None
      case "ERROR" ⇒ {
        reconnect
        throw new Exception("AIM SERVER ERROR: " + pipe.read());
      }
      case "RESULT" ⇒ {
        val schema = AimSchema.fromString(pipe.read)
        System.err.println("!!")
        Some(new AimResult(schema, pipe))
      }
      case _ ⇒ {
        reconnect
        throw new IOException("Stream is curroupt, closing..")
      }
    }
  }
}