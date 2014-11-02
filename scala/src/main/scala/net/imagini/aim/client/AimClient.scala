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

class AimClient(val host: String, val port: Int) {

  private var socket = new Socket(InetAddress.getByName(host), port)
  private var pipe = new PipeLZ4(socket, Protocol.QUERY_LOCAL)
  private var error: Option[String] = None
  private var hasData: Option[Boolean] = None
  private var schema: Option[AimSchema] = None

  private def reconnect = {
    socket.close
    socket = new Socket(InetAddress.getByName(host), port)
    pipe = new PipeLZ4(socket, Protocol.QUERY_LOCAL)
  }

  def close = {
    pipe.write("CLOSE")
    pipe.flush
    pipe.close
  }

  def query(query: String): Boolean = {
    if (hasNext) {
      reconnect
    }
    pipe.write(query).flush
    processResponse(pipe)
  }

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
      schema.get.fields.map(field ⇒ pipe.read(field.getDataType))
    }
  }

  private def processResponse(pipe: Pipe) = {
    pipe.read match {
      case "OK" ⇒ {
        schema = None
        hasData = Some(false)
        true
      }
      case "ERROR" ⇒ {
        schema = None
        hasData = Some(false)
        val error = pipe.read();
        reconnect
        throw new Exception("AIM SERVER ERROR: " + error);
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