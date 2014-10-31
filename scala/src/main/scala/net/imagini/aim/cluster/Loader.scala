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
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType

class Loader(host: String, port: Int, val schema: AimSchema, val separator: String, val fileinput: InputStream, val gzip: Boolean) {

  def this(host: String, port: Int, schema: AimSchema, separator: String, filename: String, gzip: Boolean) 
      = this(host, port, schema, separator, if (filename == null) null else new FileInputStream(filename), gzip)

  def this(host: String, port: Int, schema: AimSchema, separator: String, gzip: Boolean) 
      = this(host, port, schema, separator, null.asInstanceOf[InputStream], gzip)

  val in: InputStream = if (fileinput == null) System.in else fileinput
  val socket = new Socket(InetAddress.getByName(host), port)
  val out = new PipeLZ4(socket.getOutputStream(), Protocol.LOADER)
  out.write(schema.toString)

  def close() {
    out.close
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
            case e: Exception ⇒ {
              System.err.println(count + ":" + values);
              e.printStackTrace();
            }
          }
        }
      }
    } finally {
      out.flush
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
        out.write(t.getDataType, record(i))
        i = i + 1
      }
    } catch {
      case e: EOFException ⇒ {
        out.flush
        throw e
      }
    }
  }
}