package net.imagini.aim.cluster

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.net.InetAddress
import java.net.Socket

import net.imagini.aim.AimUtils
import net.imagini.aim.PipeLZ4
import net.imagini.aim.Protocol
import net.imagini.aim.AimSchema

object AimConsole extends AimConsole("localhost", 4000) with App {

  println("\nAIM Test Console\n")
  start

}

class AimConsole(val host: String = "localhost", val port: Int = 4000) extends Thread {
  val socket = new Socket(InetAddress.getByName(host), port)
  val pipe = new PipeLZ4(socket, Protocol.QUERY)
  println("<Enter> for STATS about the table")
  println("FILTER <field> (=|>|<|IN|CONTAINS) '<value>' [(and|or|not) ...) - to setup a filter and see cardinality of it")
  println("SELECT [<field>[,<field>[,..]] - to select the records that match previously set filter")
  println("EXIT to exit")

  override def run = {
    try {
      val bufferRead = new BufferedReader(new InputStreamReader(System.in))
      var stopped = false
      while (!stopped) {
        System.out.println();
        System.out.print(">");
        val instruction: String = bufferRead.readLine().trim()
        val input: Array[String] = instruction.split("\\s+", 1)
        try {
          input(0) match {
            case "exit" ⇒ stopped = true
            case ""     ⇒ { pipe.write("STATS").flush; processResponse }
            case _ ⇒ {
              val t = System.currentTimeMillis
              pipe.write(instruction).flush
              processResponse
              println("Query took: " + (System.currentTimeMillis() - t) + " ms")
            }
          }
        } catch {
          case e: Throwable ⇒ e.printStackTrace
        }
      }
    } finally {
      System.out.println("Console shutting down..")
      pipe.close
    }
  }

  private def processResponse = {
    while (pipe.readBool) {
      pipe.read match {
        case "ERROR" ⇒ print("Error: " + pipe.read());
        case "STATS" ⇒ {
          System.out.println("Schema: " + pipe.read)
          System.out.println("Total records: " + pipe.readLong)
          System.out.println("Total segments: " + pipe.readInt)
          val size: Long = pipe.readLong
          val originalSize: Long = pipe.readLong
          print("Total compressed/original size: " + (size / 1024 / 1024) + " Mb / " + (originalSize / 1024 / 1024))
          if (originalSize > 0) {
            print(" Mb = " + (size * 100 / originalSize) + "%")
          }
          println()
        }
        case "COUNT" ⇒ {
          val filter = pipe.read
          val count = pipe.readLong
          val totalCount: Long = pipe.readLong
          println("Filter: " + filter)
          println("Count: " + count + "/" + totalCount)
        }
        case "RESULT" ⇒ {
          val schema = AimSchema.parseSchema(pipe.read)
          val filter = pipe.read
          while (pipe.readBool) {
            val record = AimUtils.fetchRecord(schema, pipe)
            println(record.foldLeft("")(_+_+","))
          }
          val filteredCount = pipe.readLong
          val count = pipe.readLong
          val error = pipe.read
          println("Schema: " + schema)
          println("Filter: " + filter)
          println((if (error == null || error.isEmpty()) "OK" else "SERVER-SIDE EXCEPTION: " + error) + " Num.records: " + filteredCount + "/" + count)
        }
        case _ ⇒ {
          throw new IOException("Stream is curroupt, closing..")
        }
      }
    }
  }
}