package net.imagini.aim.client

import java.io.BufferedReader
import java.io.InputStreamReader

object AimConsole extends AimConsole("localhost", 4000) with App {

  println("\nAIM Test Console\n")
  start

}

class AimConsole(val host: String = "localhost", val port: Int = 4000) extends Thread {
  val client = new AimClient(host, port)
  println("<Enter> for STATS about the table")
  println("FILTER <field> (=|>|<|IN|CONTAINS) '<value>' [(and|or|not) ...) - to setup a filter and see cardinality of it")
  println("ALL / RANGE / LAST")
  println("SELECT [<field>[,<field>[,..]] - to select the records that match previously set filter")
  println("EXIT to exit")

  override def run = {
    try {
      val bufferRead = new BufferedReader(new InputStreamReader(System.in))
      var stopped = false
      while (!stopped) {
        System.out.println();
        System.out.print(">");
        val instruction: String = bufferRead.readLine.trim match {
          case ""        ⇒ "STATS"
          case i: String ⇒ i
        }
        val input: Array[String] = instruction.split("\\s+", 1)
        try {
          input(0) match {
            case "exit" ⇒ stopped = true
            case _ ⇒ {
              val t = System.currentTimeMillis
              client.query(instruction) match {
                case Some(result) ⇒ while (result.hasNext) println(result.fetchRecordLine)
                case None => println("N/A")
              }
              println("Query took: " + (System.currentTimeMillis() - t) + " ms")
            }
          }
        } catch {
          case e: Throwable ⇒ e.printStackTrace
        }
      }
    } finally {
      System.out.println("Console shutting down..")
      client.close
    }
  }

}