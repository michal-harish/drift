package net.imagini.aim.client

import java.io.BufferedReader
import java.io.InputStreamReader
import net.imagini.aim.types.AimQueryException
import scala.io.Source

object AimConsole extends AimConsole("localhost", 4000) with App {
  start
}

class AimConsole(val host: String = "localhost", val port: Int = 4000) extends Thread {

  val client = new AimClient(host, port)
  val bufferRead = Source.stdin.bufferedReader
  def close = Source.stdin.close
  private var stopped = false

  println("<Enter> for STATS about the table")
  println("FILTER <field> (=|>|<|IN|CONTAINS) '<value>' [(and|or|not) ...) - to setup a filter and see cardinality of it")
  println("ALL / RANGE / LAST")
  println("SELECT [<field>[,<field>[,..]] - to select the records that match previously set filter")
  println("EXIT to exit")

  override def run = {
    try {
      System.out.println();
      System.out.print(">");
      while (!stopped) {
        val line = bufferRead.readLine
        if (line == null) {
          stopped = true
        } else {
          val instruction: String = line.trim match {
            case ""        ⇒ "STATS"
            case i: String ⇒ i
          }
          val input: Array[String] = instruction.split("\\s+", 1)
          try {
            input(0) match {
              case "exit" ⇒ stopped = true
              case _      ⇒ query(instruction)
            }
          } catch {
            case e: AimQueryException ⇒ println(e.getMessage)
            case e: Throwable         ⇒ e.printStackTrace
          }
        }
      }
    } finally {
      System.out.println("Console shutting down..")
      client.close
      this.synchronized(notify)
      System.out.println("Console shut down complete.")
    }
  }

  def query(query: String) = {
    val t = System.currentTimeMillis
    client.query(query) match {
      case (Some(schema)) ⇒ client.printResult
      case None           ⇒ println("OK")
    }
    client.getInfo.map(println(_))
    if (client.getCount > 0) {
      println("Count: " + client.getCount)
    }
    println("Query took: " + (System.currentTimeMillis() - t) + " ms")
    System.out.println();
    System.out.print(">");
  }

}