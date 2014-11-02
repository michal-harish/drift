package net.imagini.aim.cluster

import java.io.IOException
import java.io.EOFException
import grizzled.slf4j.Logger

trait AimNodeSession extends Thread {
  val log = Logger[AimNodeSession.this.type]
  val node: AimNode
  val pipe: Pipe

  def accept

  final def close = {
    interrupt
    pipe.close
  }

  final def exceptionAsString(e: Throwable): String = e.getMessage + e.getStackTrace.map(trace ⇒ trace.toString).foldLeft("\n")(_ + _ + "\n")

  final override def run = {
    try {
      while (!node.isShutdown && !isInterrupted) {
        accept
      }
    } catch {
      case e: EOFException         ⇒ {}
      case w: InterruptedException ⇒ {}
      case e: IOException ⇒ try pipe.close catch { case e: IOException ⇒ log.error(e) }
    }
  }

}