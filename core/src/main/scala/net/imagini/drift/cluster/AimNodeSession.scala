package net.imagini.drift.cluster

import java.io.EOFException
import grizzled.slf4j.Logger
import net.imagini.drift.types.AimQueryException
import java.io.IOException

trait AimNodeSession extends Thread {
  val log = Logger[this.type]
  val node: AimNode
  val pipe: Pipe

  def accept

  final def close = {
    interrupt
    try { pipe.close } catch { case e:IOException ⇒ {} }
  }

  final def exceptionAsString(e: Throwable): String = e.getMessage + e.getStackTrace.map(trace ⇒ trace.toString).foldLeft("\n")(_ + _ + "\n")

  final override def run = {
    try {
      while (!isInterrupted) {
        try {
          accept
        } catch {
          case e: AimQueryException ⇒ log.warn(e)
        }
      }
    } catch {
      case e: EOFException ⇒ {
        close
      }
      //      case w: InterruptedException ⇒ log.warn(w)
      //      case e: Throwable ⇒ { try pipe.close catch { case e: IOException ⇒ {}}}
    }
  }

}