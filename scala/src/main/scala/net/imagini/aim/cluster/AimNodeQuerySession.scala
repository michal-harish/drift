package net.imagini.aim.cluster

import java.io.IOException
import net.imagini.aim.utils.Tokenizer
import java.util.Queue
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.partition.StatScanner
import java.io.EOFException
import java.io.InputStream
import net.imagini.aim.types.TypeUtils

trait AimNodeSession extends Thread {
  def close
}
class AimNodeQuerySession(val node: AimNode, val pipe: Pipe) extends AimNodeSession {
  def exceptionAsString(e: Throwable): String = e.getMessage + e.getStackTrace.map(trace ⇒ trace.toString).foldLeft("\n")(_ + _ + "\n")

  start
  def close = {
    interrupt
    pipe.close
  }
  override def run = {
    try {
      while (!node.isShutdown) {
        try {
          val input: String = pipe.read
          try {
            val cmd = Tokenizer.tokenize(input)
            val command = cmd.poll.toUpperCase
            command.toUpperCase match {
              case "STATS" ⇒ handleSelectStream(node.stats)
              //            case "FILTER" ⇒ handleSelectStream(node.fitler(cmd))
              //            case "SELECT" ⇒ handleSelectStream(node.select(cmd))
              case _       ⇒ {}
            }
            pipe.write(false)
            pipe.flush()
          } catch {
            case e: Throwable ⇒ try {
              pipe.write(true);
              pipe.write("ERROR");
              pipe.write(exceptionAsString(e));
              pipe.write(false);
              pipe.flush();
            } catch {
              case e1: Throwable ⇒ {
                e.printStackTrace();
              }
            }
          }
        } catch {
          case w: InterruptedException ⇒ {}
        }

      }
    } catch {
      case e: IOException ⇒ {
        try pipe.close catch { case e: IOException ⇒ e.printStackTrace }
      }
    }
  }

  private def handleSelectStream(scanner: AbstractScanner) = {
    pipe.write(true)
    pipe.write("RESULT")
    pipe.write(scanner.schema.toString)
    pipe.write("*") //TODO remove this
    var count: Long = 0
    try {
      while (scanner.next) {
        val row = scanner.selectRow
        pipe.write(true)
        for ((c,t) ← ((0 to scanner.schema.size - 1) zip scanner.schema.fields)) {
          val dataType = t.getDataType
          pipe.write(dataType.getDataType, row(c))
        }
        count += 1
      }
      throw new EOFException
    } catch {
      //TODO refactor this was just copy-paste from old java code
      case e: Throwable ⇒ {
        pipe.write(false)
        pipe.write(count)
        pipe.write(0L) // TODO remove this
        pipe.write(if (e.isInstanceOf[EOFException]) "" else exceptionAsString(e)) //success flag
        pipe.flush
      }
    }


  }

}