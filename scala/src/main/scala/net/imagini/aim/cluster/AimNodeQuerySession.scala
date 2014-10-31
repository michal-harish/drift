package net.imagini.aim.cluster

import java.io.IOException
import net.imagini.aim.utils.Tokenizer
import net.imagini.aim.tools.Pipe
import java.util.Queue

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
              case "STATS" ⇒ handleStats(cmd)
              //            case "FILTER" ⇒ handleFilter(cmd)
              //            case "SELECT" ⇒ handleSelectRequest(cmd)
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

  private def handleStats(cmd: Queue[String]) = {
    say("STATS")

    node.keyspaces.map(k ⇒ {
      say("KEYSPACE: " + k + ", PARTITION: " + node.id)
      node.keyspace(k).map {
        case (t, partition) ⇒ {
          say("- REGION " + t + "\tSCHEMA: " + partition.schema.toString)
          say("- REGION " + t + "\tSEGMENTS: " + partition.numSegments)
          if (partition.getUncompressedSize > 0) {
              say("- REGION " + t + "\tCOMPRESSION RATE: " + partition.getCompressedSize + " / " + partition.getUncompressedSize + " " + math.round(partition.getCompressedSize / partition.getUncompressedSize * 100) +  "%")
          } else {
             say("- REGION " + t + "\tCOMPRESSION RATE: N/A EMPTY")
          }
          say("- REGION " + t + "\tCOUNT(*): " + partition.count("*"))
          //say("- TABLE" + t + "\tCOUNT(DISTINCT): " + partition.count("!"))
        }
      }
    })
  }
  private def say(line: String) = {
    pipe.write(true)
    pipe.write(line)
    pipe.flush
  }
}