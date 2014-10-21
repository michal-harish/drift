package net.imagini.aim.partition

import net.imagini.aim.tools.Pipe
import java.io.IOException
import net.imagini.aim.tools.AimFilter
import java.util.Queue
import net.imagini.aim.tools.Tokenizer
import java.util.ArrayList
import java.io.InputStream
import net.imagini.aim.tools.PipeUtils
import java.io.EOFException

class AimPartitionServerQuerySession(val partition: AimPartition, val pipe: Pipe) extends Thread {

  var filter: AimFilter = null

  def exceptionAsString(e: Throwable): String = e.getMessage + e.getStackTrace.map(trace ⇒ trace.toString).foldLeft("\n")(_ + _ + "\n")

  override def run = {
    try {
      while (true) {
        val input: String = pipe.read
        try {
          val cmd = Tokenizer.tokenize(input)
          val command = cmd.poll.toUpperCase
          command.toUpperCase match {
            case "STATS"  ⇒ handleStats(cmd)
            case "FILTER" ⇒ handleFilter(cmd)
            case "SELECT" ⇒ handleSelectRequest(cmd)
            case _        ⇒ {}
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
      }
    } catch {
      case e: IOException ⇒ {
        try pipe.close catch { case e: IOException ⇒ e.printStackTrace }
      }
    }
  }

  private def handleFilter(cmd: Queue[String]) = {
    if (cmd.size > 0) {
      filter = AimFilter.fromTokenQueue(partition.schema, cmd)
    }
    pipe.write(true);
    pipe.write("COUNT");
    val count = partition.count(filter)
    pipe.write(if (filter == null) null else filter.toString)
    pipe.write(count);
    pipe.write(partition.getCount)
    pipe.flush();

  }

  private def handleStats(cmd: Queue[String]) = {
    pipe.write(true)
    pipe.write("STATS")
    pipe.write(partition.schema.toString)
    pipe.write(partition.getCount)
    pipe.write(partition.getNumSegments)
    pipe.write(partition.getCompressedSize)
    pipe.write(partition.getUncompressedSize)
  }

  private def handleSelectRequest(cmd: Queue[String]) = {
    val schema = if (cmd.size > 0) partition.schema.subset(new ArrayList(cmd)) else partition.schema
    val result: InputStream = partition.select(filter, schema.names);
    pipe.write(true)
    pipe.write("RESULT")
    pipe.write(schema.toString)
    pipe.write(if (filter == null) null else filter.toString)
    var count: Long = 0
    try {
      while (true) {
        var written = false;
        for (t ← schema.fields) {
          val dataType = t.getDataType
          val value: Array[Byte] = PipeUtils.read(result, dataType)
          if (!written) {
            written = true
            pipe.write(written)
          }
          pipe.write(dataType.getDataType, value)
        }
        count += 1
      }
    } catch {
      case e: Throwable ⇒ {
        pipe.write(false)
        pipe.write(count)
        pipe.write(partition.getCount)
        pipe.write(if (e.isInstanceOf[EOFException]) "" else exceptionAsString(e)) //success flag
        pipe.flush
      }
    }

  }
}