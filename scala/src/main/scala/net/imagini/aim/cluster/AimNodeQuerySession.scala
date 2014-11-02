package net.imagini.aim.cluster

import java.io.IOException
import net.imagini.aim.utils.Tokenizer
import java.util.Queue
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.partition.StatScanner
import java.io.EOFException
import java.io.InputStream
import net.imagini.aim.types.TypeUtils
import grizzled.slf4j.Logger

class AimNodeQuerySession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {

  private var keyspace: Option[String] = None
  override def accept = {
    try {
      pipe.read.trim match {
        case command: String if (command.toUpperCase.startsWith("USE")) ⇒ useKeySpace(command)
        case query: String if (query.toUpperCase.startsWith("STAT"))    ⇒ handleSelectStream(node.stats)
        case query: String ⇒ keyspace match {
          case Some(k) ⇒ handleSelectStream(node.query(k, query))
          case None    ⇒ throw new IllegalStateException("No keyspace selected, usage USE <keyspace>")
        }
      }
    } catch {
      case e: Throwable ⇒ try {
        pipe.write(true);
        pipe.write("ERROR");
        pipe.write(exceptionAsString(e));
        pipe.write(false);
        pipe.flush();
      } catch {
        case e1: Throwable ⇒ log.error(e)
      }
    }
  }

  private def useKeySpace(command: String) = {
    val cmd = Tokenizer.tokenize(command, false)
    cmd.poll 
    keyspace = Some(cmd.poll)
    pipe.write(false)
    pipe.flush
  }

  private def handleSelectStream(scanner: AbstractScanner) = {
    pipe.write(true)
    pipe.write("RESULT")
    pipe.write(scanner.schema.toString)
    var count: Long = 0
    try {
      while (scanner.next) {
        val row = scanner.selectRow
        pipe.write(true)
        for ((c, t) ← ((0 to scanner.schema.size - 1) zip scanner.schema.fields)) {
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
        pipe.write(if (e.isInstanceOf[EOFException]) "" else exceptionAsString(e)) //success flag
        pipe.flush
      }
    }

  }

}