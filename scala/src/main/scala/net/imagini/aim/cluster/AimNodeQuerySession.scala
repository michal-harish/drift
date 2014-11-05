package net.imagini.aim.cluster

import java.io.EOFException
import java.io.IOException
import java.net.SocketException

import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.tools.CountScanner
import net.imagini.aim.tools.StreamUtils
import net.imagini.aim.types.Aim
import net.imagini.aim.utils.Tokenizer

class AimNodeQuerySession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {

  private var keyspace: Option[String] = None
  override def accept = {
    try {
      pipe.read.trim match {
        case command: String if (command.toUpperCase.startsWith("CLOSE")) ⇒ {
          close
        }
        case command: String if (command.toUpperCase.startsWith("TRANSFORM")) ⇒ keyspace match {
          case Some(k) ⇒ {
            node.transform("addthis", "select vdna_user_uid from syncs join select timestamp,url from pageviews", "vdna", "pageviews")
            pipe.write("OK")
            pipe.flush
          }
          case None ⇒ throw new IllegalStateException("No keyspace selected, usage USE <keyspace>")
        }
        case command: String if (command.toUpperCase.startsWith("USE")) ⇒ useKeySpace(command)
        //        case query: String if (query.toUpperCase.startsWith("DESCRIBE")) ⇒ keyspace match {
        //TODO          case Some(k) ⇒ handleSelectStream(node.describe(k,query))
        //          case None ⇒ throw new IllegalStateException("No keyspace selected, usage USE <keyspace>")
        //        }
        case query: String if (query.toUpperCase.startsWith("STAT")) ⇒ keyspace match {
          case Some(k) ⇒ handleSelectStream(node.stats(k))
          case None    ⇒ throw new IllegalStateException("No keyspace selected, usage USE <keyspace>")
        }
        case query: String ⇒ keyspace match {
          case Some(k) ⇒ handleSelectStream(node.query(k, query))
          case None    ⇒ throw new IllegalStateException("No keyspace selected, usage USE <keyspace>")
        }
      }
    } catch {
      case e: SocketException ⇒ throw new EOFException
      case e: IOException ⇒ {
        log.error(e.getMessage, e)
        pipe.write("ERROR")
        pipe.write(exceptionAsString(e))
        pipe.flush
      }
      case e: Throwable ⇒ {
        log.error(e.getMessage, e)
        pipe.write("ERROR")
        pipe.write(if (e.getMessage == null) e.getClass().getSimpleName() else e.getMessage)
        pipe.flush
      }
    }
  }

  private def useKeySpace(command: String) = {
    val cmd = Tokenizer.tokenize(command, false)
    cmd.poll
    keyspace = Some(cmd.poll)
    if (node.keyspaces.contains(keyspace.get)) {
      pipe.write("OK")
    } else {
      pipe.write("ERROR")
      pipe.write("Unknown keyspace, available: " + node.keyspaces.mkString(","))
    }
    pipe.flush
  }

  private def handleSelectStream(scanner: AbstractScanner) = {
    scanner match {
      case scanner: CountScanner ⇒ {
        val count = scanner.count.toString
        pipe.write("COUNT")
        pipe.write(count)
        pipe.flush
      }
      case scanner: AbstractScanner ⇒ {
        pipe.write("RESULT")
        pipe.write(scanner.schema.toString)
        try {
          while (scanner.next) {
            val row = scanner.selectRow
            pipe.writeInt(scanner.schema.size)
            for ((c, t) ← ((0 to scanner.schema.size - 1) zip scanner.schema.fields)) {
              val dataType = t.getDataType
              pipe.write(dataType.getDataType, row(c))
            }
          }
          throw new EOFException
        } catch {
          case e: Throwable ⇒ {
            pipe.writeInt(0)
            pipe.write(if (e.isInstanceOf[EOFException]) "" else exceptionAsString(e)) //success flag
            pipe.flush
          }
        }
      }
    }

  }

}