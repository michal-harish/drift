package net.imagini.aim.cluster

import java.io.EOFException
import java.io.IOException
import java.net.SocketException
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.tools.CountScanner
import net.imagini.aim.tools.StreamUtils
import net.imagini.aim.types.Aim
import net.imagini.aim.utils.Tokenizer
import net.imagini.aim.types.AimQueryException
import net.imagini.aim.client.AimClient
import net.imagini.aim.cluster.Protocol._
import net.imagini.aim.tools.StreamMerger

class AimNodeQuerySession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {

  //  private var keyspace: Option[String] = None
  private val peerClients: Seq[AimClient] = if (pipe.protocol.equals(QUERY_INTERNAL)) Seq() else node.peers.map(peer ⇒ {
    new AimClient(peer._2.getHost, peer._2.getPort, QUERY_INTERNAL)
  }).toSeq

  override def accept = {
    try {
      val request = pipe.read.trim
      if (pipe.protocol == QUERY_USER) {
        peerClients.foreach(peer ⇒ peer.query(request))
      }
      request match {
        case command: String if (command.toUpperCase.startsWith("TRANSFORM")) ⇒ {
          node.transform("select vdna_user_uid from addthis.syncs join select timestamp,url from views", "vdna", "pageviews")
          pipe.write("OK")
          pipe.flush
        }
        case command: String if (command.toUpperCase.startsWith("CLOSE")) ⇒ close
        case command: String if (command.toUpperCase.startsWith("USE"))   ⇒ throw new NotImplementedError
        case query: String if (query.toUpperCase.startsWith("STAT")) ⇒ handleSelectStream(node.stats(request.substring(5).trim))
        case query: String ⇒ handleSelectStream(node.query(query))
      }
    } catch {
      case e: SocketException ⇒ throw new EOFException
      case e: Throwable ⇒ {
        log.error(e.getMessage, e)
        pipe.write("ERROR")
        pipe.write(if (e.getMessage == null) e.getClass().getSimpleName() else e.getMessage)
        pipe.flush
      }
    }
  }

  private def handleSelectStream(localScanner: AbstractScanner) = {

    localScanner match {
      case localScanner: CountScanner ⇒ {
        pipe.write("COUNT")
        pipe.flush
        val count: Long = pipe.protocol match {
          case QUERY_INTERNAL ⇒ localScanner.count
          case QUERY_USER ⇒ {
            peerClients.map(peer ⇒ peer.getCount).foldLeft(0L)(_ + _) + localScanner.count
          }
          case _ ⇒ throw new AimQueryException("Invalid query protocol")
        }
        pipe.writeInt(count.toInt)
        pipe.flush
      }
      case localScanner: AbstractScanner ⇒ {
        pipe.write("RESULT")
        pipe.write(localScanner.schema.toString)
        pipe.flush

        var stream = pipe.protocol match {
          case QUERY_INTERNAL ⇒ new ScannerInputStream(localScanner)
          case QUERY_USER ⇒ {
            if (peerClients.forall(peer ⇒ !peer.getSchema.isEmpty && peer.getSchema.get.toString.equals(localScanner.schema.toString))) {
              val streams = peerClients.map(peer ⇒ peer.getInputStream).toArray ++ Array(new ScannerInputStream(localScanner))
              new StreamMerger(localScanner.schema, 1, streams)
            } else {
              throw new AimQueryException("Unexpected response from peers")
            }
          }
          case _ ⇒ throw new AimQueryException("Invalid query protocol")
        }

        try {
          StreamUtils.copy(stream, pipe.getOutputStream)
        } finally {
          pipe.flush
          pipe.close
        }
      }
    }

  }

}