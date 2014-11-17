package net.imagini.aim.cluster

import java.io.EOFException
import java.io.IOException
import java.net.SocketException
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.tools.CountScanner
import net.imagini.aim.types.Aim
import net.imagini.aim.utils.Tokenizer
import net.imagini.aim.types.AimQueryException
import net.imagini.aim.client.DriftClient
import net.imagini.aim.cluster.Protocol._

class AimNodeQuerySession(override val node: AimNode, override val pipe: Pipe) extends AimNodeSession {

  private val peerClients: Seq[DriftClient] = if (pipe.protocol.equals(QUERY_INTERNAL)) Seq() else node.peers.map(peer ⇒ {
    new DriftClient(peer._2.getHost, peer._2.getPort, QUERY_INTERNAL)
  }).toSeq

  override def accept = {
    try {
      pipe.read.trim match {
        case adminQuery: String if (adminQuery.toUpperCase.startsWith("CLUSTER")) ⇒ clusterProps(adminQuery)
        case helpQuery: String if (helpQuery.toUpperCase.startsWith("HELP"))      ⇒ helpInfo()
        case dataQuery: String ⇒
          if (pipe.protocol == QUERY_USER) {
            peerClients.foreach(peer ⇒ peer.query(dataQuery))
          }
          dataQuery match {
            case command: String if (command.toUpperCase.startsWith("TRANSFORM")) ⇒ {
              pipe.write("COUNT")
              pipe.flush
              val transformedCount: Long = pipe.protocol match {
                case QUERY_INTERNAL ⇒ node.transform("select vdna_user_uid from addthis.syncs join select timestamp,url from addthis.views", "vdna", "pageviews")
                case QUERY_USER ⇒ {
                  val localCount = node.transform("select vdna_user_uid from addthis.syncs join select timestamp,url from addthis.views", "vdna", "pageviews")
                  peerClients.map(peer ⇒ peer.getCount).foldLeft(0L)(_ + _) + localCount
                }
                case _ ⇒ throw new AimQueryException("Invalid query protocol")
              }
              pipe.writeInt(transformedCount.toInt)
              pipe.flush

            }
            case command: String if (command.toUpperCase.startsWith("CLOSE")) ⇒ close
            case query: String if (query.toUpperCase.startsWith("STAT")) ⇒ handleSelectStream(node.stats(dataQuery.substring(5).trim))
            case query: String ⇒ handleSelectStream(node.query(query))
          }
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

  private def helpInfo() = {
    pipe.write("OK")
    pipeWriteInfo(
      "Available commands:\n", "STAT", "STAT <keyspace>", "SELECT <fields> FROM {<keyspace>.<table>|(SELECT ..)} [WHERE <boolean_exp>] [JOIN SELECT ..] [UNION SELECT ..] [INTERSECTION SELECT ..]", "COUNT {<keyspace>.<table>|(SELECT ..)} [WHERE <boolean_exp>]", "~SELECT ... INTO <keyspace>.<table>", "~CREATE TABLE <keyspace>.<table_name> <schema> WITH STORAGE=[MEM|MEMLZ4|FSLZ4], SEGMENT_SIZE=<int_inflated_bytes>", "CLUSTER DOWN", "CLUSTER numNodes <int_total_nodes>", "CLUSTER", "CLOSE")
  }
  private def clusterProps(command: String) = {
    val cmd = Tokenizer.tokenize(command, true)
    cmd.poll
    while (!cmd.isEmpty) cmd.poll.toUpperCase match {
      case "DOWN"     ⇒ node.manager.down
      case "NUMNODES" ⇒ node.manager.setNumNodes(cmd.poll.toInt)
    }
    pipe.write("OK")
    pipeWriteInfo(
      "cluster numNodes=" + node.manager.expectedNumNodes,
      "cluster avialble nodes=" + (node.peers.size + 1),
      "cluster defined keyspaces: " + node.keyspaces.mkString(", "))
  }

  private def pipeWriteInfo(info: String*) = {
    pipe.writeInt(info.length)
    info.map(i ⇒ pipe.write(i))
    pipe.flush
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
              if (peerClients.size == 0) {
                new ScannerInputStream(localScanner)
              } else {
                val streams = peerClients.map(peer ⇒ peer.getInputStream).toArray ++ Array(new ScannerInputStream(localScanner))
                new StreamMerger(localScanner.schema, 16, streams)
                //TODO queue size=100 must be exposed in the keyspace/table config
              }
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