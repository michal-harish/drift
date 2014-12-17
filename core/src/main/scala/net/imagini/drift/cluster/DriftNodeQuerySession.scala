package net.imagini.drift.cluster

import java.io.EOFException
import java.io.IOException
import java.net.SocketException
import net.imagini.drift.segment.AbstractScanner
import net.imagini.drift.segment.CountScanner
import net.imagini.drift.types.Drift
import net.imagini.drift.utils.Tokenizer
import net.imagini.drift.types.DriftQueryException
import net.imagini.drift.client.DriftClient
import net.imagini.drift.cluster.Protocol._
import net.imagini.drift.segment.TransformScanner

class DriftNodeQuerySession(override val node: DriftNode, override val pipe: Pipe) extends DriftNodeSession {

  private val peerClients: Seq[DriftClient] = if (pipe.protocol.equals(QUERY_INTERNAL)) Seq() else node.peers.map(peer ⇒ {
    new DriftClient(peer._2.getHost, peer._2.getPort, QUERY_INTERNAL)
  }).toSeq

  override def accept = {
    try {
      pipe.read.trim match {
        case adminQuery: String if (adminQuery.toUpperCase.startsWith("CLUSTER")) ⇒ clusterProps(adminQuery)
        case helpQuery: String if (helpQuery.toUpperCase.startsWith("HELP")) ⇒ helpInfo()
        case ddlQuery: String if (ddlQuery.toUpperCase.startsWith("DESCRIBE")) ⇒ describe(ddlQuery)
        case dataQuery: String ⇒
          if (pipe.protocol == QUERY_USER) {
            peerClients.foreach(peer ⇒ peer.query(dataQuery))
          }
          dataQuery match {
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
      "Available commands:\n",
      "DESCRIBE",
      "DESCRIBE <keyspace>",
      "DESCRIBE <keyspace>.<table>",
      "STAT",
      "STAT <keyspace>",
      "SELECT <fields> FROM {<keyspace>.<table>|(SELECT ..)} [WHERE <boolean_exp>] [JOIN SELECT ..] [UNION SELECT ..] [INTERSECTION SELECT ..]",
      "COUNT {<keyspace>.<table>|(SELECT ..)} [WHERE <boolean_exp>]",
      "CLUSTER DOWN",
      "CLUSTER numNodes <int_total_nodes>",
      "CLUSTER",
      "CLOSE",
      "~SELECT ... INTO <keyspace>.<table>",
      "~CREATE TABLE <keyspace>.<table_name> <schema> WITH STORAGE=[MEM|MEMLZ4|FSLZ4], SEGMENT_SIZE=<int_inflated_bytes>",
      "~DROP <keyspace>.<table>",
      "~TRUNCATE <keyspace>.<table>")
  }

  private def describe(ddlQuery: String) = {
    val cmd = Tokenizer.tokenize(ddlQuery, true)
    cmd.poll
    pipe.write("OK")
    if (cmd.isEmpty) {
      //describe cluster
      pipeWriteInfo(
        "cluster id=" + node.manager.clusterId,
        "cluster numNodes=" + node.manager.expectedNumNodes,
        "cluster avialble nodes=" + (node.peers.size + 1),
        "cluster defined keyspaces: " + node.keyspaces.mkString(", "))
    } else {
      val keyspace = cmd.poll
      if (cmd.isEmpty()) {
        //describe keyspace
        pipeWriteInfo(
          "keyspace: " + keyspace,
          "tables: " + node.manager.listTables(keyspace).mkString(", "))
      } else {
        //describe table
        cmd.poll
        val table = cmd.poll
        val descriptor = node.manager.getDescriptor(keyspace, table)
        pipeWriteInfo(
          "keyspace: " + keyspace,
          "table: " + table,
          "sechema: " + descriptor.schema.toString)
      }
    }
  }

  private def clusterProps(command: String) = {
    val cmd = Tokenizer.tokenize(command, true)
    cmd.poll
    while (!cmd.isEmpty) cmd.poll.toUpperCase match {
      case "DOWN" ⇒ node.manager.down
      case "NUMNODES" ⇒ node.manager.setNumNodes(cmd.poll.toInt)
    }
    pipe.write("OK")
    pipeWriteInfo(
      "cluster id=" + node.manager.clusterId,
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
      case localScanner: TransformScanner => {
        pipe.write("COUNT")
        pipe.flush
        val transformedCount: Long = pipe.protocol match {
          case QUERY_INTERNAL ⇒ node.transform(localScanner)
          case QUERY_USER ⇒ {
            val localCount = node.transform(localScanner)
            peerClients.map(peer ⇒ peer.getCount).foldLeft(0L)(_ + _) + localCount
          }
          case _ ⇒ throw new DriftQueryException("Invalid query protocol")
        }
        pipe.writeInt(transformedCount.toInt)
        pipe.flush
      }
      case localScanner: CountScanner ⇒ {
        pipe.write("COUNT")
        pipe.flush
        val count: Long = pipe.protocol match {
          case QUERY_INTERNAL ⇒ localScanner.count
          case QUERY_USER ⇒ {
            peerClients.map(peer ⇒ peer.getCount).foldLeft(0L)(_ + _) + localScanner.count
          }
          case _ ⇒ throw new DriftQueryException("Invalid query protocol")
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
                new StreamMerger(localScanner.schema, 10, streams)
                //TODO queue size=10 must be exposed in the keyspace/table config
              }
            } else {
              throw new DriftQueryException("Unexpected response from peers")
            }
          }
          case _ ⇒ throw new DriftQueryException("Invalid query protocol")
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