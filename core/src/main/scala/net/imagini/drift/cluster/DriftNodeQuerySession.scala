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
import net.imagini.drift.utils.BlockStorageFS
import net.imagini.drift.utils.BlockStorage
import net.imagini.drift.utils.BlockStorageMEMLZ4
import net.imagini.drift.types.DriftSchema

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
        case ddlQuery: String if (ddlQuery.toUpperCase.startsWith("CREATE")) ⇒ create(ddlQuery)
        //case ddlQuery: String if (ddlQuery.toUpperCase.startsWith("DROP")) ⇒ create(ddlQuery)
        case dataQuery: String ⇒
          if (pipe.protocol == QUERY_USER) {
            peerClients.foreach(peer ⇒ peer.query(dataQuery))
          }
          dataQuery match {
            case command: String if (command.toUpperCase.startsWith("CLOSE")) ⇒ close
            case query: String if (query.toUpperCase.startsWith("STAT")) ⇒ handleSelectStream(node.stats(dataQuery.substring(5).trim))
            //case query:String if (query.toUpperCase.startsWith("DELETE"))
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
      "CREATE [MEM] TABLE <keyspace>.<table_name> <schema>", //TODO WITH SEGMENT_SIZE=<int_inflated_bytes>
      "CLUSTER DOWN",
      "CLUSTER numNodes <int_total_nodes>",
      "CLUSTER",
      "CLOSE",
      "DESCRIBE",
      "DESCRIBE <keyspace>",
      "DESCRIBE <keyspace>.<table>",
      "STAT",
      "STAT <keyspace>",
      "COUNT {<keyspace>.<table>|(SELECT ..)} [WHERE <boolean_exp>]",
      "SELECT <fields> FROM {<keyspace>.<table>|(SELECT ..)} [WHERE <boolean_exp>] [JOIN SELECT ..] [UNION SELECT ..] [INTERSECTION SELECT ..]",
      "SELECT ... INTO <keyspace>.<table>",
      "~DROP <keyspace>.<table>",
      "~DELETE <keyspace>.<table>")
  }

  private def create(ddlQuery: String) = {
    val cmd = Tokenizer.tokenize(ddlQuery, true)
    if (!cmd.poll.toUpperCase.equals("CREATE")) throw new DriftQueryException("Expecting CREATE [MEM] TABLE ...")
    var storageType: Class[_ <: BlockStorage] = classOf[BlockStorageFS]
    cmd.poll.toUpperCase match {
      case "MEM" => {
        storageType = classOf[BlockStorageMEMLZ4]
        if (!cmd.poll.toUpperCase.equals("TABLE")) throw new DriftQueryException("Expecting CREATE [MEM] TABLE ...")
      }
      case "TABLE" => {}
      case _ => throw new DriftQueryException("Expecting CREATE [MEM] TABLE ...")
    }
    val keyspace = cmd.poll
    if (!cmd.poll.equals(".")) throw new DriftQueryException("Expecting CREATE [MEM] TABLE <keyspace>.<table> <schema>")
    val table = cmd.poll
    val schema = DriftSchema.fromTokens(cmd)
    node.manager.createTable(keyspace, table, schema, 10485760, storageType)
    describe("DESCRIBE " + keyspace + "." + table)
  }

  private def describe(ddlQuery: String) = {
    val cmd = Tokenizer.tokenize(ddlQuery, true)
    if (!cmd.poll.toUpperCase.equals("DESCRIBE")) throw new DriftQueryException("Expecting DESCRIBE [<keyspace>[.<table>]]")
    if (cmd.isEmpty) {
      //describe cluster
      pipe.write("OK")
      pipeWriteInfo(
        "cluster id=" + node.manager.clusterId,
        "cluster numNodes=" + node.manager.expectedNumNodes,
        "cluster avialble nodes=" + (node.peers.size + 1),
        "cluster defined keyspaces: " + node.keyspaces.mkString(", "))
    } else {
      val keyspace = cmd.poll
      if (cmd.isEmpty()) {
        //describe keyspace
        pipe.write("OK")
        pipeWriteInfo(
          "keyspace: " + keyspace,
          "tables: " + node.manager.listTables(keyspace).mkString(", "))
      } else {
        //describe table
        cmd.poll
        val table = cmd.poll
        val descriptor = node.manager.getDescriptor(keyspace, table)
        pipe.write("OK")
        pipeWriteInfo(
          "keyspace: " + keyspace,
          "table: " + table,
          "schema: " + descriptor.schema.toString,
          "sort type: " + descriptor.sortType,
          "storage type: " + descriptor.storageType.getSimpleName,
          "segment size: " + descriptor.segmentSize
          )
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