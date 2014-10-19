package net.imagini.aim.partition

import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.SortOrder
import java.net.ServerSocket
import java.io.IOException
import net.imagini.aim.tools.Protocol
import net.imagini.aim.tools.Pipe

object AimPartitionServer extends App {

  var port = 4000
  var filename: String = null
  var separator = "\n"
  var gzip = false
  var schema: Option[AimSchema] = None
  val argsIterator = args.iterator
  var limit = 0L
  var segmentSize = 10485760
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--segment-size" ⇒ segmentSize = argsIterator.next.toInt
      case "--port"         ⇒ port = argsIterator.next.toInt
      case "--separator"    ⇒ separator = argsIterator.next
      case "--gzip"         ⇒ gzip = true;
      case "--schema"       ⇒ schema = Some(AimSchema.fromString(argsIterator.next))
      case "--filename"     ⇒ filename = argsIterator.next
      case arg: String      ⇒ println("Unknown argument " + arg)
    }
  }

  val key = schema.get.name(0)
  val sortOrder = SortOrder.ASC
  val partition = new AimPartition(schema.get, segmentSize, key, sortOrder)
  val server = new AimPartitionServer(partition, port)

  new AimPartitionLoader("localhost", port, schema.get, separator, filename, gzip).processInput

}

class AimPartitionServer(val partition: AimPartition, val port: Int) {

  val controllerListener = new ServerSocket(port)
  val controllerAcceptor = new Thread {
    override def run = {
      println("Aim Partition Server accepting connections on port " + port + " " + partition.schema)
      try {
        while (!isInterrupted) {
          val socket = controllerListener.accept
          try {
            val pipe = Pipe.open(socket)
            println("Aim Partition Server " + pipe.protocol + " connection from " + socket.getRemoteSocketAddress.toString)
            pipe.protocol match {
              //TODO case Protocol.BINARY: new ReaderThread(); break;
              //TODO case Protocol.MAPREDUCE: new ReaderThread(); break;
              case Protocol.LOADER ⇒ new AimPartitionServerLoaderSession(partition, pipe).start
              case Protocol.QUERY  ⇒ new AimPartitionServerQuerySession(partition, pipe).start
              case _               ⇒ println("Unsupported protocol request " + pipe.protocol)
            }
          } catch {
            case e: IOException ⇒ {
              println("Aim Partition at port " + port + " failed to establish client connection: " + e.getMessage)
            }
          }
        }
      } catch {
        case e: Throwable ⇒ {
          println(e.getMessage)
          controllerListener.close
        }
      }
    }
  }

  controllerAcceptor.start

  def close = {
    controllerAcceptor.interrupt
    controllerListener.close
    System.out.println("Aim Server at port "+port + " shut down.")
  }

} 