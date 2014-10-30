package net.imagini.aim.cluster

import java.net.ServerSocket
import java.io.IOException
import net.imagini.aim.tools.Pipe
import grizzled.slf4j.Logger

class AimNodeAcceptor(listenPort: Int) extends Thread {
  val log = Logger[AimNodeAcceptor.this.type]
  val controllerListener = new ServerSocket(listenPort)
  val port = controllerListener.getLocalPort
  start
  println("Drift Node accepting connections on port " + port)


  def close = {
    interrupt
    controllerListener.close
  }

  override def run = {
    try {
      while (!isInterrupted) {
        val socket = controllerListener.accept
        try {
          val pipe = Pipe.open(socket)
          println("Aim Partition Server " + pipe.protocol + " connection from " + socket.getRemoteSocketAddress.toString)
          pipe.protocol match {
            //TODO case Protocol.DATA: new ReaderThread(); break; //for between-node communication
            //TODO case Protocol.MAPREDUCE: new ReaderThread(); break;
            //                      case Protocol.LOADER ⇒ new AimPartitionServerLoaderSession(partition, pipe).start
            //                      case Protocol.QUERY  ⇒ new AimPartitionServerQuerySession(partition, pipe).start
            case _ ⇒ println("Unsupported protocol request " + pipe.protocol)
          }
        } catch {
          case e: IOException ⇒ {
            println("Aim Partition at port " + port + " failed to establish client connection: " + e.getMessage)
          }
        }
      }
    } catch {
      case e: Throwable ⇒ {
        log.error(e)
        controllerListener.close
      }
    }
  }
}