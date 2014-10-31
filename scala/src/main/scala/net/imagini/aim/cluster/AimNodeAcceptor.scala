package net.imagini.aim.cluster

import java.net.ServerSocket
import java.io.IOException
import net.imagini.aim.tools.Pipe
import grizzled.slf4j.Logger

class AimNodeAcceptor(val node: AimNode, listenPort: Int) extends Thread {
  val log = Logger[AimNodeAcceptor.this.type]
  val controllerListener = new ServerSocket(listenPort)
  val port = controllerListener.getLocalPort
  start
  private[cluster] def close = {
    interrupt
    controllerListener.close
  }

  override def run = {
    try {
      while (!isInterrupted && !node.isShutdown) {
        val socket = controllerListener.accept
        try {
          val pipe = Pipe.open(socket)
          log.debug("Node " + pipe.protocol + " connection from " + socket.getRemoteSocketAddress.toString)
          pipe.protocol match {
            //TODO case Protocol.DATA: new ReaderThread(); break; //for between-node communication
            //TODO case Protocol.MAPREDUCE: new ReaderThread(); break;
            //            case Protocol.LOADER ⇒ new AimNodeServerLoaderSession(partition, pipe).start
            case Protocol.QUERY ⇒ node.session(new AimNodeQuerySession(node, pipe))
            case _              ⇒ log.debug("Unsupported protocol request " + pipe.protocol)
          }
        } catch {
          case e: IOException ⇒ log.error("Node at port " + port + " failed to establish client connection: ", e)
        }
      }
    } catch {
      case e: java.net.SocketException ⇒ {}
      case e: Throwable ⇒ {
        log.error(e)
        controllerListener.close
      }
    }
  }
}