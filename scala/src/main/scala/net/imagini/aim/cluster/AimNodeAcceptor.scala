package net.imagini.aim.cluster

import java.net.ServerSocket
import java.io.IOException
import grizzled.slf4j.Logger

class AimNodeAcceptor(val node: AimNode, listenPort: Int) extends Thread {
  val log = Logger[this.type]
  val controllerListener = new ServerSocket(listenPort)
  val port = controllerListener.getLocalPort
  start
  private[cluster] def close = {
    interrupt
    controllerListener.close
  }

  override def run = {
    try {
      while (!isInterrupted) {
        val socket = controllerListener.accept
        try {
          val pipe = new Pipe(socket)
          log.debug("Node " + pipe.protocol + " connection from " + socket.getRemoteSocketAddress.toString)
          pipe.protocol match {
            case Protocol.LOADER_USER | Protocol.LOADER_INTERNAL ⇒ if (node.isSuspended) interrupt else node.session(new AimNodeLoaderSession(node, pipe))
            case Protocol.QUERY_USER | Protocol.QUERY_INTERNAL ⇒ node.session(new AimNodeQuerySession(node, pipe))
            case _ ⇒ log.debug("Unsupported protocol request " + pipe.protocol)
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