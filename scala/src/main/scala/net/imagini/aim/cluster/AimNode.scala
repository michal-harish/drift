package net.imagini.aim.cluster

import grizzled.slf4j.Logger
import java.net.InetAddress
import java.net.URI

object AimNode extends App {

  var port: Int = 4000
  var zkConnect: String = "localhost:2181"
  val argsIterator = args.iterator
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--zookeeper" ⇒ zkConnect = argsIterator.next
      case "--port"      ⇒ port = argsIterator.next.toInt
      case arg: String   ⇒ println("Unknown argument " + arg)
    }
  }

  val node1 = new AimNode("1", "localhost:4000", zkConnect)
  val node2 = new AimNode("2", "localhost:4001", zkConnect)
  node1.close
  node2.close
}

class AimNode(val id:String, val address: String, val zkConnect: String) {
  val zkUtils = new ZkUtils(zkConnect)
  val acceptor = new AimNodeAcceptor(new URI("drift://"+address).getPort)
  val log = Logger[AimNode.this.type]
  zkUtils.registerNode(id, address)

  def close = {
    log.info("Aim Node at port " + acceptor.port + " shutting down.")
    zkUtils.unregisterNode(id)
    acceptor.close
    zkUtils.close
  }

}