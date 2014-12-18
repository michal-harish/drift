package net.imagini.drift.client

import grizzled.slf4j.Logger
import net.imagini.drift.utils.BlockStorageMEMLZ4
import net.imagini.drift.utils.BlockStorage
import net.imagini.drift.cluster.DriftNode
import net.imagini.drift.cluster.DriftManager
import net.imagini.drift.cluster.DriftManagerZk
import net.imagini.drift.utils.BlockStorageMEMLZ4

object DriftCluster extends App {
  val log = Logger[this.type]
  var host = "localhost"
  var port: Int = 4000
  var zkConnect: String = "localhost:2181"
  var clusterId:String = null
  var localNumNodes = 1
  var firstNode = 1
  val argsIterator = args.iterator
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--zookeeper"    ⇒ zkConnect = argsIterator.next
      case "--host"         ⇒ host = argsIterator.next
      case "--cluster-id"   ⇒ clusterId = argsIterator.next
      case "--first-node"   ⇒ firstNode = argsIterator.next.toInt
      case "--num-nodes"    ⇒ localNumNodes = argsIterator.next.toInt
      case "--port"         ⇒ port = argsIterator.next.toInt
      case arg: String      ⇒ {
        println("Unknown argument " + arg)
        System.exit(2)
      }
    }
  }
  if (clusterId == null) {
    println("Missing --cluster-id argument")
    System.exit(1)
  }

  //SPAWNING CLUSTER
  val manager: DriftManager = new DriftManagerZk(zkConnect, clusterId)
  println("Joining drift cluster: " + clusterId + "@" + zkConnect)
  println("Number of local nodes: " + localNumNodes)
  println("Number of total nodes: " + manager.expectedNumNodes)
  val localNodes = (1 to localNumNodes).map(n ⇒ new DriftNode(n + firstNode - 1, host + ":" + (port + n - 1).toString, manager))

  //ATTACH CONSOLE
  val console = new DriftConsole("localhost", port)
  console.start

  //WAIT FOR DISTRIBUTED SHUTDOWN
  if (localNodes.forall(_.isShutdown)) {
    manager.close
    console.close
    System.exit(0)
  }
  log.debug("Main thread premature exit")
  System.exit(1)
}