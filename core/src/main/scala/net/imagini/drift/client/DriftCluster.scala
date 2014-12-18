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
//  var storageType: Class[_ <: BlockStorage] = classOf[BlockStorageMEMLZ4]
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--zookeeper"    ⇒ zkConnect = argsIterator.next
      case "--host"         ⇒ host = argsIterator.next
      case "--cluster-id"   ⇒ clusterId = argsIterator.next
      case "--first-node"   ⇒ firstNode = argsIterator.next.toInt
      case "--num-nodes"    ⇒ localNumNodes = argsIterator.next.toInt
//      case "--storage-type" ⇒ storageType = Class.forName("net.imagini.drift.utils.BlockStorage" + argsIterator.next).asInstanceOf[Class[BlockStorage]]
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
//  println("Storage type: " + storageType.getSimpleName)
  val localNodes = (1 to localNumNodes).map(n ⇒ new DriftNode(n + firstNode - 1, host + ":" + (port + n - 1).toString, manager))

//  //CREATING TABLES
//  //TODO querysession handle CREATE TABLE
//  manager.createTable("addthis", "views", "at_id(STRING), url(STRING), timestamp(TIME), useragent(STRING)", 10485760, storageType)
//  manager.createTable("addthis", "syncs", "at_id(STRING), vdna_user_uid(UUID), timestamp(TIME)", 10485760, storageType)
//  //  manager.createTable("vdna", "events", "user_uid(UUID), timestamp(LONG), type(STRING), url(STRING)", 100000000, storageType)
//  manager.createTable("vdna", "pageviews", "user_uid(UUID), timestamp(TIME), url(STRING), ip(IPV4), useragent(STRING)", 10485760, storageType)
//  manager.createTable("ips", "test", "ip(IPV4), useragent(STRING), user_uid(UUID)", 10485760, classOf[BlockStorageMEMLZ4])
//  //  manager.createTable("vdna", "syncs", "user_uid(UUID), timestamp(LONG), id_space(STRING), partner_user_id(STRING)", 100000000, storageType)

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