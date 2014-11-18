package net.imagini.aim.client

import grizzled.slf4j.Logger
import net.imagini.aim.utils.BlockStorageMEMLZ4
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.cluster.AimNode
import net.imagini.aim.cluster.DriftManager
import net.imagini.aim.cluster.DriftManagerZk
import net.imagini.aim.utils.BlockStorageMEMLZ4

object DriftCluster extends App {
  val log = Logger[this.type]
  var port: Int = 4000
  var zkConnect: String = "localhost:2181"
  var clusterId = "default"
  var localNumNodes = 1
  val argsIterator = args.iterator
  var storageType: Class[_ <: BlockStorage] = classOf[BlockStorageMEMLZ4]
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--zookeeper" ⇒ zkConnect = argsIterator.next
      case "--cluster-id"      ⇒ clusterId = argsIterator.next
      case "--num-nodes" ⇒ localNumNodes = argsIterator.next.toInt
      case "--storage-type" ⇒ storageType = Class.forName("net.imagini.aim.utils.BlockStorage" + argsIterator.next).asInstanceOf[Class[BlockStorage]]
      case "--port"      ⇒ port = argsIterator.next.toInt
      case arg: String   ⇒ println("Unknown argument " + arg)
    }
  }

  //SPAWNING CLUSTER
  val manager: DriftManager = new DriftManagerZk(zkConnect, clusterId)
  println("Joining drift cluster: "+clusterId + "@"+zkConnect)
  println("Number of local nodes: " + localNumNodes)
  println("Number of total nodes: " + manager.expectedNumNodes)
  println("Storage type: " + storageType.getSimpleName)
  val localNodes = (1 to localNumNodes).map(n ⇒ new AimNode(n, "localhost:" + (port + n - 1).toString, manager))

  //CREATING TABLES
  //TODO querysession handle CREATE TABLE
  manager.createTable("addthis", "views", "at_id(STRING), url(STRING), timestamp(LONG)", 10485760, storageType)
  manager.createTable("addthis", "syncs", "at_id(STRING), vdna_user_uid(UUID:BYTEARRAY[16]), timestamp(LONG)", 10485760, storageType)
  //  manager.createTable("vdna", "events", "user_uid(UUID:BYTEARRAY[16]), timestamp(LONG), type(STRING), url(STRING)", 100000000, storageType)
  manager.createTable("vdna", "pageviews", "user_uid(UUID:BYTEARRAY[16]), timestamp(LONG), url(STRING)", 10485760, storageType)
  //  manager.createTable("vdna", "syncs", "user_uid(UUID:BYTEARRAY[16]), timestamp(LONG), id_space(STRING), partner_user_id(STRING)", 100000000, storageType)

  //ATTACH CONSOLE
  val console = new DriftConsole("localhost", 4000)
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