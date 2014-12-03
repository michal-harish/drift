package net.imagini.drift.cluster

import net.imagini.drift.types.AimSchema
import grizzled.slf4j.Logger
import net.imagini.drift.utils.BlockStorage
import net.imagini.drift.utils.BlockStorageMEMLZ4
import java.net.URI
import net.imagini.drift.types.AimTableDescriptor

trait DriftManager {
  val log = Logger[this.type]
  val clusterId = "default"
  var expectedNumNodes: Int = -1
  protected def list[T](path: String): Map[String, T]
  protected def get[T](path: String): T
  protected def pathExists(path: String): Boolean
  protected def pathCreatePersistent(path: String, data: Any)
  protected def pathCreateEphemeral(path: String, data: Any)
  protected def pathUpdate(path: String, data: Any)
  protected def pathDelete(path: String): Boolean
  protected def watchPathData[T](path: String, listener: (Option[T] ⇒ Unit))
  protected def watchPathChildren[T](path: String, listener: (Map[String, T]) ⇒ Unit)
  def close
  final protected[drift] def watchData[T](path: String, listener: (Option[T] ⇒ Unit)) = {
    watchPathData("/drift/" + clusterId + path, listener)
  }
  final protected[drift] def watch[T](path: String, listener: (Map[String, T]) ⇒ Unit) = {
    watchPathChildren("/drift/" + clusterId + path, listener)
  }

  final def getNodeConnectors: Map[Int, URI] = {
    val nodeConnectors = list[String]("/drift/" + clusterId + "/nodes").map(p ⇒ p._1.toInt -> new URI("drift://" + p._2)).toMap
    if (nodeConnectors.size != expectedNumNodes) throw new IllegalStateException("Expecting " + expectedNumNodes + " nodes, found " + nodeConnectors.size)
    nodeConnectors
  }

  final def getDescriptor(keyspace: String, table: String): AimTableDescriptor = {
    new AimTableDescriptor(get[String]("/drift/" + clusterId +"/keyspaces/" + keyspace + "/" + table))
  }

  final def clusterIsSuspended: Boolean = {
    //TODO replicate behaviour from the AimNode
    false
  }

  final def init = {
    if (!pathExists("/drift")) {
      pathCreatePersistent("/drift", "")
    }
    if (!pathExists("/drift/" + clusterId)) {
      pathCreatePersistent("/drift/" + clusterId, "")
      pathCreatePersistent("/drift/" + clusterId + "/nodes", "1")
      pathCreatePersistent("/drift/" + clusterId + "/keyspaces", "")
    }
    watchData("/nodes", (num: Option[String]) ⇒ num match {
      case Some(n) ⇒ expectedNumNodes = Integer.valueOf(n)
      case None    ⇒ expectedNumNodes = -1
    })
  }

  final def down = {
    for (id ← (1 to expectedNumNodes)) unregisterNode(id)
  }

  final def setNumNodes(totalNodes: Int) {
    pathUpdate("/drift/" + clusterId + "/nodes", totalNodes.toString)
  }

  final def createTable(keyspace: String, name: String, schemaDeclaration: String) {
    createTable(keyspace, name, schemaDeclaration, 10485760, classOf[BlockStorageMEMLZ4])
  }

  final def createTable(keyspace: String, name: String, schemaDeclaration: String, segmentSize: Int, storage: Class[_ <: BlockStorage]) {
    AimSchema.fromString(schemaDeclaration)
    val keyspacePath = "/drift/" + clusterId + "/keyspaces/" + keyspace
    if (!pathExists(keyspacePath)) pathCreatePersistent(keyspacePath, "")
    val tablePath = keyspacePath + "/" + name
    if (!pathExists(tablePath)) {
      pathCreatePersistent(tablePath, schemaDeclaration + "\n" + segmentSize.toString + "\n" + storage.getName)
    }
  }

  final def registerNode(id: Int, address: String): Boolean = {
    val nodePath = "/drift/" + clusterId + "/nodes/" + id.toString
    log.info("registering node " + id)
    for (i ← (1 to 30)) {
      if (pathExists(nodePath)) Thread.sleep(1000) else {
        pathCreateEphemeral(nodePath, address)
        return true
      }
    }
    false
  }

  final def unregisterNode(id: Int) = {
    pathDelete("/drift/" + clusterId + "/nodes/" + id.toString)
  }

}