package net.imagini.aim.cluster

import net.imagini.aim.types.AimSchema
import grizzled.slf4j.Logger
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.utils.BlockStorageLZ4

trait DriftManager {
  val log = Logger[this.type]
  val root = "/drift/default"
  var expectedNumNodes: Int = -1
  protected def pathExists(path: String): Boolean
  protected def pathCreatePersistent(path: String, data: Any)
  protected def pathCreateEphemeral(path: String, data: Any)
  protected def pathUpdate(path: String, data: Any)
  protected def pathDelete(path: String): Boolean
  protected def watchPathData[T](path: String, listener: (Option[T] ⇒ Unit))
  protected def watchPathChildren[T](path: String, listener: (Map[String, T]) ⇒ Unit)
  def close
  final protected[cluster] def watchData[T](path: String, listener: (Option[T] ⇒ Unit)) = {
    watchPathData(root + path, listener)
  }
  final protected[cluster] def watch[T](path: String, listener: (Map[String, T]) ⇒ Unit) = {
    watchPathChildren(root + path, listener)
  }

  final def init = {
    if (!pathExists(root)) {
      pathCreatePersistent(root, "")
      pathCreatePersistent(root + "/nodes", "1")
      pathCreatePersistent(root + "/keyspaces", "")
    }
  }

  final def down = {
    for (id ← (1 to expectedNumNodes)) unregisterNode(id)
  }

  final def setNumNodes(totalNodes: Int) {
    pathUpdate(root + "/nodes", totalNodes.toString)
  }

  final def createTable(keyspace: String, name: String, schemaDeclaration: String) {
    createTable(keyspace, name, schemaDeclaration, 100000000, classOf[BlockStorageLZ4])
  }

  final def createTable(keyspace: String, name: String, schemaDeclaration: String, segmentSize: Int, storage: Class[_ <: BlockStorage]) {
    AimSchema.fromString(schemaDeclaration)
    val keyspacePath = root + "/keyspaces/" + keyspace
    if (!pathExists(keyspacePath)) pathCreatePersistent(keyspacePath, "")
    val tablePath = keyspacePath + "/" + name
    if (!pathExists(tablePath)) {
      pathCreatePersistent(tablePath, schemaDeclaration + "\n" + segmentSize.toString + "\n" + storage.getName)
    }
  }

  final def registerNode(id: Int, address: String): Boolean = {
    val nodePath = root + "/nodes/" + id.toString
    for (i ← (1 to 30)) {
      if (pathExists(nodePath)) Thread.sleep(1000) else {
        pathCreateEphemeral(nodePath, address)
        return true
      }
    }
    false
  }

  final def unregisterNode(id: Int) = {
    pathDelete(root + "/nodes/" + id.toString)
  }

}