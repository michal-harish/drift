package net.imagini.aim.cluster

import net.imagini.aim.types.AimSchema

trait DriftManager {

  protected def pathExists(path: String): Boolean
  protected def pathCreatePersistent(path: String, data: Any)
  protected def pathCreateEphemeral(path: String, data: Any)
  protected def pathDelete(path: String): Boolean
  protected[cluster] def watchData[T](zkPath: String, listener: (Option[T] ⇒ Unit))
  protected[cluster] def watch[T](zkPath: String, listener: (Map[String, T]) ⇒ Unit)
  def close

  final def createTable(keyspace: String, name: String, schemaDeclaration: String, ifNotExists: Boolean) {
    AimSchema.fromString(schemaDeclaration)
    val keyspacePath = "/drift/keyspaces/" + keyspace
    if (!pathExists(keyspacePath)) pathCreatePersistent(keyspacePath, "")
    val tablePath = keyspacePath + "/" + name
    if (pathExists(tablePath)) ifNotExists match {
      case false ⇒ throw new IllegalStateException("Table already exists")
      case true  ⇒ {}
    }
    else {
      pathCreatePersistent(tablePath, schemaDeclaration)
    }
  }

  final def registerNode(id: Int, address: String): Boolean = {
    if (!pathExists("/drift")) {
      pathCreatePersistent("/drift", "")
      pathCreatePersistent("/drift/nodes", 1)
      pathCreatePersistent("/drift/keyspaces", "")
    }
    val nodePath = "/drift/nodes/" + id.toString
    for (i ← (1 to 30)) {
      if (pathExists(nodePath)) Thread.sleep(1000) else {
        pathCreateEphemeral(nodePath, address)
        return true
      }
    }
    false
  }

  final def unregisterNode(id: Int) = {
    pathDelete("/drift/nodes/" + id.toString)
  }

}