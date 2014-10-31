package net.imagini.aim.cluster

import scala.collection.JavaConverters.asScalaBufferConverter
import org.I0Itec.zkclient.IZkChildListener
import org.I0Itec.zkclient.IZkDataListener
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.CreateMode
import java.util.concurrent.ConcurrentHashMap
import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentMap
import scala.collection.JavaConverters._
import grizzled.slf4j.Logger
import net.imagini.aim.types.AimSchema

class DriftManager(val zkConnect: String, val totalNodes: Int) {
  val log = Logger[AimNode]
  val zkClient = new ZkClient(zkConnect)
  if (!zkClient.exists("/drift")) {
    zkClient.create("/drift", "", CreateMode.PERSISTENT)
    zkClient.create("/drift/nodes", totalNodes, CreateMode.PERSISTENT)
    zkClient.create("/drift/keyspaces", "", CreateMode.PERSISTENT)
  }

  def close = {
    zkClient.close
  }
  def createTable(keyspace: String, name: String, schemaDeclaration: String, ifNotExists: Boolean) {
    AimSchema.fromString(schemaDeclaration)
    val keyspacePath = "/drift/keyspaces/" + keyspace
    if (!zkClient.exists(keyspacePath)) zkClient.create(keyspacePath, "", CreateMode.PERSISTENT)
    val tablePath = keyspacePath + "/" + name
    if (zkClient.exists(tablePath)) ifNotExists match {
      case false ⇒ throw new IllegalStateException("Table already exists")
      case true  ⇒ {}
    }
    else {
      zkClient.createPersistent(tablePath, schemaDeclaration)
    }
  }
  def registerNode(id: Int, address: String): Boolean = {
    val nodePath = "/drift/nodes/" + id.toString
    for (i ← (1 to 30)) {
      if (zkClient.exists(nodePath)) Thread.sleep(1000) else {
        zkClient.create(nodePath, address, CreateMode.EPHEMERAL)
        return true
      }
    }
    false
  }

  def unregisterNode(id: Int) = {
    zkClient.delete("/drift/nodes/" + id.toString)
  }

  def watchData[T](zkPath: String, listener: (Option[T] ⇒ Unit)) {
    zkClient.subscribeDataChanges(zkPath, new IZkDataListener {
      override def handleDataChange(zkPath: String, data: Object) = listener(Some(data.asInstanceOf[T]))
      override def handleDataDeleted(zkPath: String) = listener(None)
    });
    listener(Some(zkClient.readData(zkPath)))
  }
  def watch[T](zkPath: String, listener: (Map[String, T]) ⇒ Unit) {
    zkClient.subscribeChildChanges(zkPath, new IZkChildListener {
      override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]) = update(zkPath, currentChilds, listener);
    })
    update(zkPath, zkClient.getChildren(zkPath), listener)
  }
  private def update[T](zkPath: String, children: java.util.List[String], listener: (Map[String, T]) ⇒ Unit) = {
    listener(zkClient.getChildren(zkPath).asScala.map(p ⇒ p -> zkClient.readData[T](zkPath + "/" + p)).toMap)
  }

}