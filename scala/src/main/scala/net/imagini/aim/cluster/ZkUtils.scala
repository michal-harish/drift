package net.imagini.aim.cluster

import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.CreateMode
import org.I0Itec.zkclient.IZkChildListener
import scala.collection.JavaConverters._

class ZkUtils(val zkConnect: String) {
  val zkClient = new ZkClient(zkConnect)
  val id =
    if (!zkClient.exists("/drift")) {
      zkClient.create("/drift", "", CreateMode.PERSISTENT)
      zkClient.create("/drift/nodes", "", CreateMode.PERSISTENT)
      zkClient.create("/drift/keyspaces", "", CreateMode.PERSISTENT)
    }

  def registerNode(id: String, address: String) = {
    zkClient.create("/drift/nodes/" + id, address, CreateMode.EPHEMERAL)
    watch("/drift/nodes", (children: Seq[String]) ⇒ println(children./*filter(!_.equals(id))*/foldLeft("")(_ + _ + " ")))
  }

  def unregisterNode(id: String) = {
    zkClient.delete("/drift/nodes/" + id)
  }

  def get(zkPath: String): String = zkClient.readData("/drift/nodes/" + id)

  def watch(zkPath: String, listener: (Seq[String]) ⇒ Unit) {
    zkClient.subscribeChildChanges(zkPath, new IZkChildListener() {
      override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]) {
        listener(currentChilds.asScala.toSeq);
      }
    })
    listener(zkClient.getChildren(zkPath).asScala.toSeq)
  }

  def close = {
    zkClient.close
  }
}