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

class DriftManagerZk(val zkConnect: String, val totalNodes: Int) extends DriftManager {
  val log = Logger[AimNode]
  val zkClient = new ZkClient(zkConnect)
  init(totalNodes)
  override protected def pathExists(path: String) = zkClient.exists(path)
  override protected def pathCreatePersistent(path: String, data:Any) = zkClient.create(path, data, CreateMode.PERSISTENT)
  override protected def pathCreateEphemeral(path: String, data:Any) = zkClient.create(path, data, CreateMode.EPHEMERAL)
  override def close = zkClient.close
  override protected def pathDelete(path: String) = zkClient.delete(path)

  override def watchData[T](zkPath: String, listener: (Option[T] ⇒ Unit)) {
    zkClient.subscribeDataChanges(zkPath, new IZkDataListener {
      override def handleDataChange(zkPath: String, data: Object) = listener(Some(data.asInstanceOf[T]))
      override def handleDataDeleted(zkPath: String) = listener(None)
    });
    listener(Some(zkClient.readData(zkPath)))
  }
  override def watch[T](zkPath: String, listener: (Map[String, T]) ⇒ Unit) {
    zkClient.subscribeChildChanges(zkPath, new IZkChildListener {
      override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]) = update(zkPath, currentChilds, listener);
    })
    update(zkPath, zkClient.getChildren(zkPath), listener)
  }
  private def update[T](zkPath: String, children: java.util.List[String], listener: (Map[String, T]) ⇒ Unit) = {
    listener(zkClient.getChildren(zkPath).asScala.map(p ⇒ p -> zkClient.readData[T](zkPath + "/" + p)).toMap)
  }

}