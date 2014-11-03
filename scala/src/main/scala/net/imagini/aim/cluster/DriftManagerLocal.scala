package net.imagini.aim.cluster

import net.imagini.aim.types.AimSchema
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.MultiMap
import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import grizzled.slf4j.Logger

class DriftManagerLocal extends DriftManager {
  val log = Logger[DriftManagerLocal.this.type]

  val tree = HashMap[String, Any]()
  val dataWatchers = new HashMap[String, Set[(Option[Any]) ⇒ Unit]] with MultiMap[String, (Option[Any]) ⇒ Unit]
  val pathWatchers = new HashMap[String, Set[(Map[String, Any]) ⇒ Unit]] with MultiMap[String, (Map[String, Any]) ⇒ Unit]
  init(1)
  override def close = {}
  override protected def pathExists(path: String): Boolean = {
    log.debug("EXISTS ? " + path + " " + tree.contains(path))
    tree.contains(path)
  }
  override protected def pathCreatePersistent(path: String, data: Any) = {
    log.debug("CREATE PERSISTENT " + path + " " + data)
    tree.put(path, data)
    notifyWatchers(path)
  }
  override protected def pathCreateEphemeral(path: String, data: Any) = {
    log.debug("CREATE EPHEMERAL " + path + " " + data)
    tree.put(path, data)
    notifyWatchers(path)
  }
  override protected def pathDelete(path: String): Boolean = {
    log.debug("DELETE " + path)
    tree.remove(path)
    notifyDataWatchers(path)
    notifyWatchers(path);
    true
  }
  override protected[cluster] def watchData[T](path: String, listener: (Option[T] ⇒ Unit)) = {
    log.debug("WATCHING DATA " + path)
    dataWatchers.addBinding(path, listener.asInstanceOf[(Option[_]) ⇒ Unit])
    listener(tree.get(path).asInstanceOf[Option[T]])
  }
  override protected[cluster] def watch[T](path: String, listener: (Map[String, T]) ⇒ Unit) = {
    log.debug("WATCHING PATHS " + path)
    pathWatchers.addBinding(path, listener.asInstanceOf[(Map[String, _]) ⇒ Unit])
    val children = tree.filter(e ⇒ e._1.matches("^" + path + "/[^/]+$")).map(e ⇒ e._1.split("/").last -> e._2.asInstanceOf[T]).toMap
    listener(children)
  }
  private def notifyDataWatchers(path: String) = {
    log.debug("NOTIFY DATA " + path)
    if (dataWatchers.contains(path)) {
      dataWatchers(path).foreach(w ⇒ w(tree.get(path)))
    }
  }
  private def notifyWatchers(path: String) = {
    val parentPath = path.split("/").dropRight(1).mkString("/")
    log.debug("NOTIFY CHILDREN " + parentPath)
    if (pathWatchers.contains(parentPath)) {
      val children = tree.filter(e ⇒ e._1.matches("^" + parentPath + "/[^/]+$")).map(e ⇒ e._1.split("/").last -> e._2).toMap
      pathWatchers(parentPath).foreach(w ⇒ w(children))
    }
  }
}