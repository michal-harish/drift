package net.imagini.aim.cluster

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.scalatest.Finders

class ManagerLocalTest extends FlatSpec with Matchers {

  var notification: Option[String] = None
  "Local Manager " should " immitate zk behaviours" in {
    val manager = new DriftManagerLocal(1)
    manager.watch("/keyspaces", (children: Map[String, String]) ⇒ {
      children.keys.map(k ⇒ {
        manager.watch("/keyspaces/" + k, (tables: Map[String, String]) ⇒ {
          tables.map(t ⇒ {
            println(t._2)
            notification = Some(t._2)
          })
        })
      })
    })
    manager.createTable("xyz", "table1", "user_id(STRING)")
    notification.get should be("user_id(STRING)\n10485760\nnet.imagini.aim.utils.BlockStorageMEMLZ4")
  }
}