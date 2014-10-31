package net.imagini.aim.cluster

import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter
import grizzled.slf4j.Logger
import net.imagini.aim.types.AimSchema
import java.util.concurrent.TimeoutException

object AimNode extends App {
  val log = Logger[AimNode.this.type]
  var port: Int = 4000
  var zkConnect: String = "localhost:2181"
  val argsIterator = args.iterator
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--zookeeper" ⇒ zkConnect = argsIterator.next
      case "--port"      ⇒ port = argsIterator.next.toInt
      case arg: String   ⇒ println("Unknown argument " + arg)
    }
  }

  //SPAWNING CLUSTER
  val manager = new DriftManager(zkConnect, 3)
  val node1 = new AimNode(1, "localhost:4001", manager)
  val node2 = new AimNode(2, "localhost:4002", manager)
  val node3 = new AimNode(3, "localhost:4003", manager)

  //CREATING TABLES
  manager.createTable("addthis", "pageviews", "at_id(STRING), url(STRING), timestamp(TIME:LONG)", true)
  manager.createTable("addthis", "syncs", "at_id(STRING), vdna_user_uid(UUID:BYTEARRAY[16]), timestamp(TIME:LONG)", true)

  //SHUTTING FIRST NODE
  node1.shutdown
  Thread.sleep(3000)

  //SHUTTING REST OF THE CLUSTER
  node2.shutdown
  node3.shutdown
  manager.close
}

class AimNode(val id: Int, val address: String, val manager: DriftManager) {
  val log = Logger[AimNode.this.type]
  val acceptor = new AimNodeAcceptor(this, new URI("drift://" + address).getPort)
  log.info("Drift Node accepting connections on port " + acceptor.port)
  manager.registerNode(id, address)
  val peers: ConcurrentMap[Int, URI] = new ConcurrentHashMap[Int, URI]()

  private var numNodes = -1
  private var suspended = new AtomicBoolean(true)
  private var keyspaces = new ConcurrentHashMap[String, ConcurrentMap[String, AimSchema]]()

  manager.watch("/drift/nodes", (children: Map[String, String]) ⇒ {
    val nodes = children.map(p ⇒ p._1.toInt -> new URI(p._2)).toMap
    peers.asScala.keys.filter(!nodes.contains(_)).map(peers.remove(_))
    nodes.map(n ⇒ peers.put(n._1, n._2))
    rebalance
  })
  manager.watchData("/drift/nodes", (num: Option[Int]) ⇒ num match {
    case Some(n) ⇒ { numNodes = n; rebalance }
    case None    ⇒ { numNodes = -1; rebalance }
  })
  manager.watch("/drift/keyspaces", (ks: Map[String, String]) ⇒ {
    keyspaces.asScala.keys.filter(!ks.contains(_)).map(keyspaces.remove(_))
    ks.keys.filter(!keyspaces.containsKey(_)).map(k ⇒ {
      keyspaces.put(k, new ConcurrentHashMap[String, AimSchema]())
      manager.watch("/drift/keyspaces/" + k, (tables: Map[String, String]) ⇒ {
        keyspaces.get(k).asScala.keys.filter(!tables.contains(_)).map(keyspaces.get(k).remove(_))
        tables.filter(t ⇒ !keyspaces.get(k).containsKey(t._1)).map(t ⇒ {
          keyspaces.get(k).put(t._1, AimSchema.fromString(t._2))
          log.debug(id + ": " + k + "." + t._1 + " " + keyspaces.get(k).get(t._1).toString)
        })
      })
    })
  })

  def rebalance {
    if (numNodes == peers.size) {
      if (suspended.compareAndSet(true, false)) {
        log.info(id + ": resuming")
      }
    } else if (suspended.compareAndSet(false, true)) {
      log.info(id + ": suspending")
    }
  }

  def shutdown = {
    log.debug(id +": Node at port " + acceptor.port + " shutting down.")
    val OK = new AtomicBoolean(false)
    manager.watchData("/drift/nodes/" + id.toString, (data: Option[String]) ⇒ data match {
      case Some(address) ⇒ manager.unregisterNode(id) //TODO check if address is this address
      case None ⇒ OK.synchronized {
        OK.set(true)
        OK.notify
      }
    })

    try {
      OK.synchronized {
        for (i ← (1 to 100)) if (!OK.get) OK.wait(200L)
      }
      if (!OK.get) throw new TimeoutException() else log.debug(id + ": Node clean shutdown successful")
    } catch {
      case e: Throwable ⇒ {
        log.error(id + ": Node clean shutdown failed", e)
        throw e
      }
    } finally {
      acceptor.close
      //TODO close all sessions
    }
  }
}