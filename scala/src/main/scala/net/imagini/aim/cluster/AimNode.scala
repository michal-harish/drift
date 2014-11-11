package net.imagini.aim.cluster

import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import grizzled.slf4j.Logger
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.partition.QueryParser
import net.imagini.aim.partition.StatScanner
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.BlockStorage
import net.imagini.aim.utils.BlockStorageMEMLZ4

class AimNode(val id: Int, val address: String, val manager: DriftManager) {

  val log = Logger[this.type]
  val nodes: ConcurrentMap[Int, URI] = new ConcurrentHashMap[Int, URI]()
  def peers: Map[Int, URI] = nodes.asScala.filter(n ⇒ n._1 != id).toMap

  private var suspended = new AtomicBoolean(true)
  @volatile private var shutdown = false
  def isShutdown: Boolean = {
    while (!shutdown && !isSuspended) {
      suspended.synchronized(suspended.wait)
    }
    true
  }
  def isSuspended: Boolean = suspended.get match {
    case false ⇒ false
    case true ⇒ {
      while (!shutdown && suspended.get) {
        try {
          suspended.synchronized {
            if (!shutdown) {
              suspended.wait
            } else for (i ← (0 to 100)) {
              if (i == 100) throw new TimeoutException() else log.debug(id + ": Node clean shutdown successful")
              suspended.wait(200L)
            }
          }
        } catch {
          case e: Throwable ⇒ {
            log.error(id + ": Node clean shutdown failed", e)
            throw e
          }
        }
      }
      shutdown
    }
  }
  private var keyspaceRefs = new ConcurrentHashMap[String, ConcurrentMap[String, AimPartition]]()
  def keyspaces = keyspaceRefs.keySet.asScala
  def regions: Map[String, AimPartition] = keyspaceRefs.asScala.flatMap(r ⇒ {
    val keyspace = r._1
    r._2.asScala.map(partition ⇒ {
      val table = partition._1
      val region = partition._2
      keyspace + "." + table -> region
    })
  }).toMap
  def stats(keyspaceName: String): AbstractScanner = {
    if (keyspaceName == null || keyspaceName.isEmpty) {
      new StatScanner(id, keyspaceRefs.asScala.flatMap(k ⇒ k._2.asScala.map { case (t, partition) ⇒ k._1 + "." + t -> partition }).toMap)
    } else {
      new StatScanner(id, keyspaceRefs.get(keyspaceName).asScala.map { case (t, partition) ⇒ t -> partition }.toMap)
    }
  }
  def query(query: String): AbstractScanner = new QueryParser(regions).parse(query)

  def transform(srcQuery: String, destKeyspace: String, destTable: String): Long = {
    val t = System.currentTimeMillis
    val scanner = query(srcQuery)
    val loader = new AimNodeLoader(destKeyspace, destTable, this)
    while (scanner.next) {
      loader.insert(scanner.selectRow)
    }
    val transformationCount = loader.finish
    log.info("TRANSFORM INTO " + destKeyspace + "." + destTable + " in " + (System.currentTimeMillis - t))
    transformationCount
  }

  var sessions = scala.collection.mutable.ListBuffer[AimNodeSession]()
  def session(s: AimNodeSession) = {
    sessions += s
    s.start
  }

  val acceptor = new AimNodeAcceptor(this, new URI("drift://" + address).getPort)
  log.info("Drift Node accepting connections on port " + acceptor.port)
  manager.registerNode(id, address)
  manager.watchData("/nodes/" + id.toString, (data: Option[String]) ⇒ data match {
    case Some(address) ⇒ {} //TODO check if address is this address
    case None          ⇒ doShutdown
  })

  manager.watch("/nodes", (children: Map[String, String]) ⇒ {
    val newNodes = children.map(p ⇒ p._1.toInt -> new URI("drift://" + p._2)).toMap
    nodes.asScala.keys.filter(!newNodes.contains(_)).map(nodes.remove(_))
    newNodes.map(n ⇒ nodes.put(n._1, n._2))
    rebalance
  })
  manager.watchData("/nodes", (num: Option[String]) ⇒ num match {
    case Some(n) ⇒ { manager.expectedNumNodes = Integer.valueOf(n); rebalance }
    case None    ⇒ { manager.expectedNumNodes = -1; rebalance }
  })
  manager.watch("/keyspaces", (ks: Map[String, String]) ⇒ {
    keyspaceRefs.asScala.keys.filter(!ks.contains(_)).map(keyspaceRefs.remove(_))
    ks.keys.filter(!keyspaceRefs.containsKey(_)).map(k ⇒ {
      keyspaceRefs.put(k, new ConcurrentHashMap[String, AimPartition]())
      manager.watch("/keyspaces/" + k, (tables: Map[String, String]) ⇒ {
        keyspaceRefs.get(k).asScala.keys.filter(!tables.contains(_)).map(keyspaceRefs.get(k).remove(_))
        tables.filter(t ⇒ !keyspaceRefs.get(k).containsKey(t._1)).map(t ⇒ {
          val tableDescriptor = t._2.split("\n")
          val schema = AimSchema.fromString(tableDescriptor(0))
          val segmentSize = java.lang.Integer.valueOf(tableDescriptor(1))
          val storageType = Class.forName(tableDescriptor(2)).asInstanceOf[Class[BlockStorage]]
          keyspaceRefs.get(k).put(t._1, new AimPartition(schema, segmentSize, storageType))
          log.debug(id + ": " + k + "." + t._1 + " " + keyspaceRefs.get(k).get(t._1).toString)
        })
      })
    })
  })

  def rebalance {
    if (manager.expectedNumNodes == nodes.size) {
      if (suspended.compareAndSet(true, false)) {
        log.info(id + ": resuming")
        suspended.synchronized(suspended.notifyAll)
      }
    } else if (suspended.compareAndSet(false, true)) {
      log.info(id + ": suspending, expecting " + manager.expectedNumNodes + " nodes, " + nodes.size + " available")
      suspended.synchronized(suspended.notifyAll)
    }
  }

  private def doShutdown {
    log.debug(id + ": Node at port " + acceptor.port + " shutting down.")
    acceptor.close
    sessions.foreach(session ⇒ try {
      session.close
    } catch {
      case e: Throwable ⇒ log.warn("Force session close: " + e.getMessage)
    })
    shutdown = true
    suspended.synchronized(suspended.notifyAll)
  }

  def down = {
    manager.unregisterNode(id)
    isSuspended
  }
}