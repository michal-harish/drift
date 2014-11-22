package net.imagini.aim.client

import java.io.FileInputStream
import java.io.InputStream
import java.net.URI
import grizzled.slf4j.Logger
import net.imagini.aim.cluster.AimNodeLoader
import net.imagini.aim.cluster.DriftManagerZk
import java.util.zip.GZIPInputStream
import net.imagini.aim.cluster.DriftManager

object DriftLoader extends App {
  var zookeeper: String = "localhost:2181"
  var clusterId: String = null
  var keyspace: String = null
  var table: String = null
  var separator: String = "\n"
  var gzip = false
  var file: Option[String] = None
  val argsIterator = args.iterator
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--zookeer"    ⇒ zookeeper = argsIterator.next
      case "--cluster-id" ⇒ clusterId = argsIterator.next
      case "--keyspace"   ⇒ keyspace = argsIterator.next
      case "--table"      ⇒ table = argsIterator.next
      case "--separator"  ⇒ separator = argsIterator.next
      case "--file"       ⇒ file = Some(argsIterator.next)
      case "--gzip"       ⇒ gzip = true
      case arg: String    ⇒ println("Unknown argument " + arg)
    }
  }
  val rawInput: InputStream = file match {
    case None           ⇒ System.in
    case Some(filename) ⇒ new FileInputStream(filename)
  }
  val manager = new DriftManagerZk(zookeeper, clusterId)
  val loader = new DriftLoader(manager, keyspace, table, separator, rawInput, gzip)
  val count = loader.streamInput
  println("Loader parsed " + count + " records")
}

class DriftLoader(
  val manager: DriftManager,
  val keyspace: String,
  val table: String,
  val separator: String,
  val inputStream: InputStream,
  val gzip: Boolean) {

  val in: InputStream = if (gzip) new GZIPInputStream(inputStream) else inputStream

  def streamInput: Long = {
    val loader = new AimNodeLoader(manager, keyspace, table)
    val count = loader.loadUnparsedStream(in, separator)
    manager.close
    count
  }
}
