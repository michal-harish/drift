package net.imagini.aim.cluster

import net.imagini.aim.types.SortOrder
import net.imagini.aim.AimPartition;
import net.imagini.aim.AimPartition ;
import net.imagini.aim.types.AimSchema

object AimNode extends App {

  var port = 4000
  var filename: String = null
  var separator = "\n"
  var gzip = false
  var schema: Option[AimSchema] = None
  val argsIterator = args.iterator
  var limit = 0L
  var segmentSize = 10485760
  while (argsIterator.hasNext) {
    argsIterator.next match {
      case "--segment-size" ⇒ segmentSize = argsIterator.next.toInt
      case "--port"         ⇒ port = argsIterator.next.toInt
      case "--separator"    ⇒ separator = argsIterator.next
      case "--gzip"         ⇒ gzip = true;
      case "--schema"       ⇒ schema = Some(AimSchema.fromString(argsIterator.next))
      case "--filename"     ⇒ filename = argsIterator.next
      case arg: String      ⇒ println("Unknown argument " + arg)
    }
  }

  val key = schema.get.name(0)
  val sortOrder = SortOrder.ASC
  val partition = new AimPartition(schema.get, segmentSize, key, sortOrder)
  val server = new AimServer(partition, port).start

  new StandardLoader(schema.get, "localhost", port, separator, filename, gzip).processInput


}