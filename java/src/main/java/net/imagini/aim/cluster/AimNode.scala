package net.imagini.aim.cluster

import net.imagini.aim.Aim.SortOrder
import net.imagini.aim.AimSchema
import net.imagini.aim.AimUtils
import scala.collection.JavaConverters._

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
      case "--schema"       ⇒ schema = Some(AimUtils.parseSchema(argsIterator.next))
      case "--filename"     ⇒ filename = argsIterator.next
      case arg:String                ⇒ println("Unknown argument " + arg)
    }
  }

  val key = schema.get.name(0)
  val table = new AimTable(schema.get, segmentSize, key, SortOrder.ASC)
  new AimServer(table, port).start

  new StandardLoader(schema.get, "localhost", port, separator, filename, gzip).start

}