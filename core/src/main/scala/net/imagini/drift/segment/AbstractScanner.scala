package net.imagini.drift.segment

import scala.Array.canBuildFrom
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.types.DriftType
import net.imagini.drift.utils.View

trait CountScanner extends AbstractScanner {}

trait TransformScanner extends CountScanner {
  var intoKeyspace:String = null
  var intoTable:String = null
  def into(keyspace: String, table: String): TransformScanner = {
    intoKeyspace = keyspace
    intoTable = table
    this
  }
}

trait AbstractScanner {

  val schema: DriftSchema

  def keyType: DriftType

  def count: Long

  def next: Boolean

  def selectRow: Array[View] //TODO instead of Array this can be only View

  def selectKey: View

  def close = {
    //TODO implement close for all scanners
  }

  final protected[drift] def nextLine: String = nextLine("\t")

  final protected[drift] def nextLine(separator: String): String = { next; selectLine(separator) }

  final def selectLine(separator: String): String = (schema.fields, selectRow).zipped.map((t,b) ⇒ t.asString(b)).mkString(separator)

  //  val mapreduce = Executors.newFixedThreadPool(4)
  //  def reduce[T](filter: RowFilter, reducer:() => T): T= {
  //    val range = defaultRange
  //    val seg: Array[Int] = new Array(range._2 - range._1 + 1)
  //    val results: List[Future[Long]] = seg.map(s => mapreduce.submit(new Callable[Long] {
  //        override def call: Long = {
  //          segments.synchronized {
  //            segments(s).reduce(filter, reducer)
  //          }
  //        }
  //      })).toList
  //    results.foldLeft(0L)(_ + _.get)
  //  }

}