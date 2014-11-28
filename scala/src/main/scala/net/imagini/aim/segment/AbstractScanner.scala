package net.imagini.aim.segment

import scala.Array.canBuildFrom
import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType
import net.imagini.aim.utils.View

trait CountScanner extends AbstractScanner {}

trait AbstractScanner {

  val schema: AimSchema

  def keyType: AimType

  def count: Long

  def next: Boolean

  def selectRow: Array[View] //TODO instead of Array this can be only View

  def selectKey: View

  def close = {
    //TODO implement close for all scanners
  }

  final protected[aim] def nextLine: String = nextLine("\t")

  final protected[aim] def nextLine(separator: String): String = { next; selectLine(separator) }

  final def selectLine(separator: String): String = (schema.fields, selectRow).zipped.map((t, b) ⇒ t.asString(b)).mkString(separator)

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