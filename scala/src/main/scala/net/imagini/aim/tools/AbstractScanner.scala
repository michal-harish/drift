package net.imagini.aim.tools

import java.nio.ByteBuffer

import scala.Array.canBuildFrom

import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType

trait AbstractScanner {

  val schema: AimSchema
  val keyColumn: Int
  final def keyType: AimType = schema.get(keyColumn)

  def rewind

  def next: Boolean

  def selectRow: Array[ByteBuffer]

  //TODO select key should work independently of selectRow to allow for decoupling of selection on joining
  final def selectKey: ByteBuffer = selectRow(keyColumn)

  def mark

  def reset

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