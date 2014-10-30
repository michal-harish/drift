package net.imagini.aim.tools

import java.nio.ByteBuffer

import scala.Array.canBuildFrom

import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType

trait AbstractScanner {

  val schema: AimSchema
  val keyColumn: Int
  final def keyType:AimType = schema.get(keyColumn)

  def rewind

  def next: Boolean

  def selectRow: Array[ByteBuffer]

  final def selectKey: ByteBuffer = selectRow(keyColumn)

  def mark

  def reset

  

  final protected[aim] def nextLine: String = nextLine("\t")

  final protected[aim] def nextLine(separator: String): String = { next; selectLine(separator) }

  final def selectLine(separator:String): String = (schema.fields, selectRow).zipped.map((t, b) ⇒ t.asString(b)).mkString(separator)

}