package net.imagini.aim.tools

import java.nio.ByteBuffer

import scala.Array.canBuildFrom

import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType

trait AbstractScanner {

  val schema: AimSchema
  val keyType: AimType
  val keyColumn: Int

  final def selectKey: ByteBuffer = selectRow(keyColumn)

  def selectRow: Array[ByteBuffer]

  def skipRow

  def mark

  def reset

  final protected[aim] def nextResultAsString: String = {
    val result = (schema.fields, selectRow).zipped.map((t, b) ⇒ t.asString(b)).mkString(" ")
    skipRow
    result
  }

}