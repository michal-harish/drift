package net.imagini.aim.partition

import net.imagini.aim.types.AimSchema
import java.nio.ByteBuffer
import net.imagini.aim.tools.Scanner
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

  final protected[aim] def selectRowAsString: String = {
    (schema.fields, selectRow).zipped.map((t, b) ⇒ t.asString(b)).foldLeft("")(_ + _ + " ")
  }

  final protected[aim] def nextResultAsString: String = {
    val result = selectRowAsString
    skipRow
    result
  }

}