package net.imagini.aim.partition

import net.imagini.aim.types.AimSchema
import java.nio.ByteBuffer
import net.imagini.aim.tools.Scanner
import net.imagini.aim.types.AimType

trait AbstractScanner {

  val schema: AimSchema

  def selectCurrentKey: ByteBuffer

  def selectCurrentRow: Seq[ByteBuffer]

  def skipCurrentRow

  def mark

  def reset

  protected[aim] def currentRowAsString: String = {
    (schema.fields, selectCurrentRow).zipped.map((t, b) ⇒ t.asString(b)).foldLeft("")(_ + _ + " ")
  }

  protected[aim] def nextResultAsString: String = {
    val result = currentRowAsString
    skipCurrentRow
    result
  }

}