package net.imagini.aim.partition

import java.io.EOFException
import net.imagini.aim.tools.Scanner
import net.imagini.aim.tools.PipeUtils
import scala.collection.mutable.LinkedList
import net.imagini.aim.types.AimSchema
import java.util.LinkedHashMap
import scala.collection.immutable.ListMap
import net.imagini.aim.types.AimType
import scala.collection.JavaConverters._
import net.imagini.aim.types.TypeUtils

//TODO AbstractScanner instead of MergeScanner 
class EquiJoinScanner(val left: MergeScanner, val leftSelect: Array[String], val right: MergeScanner, val rightSelect: Array[String])
  extends AbstractScanner {
  def this(left: MergeScanner, leftStatement: String, right: MergeScanner, rightStatement: String) = this(left, leftStatement.split(",").map(_.trim), right, rightStatement.split(",").map(_.trim))

  val keyColumn = left.schema.get(leftSelect(0))
  val keyType = left.schema.get(keyColumn)
  override val schema: AimSchema = new AimSchema(new LinkedHashMap[String, AimType](
    ((leftSelect.map(n ⇒ (n -> left.schema.field(n))) ++ rightSelect.map(n ⇒ (n -> right.schema.field(n)))).toMap).asJava))
  println(schema)

  //    while (true) {
  //      merges.map(_. currentGroup)
  var cmp: Int = -1
  do {
    cmp = TypeUtils.compare(left.scanCurrentKey, right.scanCurrentKey, keyType)
    if (cmp < 0) left.skipCurrentRow
    else if (cmp > 0) right.skipCurrentRow
  } while (cmp != 0)

  //      printJoinedRows()

  //    }
  def printJoinedRows() = {
    //        for (merge ← merges) {
    //          try {
    //            while (true) {
    //              val row = merge.nextGroupRowScan
    //              println(
    //                (merge.scanSchema.fields, row).zipped.map((t, s) ⇒ t.convert(PipeUtils.read(s, t.getDataType))).foldLeft("")(_ + _ + " "))
    //            }
    //          } catch {
    //            case e: EOFException ⇒ {}
    //          }
    //        }
  }

}