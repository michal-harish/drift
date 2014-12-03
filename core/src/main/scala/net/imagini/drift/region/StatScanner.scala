package net.imagini.drift.region

import scala.collection.immutable.SortedMap
import net.imagini.drift.segment.AbstractScanner
import net.imagini.drift.types.Drift
import net.imagini.drift.types.DriftSchema
import net.imagini.drift.utils.ByteUtils
import net.imagini.drift.cluster.DriftNode
import java.io.EOFException
import net.imagini.drift.types.DriftType
import net.imagini.drift.utils.View

class StatScanner(val region: Int, val regions: Map[String, DriftRegion]) extends AbstractScanner {

  override val schema = DriftSchema.fromString("table(STRING),region(INT),segments(LONG),compressed(LONG)")

  override val keyType: DriftType = Drift.STRING

  private val data: SortedMap[String, Array[View]] = SortedMap(regions.map(r ⇒ {
    r._1 -> Array(
      new View(schema.get(0).convert(r._1)),
      new View(schema.get(1).convert(region.toString)),
      new View(schema.get(2).convert(r._2.getNumSegments.toString)),
      new View(schema.get(3).convert(r._2.getCompressedSize.toString)))
  }).toSeq: _*)

  private val rowIndex: Map[Int, String] = (data.keys, 0 to data.size - 1).zipped.map((t, i) ⇒ i -> t).toMap

  private var currentRow = -1

  override def next: Boolean = {
    currentRow += 1
    currentRow < data.size
  }

  override def selectKey: View = if (currentRow < data.size) data(rowIndex(currentRow))(0) else throw new EOFException

  override def selectRow: Array[View] = if (currentRow < data.size) data(rowIndex(currentRow)) else throw new EOFException

  override def count: Long = data.size
}