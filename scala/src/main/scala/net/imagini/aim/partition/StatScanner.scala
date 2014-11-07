package net.imagini.aim.partition

import scala.collection.immutable.SortedMap
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.Aim
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.cluster.AimNode
import java.io.EOFException
import net.imagini.aim.types.AimType
import net.imagini.aim.utils.View

class StatScanner(val partition: Int, val regions: Map[String,AimPartition]) extends AbstractScanner {

  override val schema = AimSchema.fromString("table(STRING),region(INT),segments(LONG),count(LONG),distinct(LONG),compressed(LONG),uncompressed(LONG)")

  override val keyType:AimType = Aim.STRING

  override val keyLen = keyType.getDataType.getLen

  private val data: SortedMap[String,Array[View]] = SortedMap(regions.map(r => {
        r._1 -> Array( 
            new View(schema.get(0).convert(r._1)),
            new View(schema.get(1).convert(partition.toString)),
            new View(schema.get(2).convert(r._2.numSegments.toString)),
            new View(schema.get(3).convert(r._2.getCount.toString)),
            //TODO count(!) count distinct should be different from count(*)
            new View(schema.get(4).convert(r._2.getCount.toString)),
            new View(schema.get(5).convert(r._2.getCompressedSize.toString)),
            new View(schema.get(6).convert(r._2.getUncompressedSize.toString))
         )
  }).toSeq:_* )

  private val rowIndex: Map[Int,String] = (data.keys, 0 to data.size-1).zipped.map((t,i) => i -> t).toMap

  private var currentRow = -1
  private var rowMark = -1
  override def rewind = currentRow = -1

  override def next: Boolean = {
    currentRow += 1
    currentRow < data.size
  }

  override def selectKey: View = if (currentRow < data.size) data(rowIndex(currentRow))(0) else throw new EOFException

  override def selectRow: Array[View] = if (currentRow < data.size) data(rowIndex(currentRow)) else throw new EOFException

  override def mark = rowMark = currentRow

  override def reset = currentRow = rowMark

  override def count: Long = data.size
}