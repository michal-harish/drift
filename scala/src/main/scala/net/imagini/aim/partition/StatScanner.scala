package net.imagini.aim.partition

import java.nio.ByteBuffer
import scala.collection.immutable.SortedMap
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.types.Aim
import net.imagini.aim.types.AimSchema
import net.imagini.aim.utils.ByteUtils
import net.imagini.aim.cluster.AimNode
import java.io.EOFException
import net.imagini.aim.types.AimType

class StatScanner(val partition: Int, val regions: Map[String,AimPartition]) extends AbstractScanner {

  override val schema = AimSchema.fromString("table(STRING),region(INT),segments(LONG),count(LONG),distinct(LONG),compressed(LONG),uncompressed(LONG)")

  override val keyType:AimType = Aim.STRING

  private val data: SortedMap[String,Array[ByteBuffer]] = SortedMap(regions.map(r => {
        r._1 -> Array( 
            ByteUtils.wrap(schema.get(0).convert(r._1)),
            ByteUtils.wrap(schema.get(1).convert(partition.toString)),
            ByteUtils.wrap(schema.get(2).convert(r._2.numSegments.toString)),
            ByteUtils.wrap(schema.get(3).convert(r._2.count("*").toString)),
            ByteUtils.wrap(schema.get(4).convert(r._2.count("*").toString)),
            ByteUtils.wrap(schema.get(5).convert(r._2.getCompressedSize.toString)),
            ByteUtils.wrap(schema.get(6).convert(r._2.getUncompressedSize.toString))
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

  override def selectKey: ByteBuffer = if (currentRow < data.size) data(rowIndex(currentRow))(0) else throw new EOFException

  override def selectRow: Array[ByteBuffer] = if (currentRow < data.size) data(rowIndex(currentRow)) else throw new EOFException

  override def mark = rowMark = currentRow

  override def reset = currentRow = rowMark
}