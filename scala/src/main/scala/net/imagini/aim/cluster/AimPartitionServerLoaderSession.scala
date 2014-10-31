package net.imagini.aim.cluster

import net.imagini.aim.tools.Pipe
import java.io.IOException
import net.imagini.aim.utils.ByteUtils
import java.io.EOFException
import net.imagini.aim.segment.AimSegmentQuickSort
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.utils.BlockStorageLZ4
import net.imagini.aim.partition.AimPartition
import net.imagini.aim.utils.BlockStorageLZ4

class AimPartitionServerLoaderSession(val partition: AimPartition, val pipe: Pipe) extends Thread {

  val expectSchema = partition.schema.toString
  val actualSchema = pipe.read
  if (!actualSchema.equals(expectSchema)) {
    throw new IllegalArgumentException("Invalid loader schema, \nexpecting: " + expectSchema + "\nreceived:  " + actualSchema)
  }
  var count: Integer = 0
  var currentSegment: AimSegment = null
  /* Zero-copy support
     * TODO COLUMN_BUFFER_SIZE should be configurable for different schemas
     */
  val COLUMN_BUFFER_SIZE = 2048
  val record = ByteUtils.createBuffer(COLUMN_BUFFER_SIZE * partition.schema.size)
  override def run = {
    println("Loading into " + partition);
    try {
      val t = System.currentTimeMillis
      try {
        createNewSegmentIfNull
        while (!isInterrupted) {
          try {
            record.clear
            for (t ← partition.schema.fields) {
              pipe.read(t.getDataType, record)
            }
            record.flip
            currentSegment.appendRecord(record);
            count += 1
            if (currentSegment.getOriginalSize > partition.segmentSizeBytes) {
              addCurrentSegment
            }
          } catch {
            case e: EOFException ⇒ {
              addCurrentSegment
              throw e;
            }
          }
        }
      } catch {
        case e: EOFException ⇒ {
          System.out.println("load(EOF) records: " + count + " time(ms): " + (System.currentTimeMillis() - t))
        }
      }
    } catch {
      case e: Throwable ⇒ e.printStackTrace()
    } finally {
      System.out.println("Loading into " + partition + " finished");
      try { pipe.close(); } catch { case e: IOException ⇒ {} }
    }
  }

  private def addCurrentSegment = {
    try {
      currentSegment.close
      if (currentSegment.getOriginalSize > 0) {
        partition.add(currentSegment);
        currentSegment = null
      }
      createNewSegmentIfNull
    } catch {
      case e: Throwable ⇒ {
        throw new IOException(e);
      }
    }
  }

  private def createNewSegmentIfNull = {
    if (currentSegment == null) {
      currentSegment = new AimSegmentQuickSort(partition.schema, classOf[BlockStorageLZ4]);
      //TODO enalbe once schema contains keyField and sort order
      //            if (partition.keyField == null) {
      //                currentSegment = new AimSegmentUnsorted(partition.schema(), BlockStorageLZ4.class);
      //            } else {
      //                currentSegment = new AimSegmentQuickSort(partition.schema(), partition.keyField(), partition.sortOrder, BlockStorageLZ4.class);
      //            }
    }
  }
}