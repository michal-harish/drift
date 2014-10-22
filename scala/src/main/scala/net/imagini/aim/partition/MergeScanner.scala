package net.imagini.aim.partition

import net.imagini.aim.types.AimSchema
import net.imagini.aim.segment.AimSegment
import net.imagini.aim.tools.AimFilter
import net.imagini.aim.tools.Scanner
import java.io.InputStream
import net.imagini.aim.types.SortOrder
import net.imagini.aim.tools.PipeUtils
import java.io.EOFException

class MergeScanner(val schema: AimSchema, val filter: AimFilter, val partition: AimPartition) /*extends InputStream*/ {
  val sortOrder = SortOrder.ASC
  val keyColumn: Int = 0
  val keyField: String = schema.name(keyColumn)
  val keyType = schema.get(keyColumn)

  val scanners: Seq[Array[Scanner]] = partition.segments.map(segment ⇒ segment.wrapScanners(schema))
  /** TODO refactor filter initialisation - at the moment the filter requires calling updateFormuls
   *  because the set of scanners available for matching is dictated by what needs to be selected plus what needs to be filtered 
   */
  if (filter != null) {
    filter.updateFormula(schema.names)
  }

  def nextRecordAsString:String = (schema.fields,nextRecord).zipped.map((t,s) => t.convert(PipeUtils.read(s, t.getDataType))).foldLeft("")(_ + _ + " ")

  def nextRecord: Array[Scanner] = {
    var nextSegment = 0
    for (s ← (0 to scanners.size - 1)) {
      try {
        while (scanners(s).forall(columnScanner => !columnScanner.eof)) {
          if (filter != null && !filter.matches(scanners(s))) {
            skipRecord(s)
          } else {
            if (nextSegment != s) {
              val cmp = scanners(s)(keyColumn).compare(scanners(nextSegment)(keyColumn), keyType)
              if (sortOrder == SortOrder.ASC ^ cmp > 0) {
                nextSegment = s
              }
            }
            throw new IllegalStateException
          }
        }
      } catch { case e: IllegalStateException ⇒ {} }
    }
    nextSegment match {
      case -1     ⇒ throw new EOFException
      case s: Int ⇒ scanners(s)
    }
  }

  private def skipRecord(segment: Int) = for ((t, s) ← (schema.fields zip scanners(segment))) PipeUtils.skip(s, t.getDataType)

}