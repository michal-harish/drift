package net.imagini.aim.partition

import java.io.EOFException
import net.imagini.aim.tools.Scanner
import net.imagini.aim.tools.PipeUtils
import scala.collection.mutable.LinkedList

class JoinScanner(val merges: MergeScanner*) {
  val keyType = merges(0).scanSchema.get(0)
//  while (true) {
//    merges.map(_.currentGroup)
//    var cmp: Int = -1
//    do {
//      merges.foldLeft(merges(0))((x, y) ⇒ {
//        while (x.currentRow(0).compare(y.currentRow(0), keyType) < 0) x.skipGroup
//        y
//      })
//    } while (!merges.forall(_.currentRow(0).compare(merges(0).currentRow(0), keyType) == 0))
//
//    printJoinedRows()
//
//  }
  def printJoinedRows() = {
//    for (merge ← merges) {
//      try {
//        while (true) {
//          val row = merge.nextGroupRowScan
//          println(
//            (merge.scanSchema.fields, row).zipped.map((t, s) ⇒ t.convert(PipeUtils.read(s, t.getDataType))).foldLeft("")(_ + _ + " "))
//        }
//      } catch {
//        case e: EOFException ⇒ {}
//      }
//    }
  }

}