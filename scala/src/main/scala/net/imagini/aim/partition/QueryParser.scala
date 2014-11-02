package net.imagini.aim.partition

import scala.collection.mutable.Queue

import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.utils.Tokenizer

abstract class PDataFrame
case class PTable(name: String) extends PDataFrame

abstract class PJoin(left: PDataFrame, right: PDataFrame) extends PDataFrame
case class PSelect(table: PTable, filter: String, fields: PExp*) extends PDataFrame
case class PSubquery(subquery: PDataFrame, filter: String, fields: PExp*) extends PDataFrame
case class PUnionJoin(left: PDataFrame, right: PDataFrame) extends PJoin(left, right)
case class PIntesetionJoin(left: PDataFrame, right: PDataFrame) extends PJoin(left, right)
case class PEquiJoin(left: PDataFrame, right: PDataFrame) extends PJoin(left, right)

abstract class PExp //[T]
case class PVar(name: String) extends PExp //[AimType]
case class PWildcard extends PExp //[AimType]
abstract class PBoolExp extends PExp //[Boolean]

class QueryParser(val regions: Map[String, AimPartition]) extends App {

  def parse(query: String): AbstractScanner = toScanner(frame(query))

  def frame(query: String): PDataFrame = {
    val q = Queue[String](Tokenizer.tokenize(query, true).toArray(Array[String]()): _*)
    q.front.toUpperCase match {
      case "SELECT" ⇒ asDataFrame(q)
      //TODO case "COUNT"
    }   
  }
  private def toScanner(frame: PDataFrame): AbstractScanner = {
    frame match {
      case select: PSelect ⇒ {
        val region = regions(select.table.name)
        val schema = region.schema
        val filter = RowFilter.fromString(schema, select.filter)
        val fields: Array[String] = select.fields.flatMap(_ match {
          case w: PWildcard ⇒ schema.names
          case v: PVar      ⇒ Array(v.name)
        }).toArray
        new MergeScanner(schema, fields, filter, region.segments)
      }
    }
  }
  private def asDataFrame(q: Queue[String]): PDataFrame = {
    var frame: PDataFrame = null
    while (!q.isEmpty) q.dequeue.toUpperCase match {
      case "("      ⇒ frame = asDataFrame(q)
      case ")"      ⇒ if (frame != null) return frame else throw new IllegalArgumentException
      case "SELECT" ⇒ frame = asSelect(q)
      case "UNION"  ⇒ if (frame == null) throw new IllegalArgumentException else frame = PUnionJoin(frame, asDataFrame(q))
      //case "INTERSECTION"  ⇒ if (frame == null) throw new IllegalArgumentException else frame = PInterJoin(frame, asDataFrame(q))
      case "JOIN"   ⇒ if (frame == null) throw new IllegalArgumentException else frame = PEquiJoin(frame, asDataFrame(q))
    }
    frame
  }
  private def asSelect(q: Queue[String]): PDataFrame = {
    var fields = scala.collection.mutable.ListBuffer[PExp]()
    var table: PDataFrame = null
    var filter: String = "*"
    var state = "FIELDS"
    while (!q.isEmpty && state != "FINISHED") state match {
      case "FIELDS" ⇒ q.dequeue match {
        case s: String if (s.toUpperCase.equals("FROM")) ⇒ state = "TABLE"
        case "," if (fields == null)                     ⇒ throw new IllegalArgumentException
        case "," if (fields != null)                     ⇒ {}
        case "*"                                         ⇒ fields :+= PWildcard()
        case field: String                               ⇒ fields :+= PVar(field)
      }
      case "TABLE" ⇒ q.front match {
        case "(" ⇒ { table = asDataFrame(q); state = "FILTER_OR_FINISHED"; }
        case _   ⇒ { table = PTable(q.dequeue); state = "FILTER_OR_FINISHED"; }
      }
      case "FILTER_OR_FINISHED" ⇒ q.front.toUpperCase match {
        case "WHERE" ⇒ { filter = ""; state = q.dequeue.toUpperCase }
        case _       ⇒ state = "FINISHED"
      }
      case "WHERE" ⇒ q.front.toUpperCase match {
        case "UNION" | "JOIN" | "INTERSECTION" ⇒ state = "FINISHED"
        case _                                 ⇒ filter += q.dequeue + " "
      }
      case a ⇒ throw new IllegalStateException(a)
    }
    table match {
      case t: PTable     ⇒ new PSelect(t, filter, fields: _*)
      case s: PDataFrame ⇒ new PSubquery(s, filter, fields: _*)
    }
  }
}

