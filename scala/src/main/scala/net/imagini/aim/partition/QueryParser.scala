package net.imagini.aim.partition

import scala.collection.mutable.Queue
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.utils.Tokenizer
import net.imagini.aim.types.AimQueryException

abstract class PDataFrame(val fields: PExp*)
case class PTable(name: String) extends PDataFrame()

abstract class PJoin(left: PDataFrame, right: PDataFrame, fields: PExp*) extends PDataFrame(fields: _*)
case class PSelect(table: PTable, filter: String, override val fields: PExp*) extends PDataFrame(fields: _*)
case class PSubquery(subquery: PDataFrame, filter: String, override val fields: PExp*) extends PDataFrame(fields: _*)
case class PUnionJoin(left: PDataFrame, right: PDataFrame, override val fields: PExp*) extends PJoin(left, right, fields: _*)
case class PIntesetionJoin(left: PDataFrame, right: PDataFrame, override val fields: PExp*) extends PJoin(left, right, fields: _*)
case class PEquiJoin(left: PDataFrame, right: PDataFrame, override val fields: PExp*) extends PJoin(left, right, fields: _*)

abstract class PExp //[T]
case class PVar(name: String) extends PExp //[AimType]
case class PWildcard(dataframe: PDataFrame) extends PExp //[AimType]
abstract class PBoolExp extends PExp //[Boolean]

class QueryParser(val regions: Map[String, AimPartition]) extends App {

  def parse(query: String): AbstractScanner = compile(frame(query))

  def frame(query: String): PDataFrame = {
    val q = Queue[String](Tokenizer.tokenize(query, true).toArray(Array[String]()): _*)
    q.front.toUpperCase match {
      case "SELECT" ⇒ asDataFrame(q)
      //TODO case "COUNT"
      case _:String => throw new AimQueryException("Invalid query statment")
    }
  }
  def compile(frame: PDataFrame): AbstractScanner = {
    frame match {
      case join: PUnionJoin ⇒ new UnionJoinScanner(compile(join.left), compile(join.right))
      case join: PEquiJoin ⇒ {
        val leftScanner = compile(join.left)
        val rightScanner = compile(join.right)
        val fields: Array[String] = compile(join.fields)
        new EquiJoinScanner(fields, leftScanner, rightScanner)
      }
      case select: PSubquery ⇒ {
        val scanner = compile(select.subquery)
        val fields = compile(select.fields)
        val filter = RowFilter.fromString(scanner.schema, select.filter)
        new SubqueryScanner(fields, filter, scanner)
      }
      case select: PSelect ⇒ {
        val region = if (!regions.contains(select.table.name)) throw new AimQueryException("Unknown table") else regions(select.table.name)
        val schema = region.schema
        val filter = RowFilter.fromString(schema, select.filter)
        val fields = compile(select.fields)
        new MergeScanner(schema, fields, filter, region.segments)
      }
    }
  }

  def compile(exp: Seq[PExp]): Array[String] = {
    exp.flatMap(_ match {
      case w: PWildcard ⇒ w.dataframe match {
        case t: PTable     ⇒ regions(t.name).schema.names
        case f: PDataFrame ⇒ compile(f.fields)
      }
      case v: PVar ⇒ Array(v.name)
    }).toArray
  }

  private def asDataFrame(q: Queue[String]): PDataFrame = {
    var frame: PDataFrame = null
    while (!q.isEmpty) {
      if (q.front.equals("(")) {
        q.dequeue; frame = asDataFrame(q); q.dequeue
      } else if (q.front.equals(")")) {
        if (frame != null) return frame else throw new IllegalArgumentException
      } else q.dequeue.toUpperCase match {
        case "SELECT" ⇒ frame = asSelect(q)
        case "UNION" ⇒ {
          val right = asDataFrame(q)
          if (frame == null) throw new IllegalArgumentException else frame = PUnionJoin(frame, right, (frame.fields ++ right.fields): _*)
        }
        case "INTERSECTION" ⇒ if (frame == null) throw new IllegalArgumentException else frame = PIntesetionJoin(frame, asDataFrame(q), frame.fields: _*)
        case "JOIN" ⇒ if (frame == null) throw new IllegalArgumentException else {
          val right = asDataFrame(q)
          frame = PEquiJoin(frame, right, (frame.fields ++ right.fields): _*)
        }
      }
    }
    frame
  }
  private def asSelect(q: Queue[String]): PDataFrame = {
    var fields = scala.collection.mutable.ListBuffer[PExp]()
    var frame: PDataFrame = null
    var filter: String = "*"
    var state = "FIELDS"
    var wildcard = false
    var subquery = false
    while (!q.isEmpty && state != "FINISHED") state match {
      case "FIELDS" ⇒ q.dequeue match {
        case s: String if (s.toUpperCase.equals("FROM")) ⇒ state = "TABLE"
        case "," if (fields == null)                     ⇒ throw new IllegalArgumentException
        case "," if (fields != null)                     ⇒ {}
        case "*"                                         ⇒ wildcard = true
        case field: String                               ⇒ fields :+= PVar(field)
      }
      case "TABLE" ⇒ q.dequeue match {
        case "(" ⇒ { subquery = true; frame = asDataFrame(q); q.dequeue; state = "FILTER_OR_FINISHED"; }
        case name:String   ⇒ { frame = PTable(name); state = "FILTER_OR_FINISHED"; }
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
    if (wildcard) fields :+= PWildcard(frame)
    if (subquery) {
      new PSubquery(frame, filter.trim, fields: _*)
    } else {
      new PSelect(frame.asInstanceOf[PTable], filter.trim, fields: _*)
    }

  }
}

