package net.imagini.aim.partition

import scala.collection.mutable.Queue
import net.imagini.aim.segment.MergeScanner
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.tools.RowFilter
import net.imagini.aim.utils.Tokenizer
import net.imagini.aim.types.AimQueryException
import net.imagini.aim.tools.CountScanner

abstract class PFrame
case class PCount(frame: PDataFrame) extends PFrame
abstract class PDataFrame(val fields: PExp*) extends PFrame
case class PTable(keyspace: String, name: String) extends PDataFrame() {
  def region: String = keyspace + "." + name
}
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

  def frame(query: String): PFrame = {
    val q = Queue[String](Tokenizer.tokenize(query, true).toArray(Array[String]()): _*)
    q.front.toUpperCase match {
      case "SELECT"  ⇒ asDataFrame(q)
      case "COUNT"   ⇒ asCount(q)
      case _: String ⇒ throw new AimQueryException("Invalid query statment")
    }
  }
  def compile(frame: PFrame, asCounter: Boolean = false): AbstractScanner = {
    frame match {
      case count: PCount ⇒ compile(count.frame, true)
      case select: PSelect ⇒ {
        val region = if (!regions.contains(select.table.region)) throw new AimQueryException("Unknown table " + select.table.region) else regions(select.table.region)
        val schema = region.schema
        val filter = RowFilter.fromString(schema, select.filter)
        val fields = compile(select.fields)
        if (asCounter) {
          new MergeScanner(schema, fields, filter, region.segments) with CountScanner
        } else {
          new MergeScanner(schema, fields, filter, region.segments)
        }
      }
      case select: PSubquery ⇒ {
        val scanner = compile(select.subquery)
        val fields = compile(select.fields)
        val filter = RowFilter.fromString(scanner.schema, select.filter)
        if (asCounter) {
          new SubqueryScanner(fields, filter, scanner) with CountScanner
        } else {
          new SubqueryScanner(fields, filter, scanner)
        }

      }
      case join: PUnionJoin ⇒ {
        if (asCounter) {
          new UnionJoinScanner(compile(join.left), compile(join.right)) with CountScanner
        } else {
          new UnionJoinScanner(compile(join.left), compile(join.right))
        }

      }
      case join: PEquiJoin ⇒ {
        val leftScanner = compile(join.left)
        val rightScanner = compile(join.right)
        if (asCounter) {
          new EquiJoinScanner(leftScanner, rightScanner) with CountScanner
        } else {
          new EquiJoinScanner(leftScanner, rightScanner)
        }
      }
    }
  }

  def compile(exp: Seq[PExp]): Array[String] = {
    exp.flatMap(_ match {
      case w: PWildcard ⇒ w.dataframe match {
        case t: PTable     ⇒ regions(t.region).schema.names
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
  private def asCount(q: Queue[String]): PCount = {
    var frame: PDataFrame = null
    q.dequeue
    while (!q.isEmpty) {
      q.dequeue match {
        case "(" ⇒ {
          frame = asDataFrame(q); q.dequeue;
          val filter = asFilter(q)
          return filter match {
            case "*" ⇒ PCount(frame)
            case _   ⇒ PCount(PSubquery(frame, filter))
          }
        }
        case keyspace: String ⇒ q.dequeue match {
          case "." ⇒ {
            frame = PTable(keyspace, q.dequeue)
            return PCount(PSelect(frame.asInstanceOf[PTable], asFilter(q)))
          }
          case q: String ⇒ throw new AimQueryException("Invalid from statement, should be <keyspace>.<table>")
        }
      }
    }
    throw new AimQueryException("Invalid COUNT statement")
  }
  private def asSelect(q: Queue[String]): PDataFrame = {
    var fields = scala.collection.mutable.ListBuffer[PExp]()
    var frame: PDataFrame = null
    var state = "FIELDS"
    var wildcard = false
    while (!q.isEmpty && state != "FINISHED") state match {
      case "FIELDS" ⇒ q.dequeue match {
        case s: String if (s.toUpperCase.equals("FROM")) ⇒ state = "FROM"
        case "," if (fields == null)                     ⇒ throw new AimQueryException("Invalid select expression")
        case "," if (fields != null)                     ⇒ {}
        case "*"                                         ⇒ wildcard = true
        case field: String                               ⇒ fields :+= PVar(field)
      }
      case "FROM" ⇒ q.dequeue match {
        case "(" ⇒ {
          frame = asDataFrame(q); q.dequeue;
          if (wildcard) fields :+= PWildcard(frame)
          return PSubquery(frame, asFilter(q), fields: _*)
        }
        case keyspace: String ⇒ q.dequeue match {
          case "." ⇒ {
            frame = PTable(keyspace, q.dequeue)
            if (wildcard) fields :+= PWildcard(frame)
            return PSelect(frame.asInstanceOf[PTable], asFilter(q), fields: _*)
          }
          case q: String ⇒ throw new AimQueryException("Invalid from statement, should be <keyspace>.<table>")
        }
      }
      case a ⇒ throw new IllegalStateException(a)
    }
    throw new AimQueryException("Invalid SELECT statement")
  }

  private def asFilter(q: Queue[String]): String = {
    var filter: String = "*"
    var state = "WHERE_OR_FINSIHED"
    while (!q.isEmpty) state match {
      case "WHERE_OR_FINSIHED" ⇒ q.front.toUpperCase match {
        case "WHERE" ⇒ { filter = ""; state = q.dequeue.toUpperCase }
        case _       ⇒ return filter.trim
      }
      case "WHERE" ⇒ q.front.toUpperCase match {
        case "UNION" | "JOIN" | "INTERSECTION" | ")" ⇒ return filter.trim
        case _                                       ⇒ filter += q.dequeue + " "
      }
    }
    filter.trim
  }

}

