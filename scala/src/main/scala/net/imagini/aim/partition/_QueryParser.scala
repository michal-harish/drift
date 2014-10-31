package net.imagini.aim.partition

import scala.collection.mutable.Queue
import net.imagini.aim.tools.AbstractScanner
import net.imagini.aim.utils.Tokenizer
import scala.collection.mutable.Stack
import scala.collection.JavaConverters._
import java.util.Arrays

abstract class PDataFrame
case class PTable(name: String) extends PDataFrame
abstract class PJoin(left: PDataFrame, right: PDataFrame) extends PDataFrame
case class PUnionJoin(left: PDataFrame, right: PDataFrame) extends PJoin(left, right)
case class PEquiJoin(left: PDataFrame, right: PDataFrame) extends PJoin(left, right)
case class PSelect(table: PDataFrame, filter: String, fields: PVar*) extends PDataFrame
case class PVar(name: String)

//abstract class PExp[T]
//abstract class PBooleanExpression extends PExp[Boolean]
//abstract class PDataFrameExpression extends PExp[PDataFrame]

object QueryParser extends App {

  parse("select 'user_uid' from flags where value='true' and flag='quizzed' or flag='cc' JOIN (SELECT user_uid,url,timestamp FROM pageviews WHERE time > 1000 UNION SELECT * FROM converesions)")

  def parse(query: String): AbstractScanner = {
    val q = Queue[String](Tokenizer.tokenize(query).toArray(Array[String]()): _*)
    //q.map(println)
    q.front.toUpperCase match {
      case "SELECT" => {
        println(asDataFrame(q))
      }
      //case "COUNT"
    }
    null
  }
  def asDataFrame(q: Queue[String]): PDataFrame = {
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
  def asSelect(q: Queue[String]): PSelect = {
    var fields: Seq[PVar] = scala.collection.mutable.ListBuffer()
    var table: PDataFrame = null
    var filter: String = "*"
    var state = "FIELDS"
    while (!q.isEmpty && state != "FINISHED") state match {
      case "FIELDS" ⇒ q.dequeue match {
        case s: String if (s.toUpperCase.equals("FROM")) ⇒ state = "TABLE"
        case "," if (fields == null)                     ⇒ throw new IllegalArgumentException
        case "," if (fields != null)                     ⇒ {}
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
      case a => throw new IllegalStateException(a)
    }
    return new PSelect(table, filter, fields: _*)
  }
}

