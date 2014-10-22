package net.imagini.aim.tools

import java.util.Queue

import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.AimType

object RowFilter {
  protected[tools] def proxy(root: RowFilter, schema: AimSchema, expression: String): RowFilter = new RowFilterSimple(schema, root, expression)

  def fromString(schema: AimSchema, declaration: String): RowFilter = fromTokenQueue(schema, Tokenizer.tokenize(declaration))
  def fromTokenQueue(schema: AimSchema, cmd: Queue[String]): RowFilter = {
    //
    new RowFilter(null)
  }
}

class RowFilterSimple(schema: AimSchema, root: RowFilter, val colName: String) extends RowFilter(schema, root, schema.field(colName), null) {
  var colIndex = -1
  override protected def getColumnSet(fields: Array[String]): Set[String] = super.getColumnSet(Array(colName))
  override def update(usedColumns: Array[String]) = {
    super.update(usedColumns)
    colIndex = usedColumns.indexOf(colName)
    if (colIndex == -1) {
      throw new IllegalArgumentException("Unknwon filter column " + colName);
    }
  }
  override def toString: String = colName + super.toString
  override protected[tools] def matches(soFar: Boolean, data: Array[Scanner]): Boolean = next.matches(data(colIndex), data)

}

class RowFilter(val schema: AimSchema, val root:RowFilter, val aimType: AimType, val next:RowFilter) {
  def this(schema: AimSchema) = this(schema, null,null,null)
  protected[tools] def this(root:RowFilter, aimType: AimType) = this(null, root, aimType, null)
  protected[tools] def this(root:RowFilter, aimType:AimType , next:RowFilter) = this(null, root, aimType, next)

  /**
   * This has to be synchronized as it can be called from multiple
   * segments at the same time and changes some internal data.
   * It is not called frequently so it should be ok.
   */
  final def updateFormula(usedColumns: Array[String]) {
    this.synchronized {
      //root.update(usedColumns);
    }
  }

  override def toString: String = if (next != null) " " + next.toString() else ""

  protected def getColumnSet(fields: Array[String]): Set[String] = (fields ++ (if (next != null) next.getColumns else Array[String]())).toSet

  def getColumns: Array[String] = root.getColumnSet(Array()).toArray

  protected def update(usedColumns:Array[String]):Unit = if (next != null) next.update(usedColumns)
  /**
   * This is thread-safe and called in parallel for
   * multiple segments.
   */
  def matches(record: Array[Scanner]): Boolean = root.matches(true, record)

  protected[tools] def matches(soFar: Boolean, record: Array[Scanner]): Boolean = if (next == null) soFar else next.matches(soFar, record)

  protected[tools] def matches(value: Scanner, record: Array[Scanner]): Boolean = {
    throw new IllegalAccessError(this.getClass().getSimpleName() + " cannot be matched against a value")
  }

  def where(expression: String): RowFilter = RowFilter.proxy(root, root.schema, expression)

}
