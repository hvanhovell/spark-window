package org.apache.spark.sql.hive

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.expressions.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.PivotWindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowSpecExpression
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Sum
import org.apache.spark.sql.catalyst.expressions.CountDistinct
import org.apache.spark.sql.catalyst.expressions.ApproxCountDistinct
import org.apache.spark.sql.catalyst.expressions.SumDistinct
import org.apache.spark.sql.catalyst.expressions.Average
import org.apache.spark.sql.catalyst.expressions.Min
import org.apache.spark.sql.catalyst.expressions.Max
import org.apache.spark.sql.catalyst.expressions.Count
import org.apache.spark.sql.catalyst.expressions.Last
import org.apache.spark.sql.catalyst.expressions.First
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.ValueFollowing
import org.apache.spark.sql.catalyst.expressions.RowFrame
import org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition
import org.apache.spark.sql.catalyst.expressions.ValuePreceding
import org.apache.spark.sql.catalyst.expressions.UnboundedFollowing
import org.apache.spark.sql.catalyst.expressions.UnboundedPreceding
import org.apache.spark.sql.catalyst.expressions.CurrentRow
import org.apache.spark.sql.catalyst.trees.UnaryNode
import org.apache.spark.sql.catalyst.expressions.WindowFrame
import org.apache.spark.sql.catalyst.expressions.Rank
import org.apache.spark.sql.catalyst.expressions.DenseRank

/**
 * FIXME TESTING ONLY!!!
 * 
 * For testing the window implementation we need to remove the WindowFunctions from the named 
 * expressions.
 */
object WindowFunctionReplacer {
  // Remove all WindowFunctions...
  def apply(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    expressions.map { expr =>
      expr.transform {
        // Aggregates
        case ExtractHiveAggregate("GenericUDAFSum", child :: Nil, spec) =>
          WindowSpecExpression(Sum(child), spec)
        case ExtractHiveAggregate("GenericUDAFCount", child :: Nil, spec) =>
          WindowSpecExpression(Count(child), spec)
        case ExtractHiveAggregate("GenericUDAFFirstValue", child :: Nil, spec) =>
          WindowSpecExpression(First(child), spec)
        case ExtractHiveAggregate("GenericUDAFLastValue", child :: Nil, spec) =>
          WindowSpecExpression(Last(child), spec)
        case ExtractHiveAggregate("GenericUDAFAverage", child :: Nil, spec) =>
          WindowSpecExpression(Average(child), spec)
        case ExtractHiveAggregate("GenericUDAFMin", child :: Nil, spec) =>
          WindowSpecExpression(Min(child), spec)
        case ExtractHiveAggregate("GenericUDAFMax", child :: Nil, spec) =>
          WindowSpecExpression(Max(child), spec)
        // Lead/Lag
        case ExtractHiveAggregate("GenericUDAFLead", child :: Literal(offset: Int, IntegerType) :: Nil, spec) => {
          val frame = SpecifiedWindowFrame(RowFrame, ValueFollowing(offset), ValueFollowing(offset))
          WindowSpecExpression(child, spec.copy(frameSpecification = frame))
        }
        case ExtractHiveAggregate("GenericUDAFLag", child :: Literal(offset: Int, IntegerType) :: Nil, spec) => {
          val frame = SpecifiedWindowFrame(RowFrame, ValuePreceding(offset), ValuePreceding(offset))
          WindowSpecExpression(child, spec.copy(frameSpecification = frame))
        }
        // Pivot Aggregates
        case ExtractHiveAggregate("GenericUDAFRowNumber", _, spec) => {
          val frame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
          WindowSpecExpression(Count(Literal.create(1, IntegerType)), spec.copy(frameSpecification = frame))
        }
        case ExtractHiveAggregate("GenericUDAFRank", children, spec) => {
         val frame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
          WindowSpecExpression(Rank(children), spec.copy(frameSpecification = frame))
        }
        case ExtractHiveAggregate("GenericUDAFRank", children, spec) => {
         val frame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
          WindowSpecExpression(DenseRank(children), spec.copy(frameSpecification = frame))
        }
        // Wrap a Hive Pivot Aggregate. 
        case WindowExpression(HiveWindowFunction(func, true, false, children), spec) =>
          WindowSpecExpression(WrappingPivotWindowExpression(HiveGenericUdaf(func, children)), spec)
        case WindowExpression(HiveWindowFunction(func, true, true, children), spec) =>
          WindowSpecExpression(WrappingPivotWindowExpression(HiveUdaf(func, children)), spec)
        // Wrap a Hive Aggregate.
        case WindowExpression(HiveWindowFunction(func, false, false, children), spec) =>
          WindowSpecExpression(HiveGenericUdaf(func, children), spec)
        case WindowExpression(HiveWindowFunction(func, false, true, children), spec) =>
          WindowSpecExpression(HiveUdaf(func, children), spec)
      }.asInstanceOf[NamedExpression]
    }
  }
}

object ExtractHiveAggregate {
  def unapply(f: WindowExpression): Option[(String, Seq[Expression], WindowSpecDefinition)] = f match {
    case WindowExpression(HiveWindowFunction(HiveFunctionWrapper(name), _, _, children), spec) =>
      Some(name.replace("org.apache.hadoop.hive.ql.udf.generic.", ""), children, spec)
    case _ => None
  }
}

case class WrappingPivotWindowExpression(child: AggregateExpression)
  extends PivotWindowExpression with UnaryNode[Expression] {
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType
  override def newInstance() = child.newInstance()
}
