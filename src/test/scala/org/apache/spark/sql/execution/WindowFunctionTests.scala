package org.apache.spark.sql.execution

import org.scalatest.FlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.expressions.Sum
/*
abstract class WindowFunctionTest extends FlatSpec {
  def name: String
  def function: WindowFunction
  def calculate(index: Int): Any
  def rows: Int = 20
  def range: Range = 0 until rows
  def row(values: Any*): Row = {
    new GenericRow(values.toArray)
  }
  def data: Seq[Row] = range.map(row(_))
  
  "A " + name + " window function" should "return the proper aggregated values" in {
    val result = function
    range.foreach { index =>
      assertResult(calculate(index), s"Index($index)")(result(index))
    }
  }
}

class EmptyAggregateWindowFunctionTest extends WindowFunctionTest {
  def name = "empty"
  def function = EmptyWindowFunction
  def calculate(index: Int): Any = null
}

class ShiftingWindowFunctionTest extends WindowFunctionTest {
  val offset = 1
  def name = "shifting"
  def function = new ShiftingWindowFunction(BoundReference(0, IntegerType, true), data, offset)
  def calculate(index: Int): Any = {
    val value = index + offset 
    if (value >= 0 && value < rows) value
    else null
  }
}

class GlobalAggregateWindowFunctionTest extends WindowFunctionTest {
  val aggregate = Sum(BoundReference(0, IntegerType, true))
  def name = "global"
  def function = new GlobalAggregateWindowFunction(aggregate, data)
  def calculate(index: Int): Any = range.sum
}

class AboveAggregateWindowFunctionTest extends WindowFunctionTest {
  val offset = 0
  val aggregate = Sum(BoundReference(0, IntegerType, true))
  def name = "above"
  def function = new AboveAggregateWindowFunction(aggregate, data, offset)
  def calculate(index: Int): Any = {
    val startIdx = math.max(index + offset, 0)
    if (startIdx < rows) (startIdx until rows).sum
    else null
  }
}

class BelowAggregateWindowFunctionTest extends WindowFunctionTest {
  val offset = 0
  val aggregate = Sum(BoundReference(0, IntegerType, true))
  def name = "below"
  def function = new BelowAggregateWindowFunction(aggregate, data, offset)
  def calculate(index: Int): Any = {
    val endIdx = math.min(index + offset + 1, rows)
    if (0 < endIdx) (0 until endIdx).sum
    else null
  }
}

class RangeAggregateWindowFunctionTest extends WindowFunctionTest {
  val low = -1
  val high = 1
  val aggregate = Sum(BoundReference(0, IntegerType, true))
  def name = "range"
  def function = new RangeAggregateWindowFunction(aggregate, data, low, high)
  def calculate(index: Int): Any = {
    val startIdx = math.max(index + low, 0)
    val endIdx = math.min(index + high + 1, rows)
    if (startIdx < endIdx) (startIdx until endIdx).sum
    else null
  }
}
*/