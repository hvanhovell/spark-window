package org.apache.spark.sql.execution

import java.util.ArrayDeque
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.AtomicType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.Sum
import org.apache.spark.sql.catalyst.expressions.InterpretedProjection
import org.apache.spark.sql.catalyst.expressions.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.EmptyRow
import org.apache.spark.sql.catalyst.expressions.AggregateExpression

object Runner {
  def main(args: Array[String]) {
    // Create the data
    val input = (1 to 10).flatMap { i =>
      val row = new GenericRow(Array[Any](i))
      row :: row :: Nil
    }

    // The expression.
    val expression = BoundReference(0, IntegerType, false)

    // Index the data.
    val projection = new InterpretedProjection(expression :: Nil)
    val values = input.map(projection)

    // Execute the sliding window. 
    val factory = Sum(expression)
    val evaluator = new RangeBoundsEvaluator(1, 1, values, expression.dataType)
    val out1 = sliding(input, factory, evaluator)

  }

  def sliding(input: Seq[Row], factory: AggregateExpression, evaluator: BoundsEvaluator): Array[Any] = {
    val size = input.size
    val output = new Array[Any](size)
    val buffer = new ArrayDeque[AggregateFunction]
    var inputIndex = 0
    var bufferIndex = 0
    var outputIndex = 0
    while (inputIndex < size) {

      // Setup an aggregate for all (new) rows in scope. These rows can be recognized by the the 
      // fact that their lowValue <= currentValue. 
      while (bufferIndex < size && evaluator.evalLowerBound(inputIndex, bufferIndex)) {
        buffer.offer(factory.newInstance())
        bufferIndex += 1
      }

      // Output the current aggregate value for all finished rows. A finished row can be recognized 
      // by the fact that their highValue < currentValue.
      while (outputIndex < bufferIndex && evaluator.evalUpperBound(inputIndex, outputIndex)) {
        output(outputIndex) = buffer.pop().eval(EmptyRow)
        outputIndex += 1
      }

      // Update aggregates and move to the next row.
      // TODO every aggregate in the buffer will execute some expression of unknown complexity. 
      // This can become quite costly when there are more than a few aggregates in the buffer. 
      // There are a few optimizations possible here:
      // - Execute the 'complex' expressions once by putting all these expressions into a single 
      //   projection and by replacing the expressions in the aggregate by a simple BoundReference.
      // - Use Code Generation in the projection. 
      val row = input(inputIndex)
      val iterator = buffer.iterator
      while (iterator.hasNext()) {
        iterator.next().update(row)
      }
      inputIndex += 1
    }

    // Write the partially filled aggregates for all remaining rows.
    while (outputIndex < bufferIndex) {
      output(outputIndex) = buffer.pop().eval(EmptyRow)
      outputIndex += 1
    }
    
    // Done
    output
  }

  def below(input: Seq[Row], factory: AggregateExpression, evaluator: BoundsEvaluator): Array[Any] = {
    val size = input.size
    val output = new Array[Any](size)
    val aggregate = factory.newInstance()
    var inputIndex = 0
    var outputIndex = 0
    while (inputIndex < size) {
      // Output the current aggregate value for all finished rows. A finished row can be recognized 
      // by the fact that their highValue < currentValue
      while (outputIndex < size && evaluator.evalLowerBound(inputIndex, outputIndex)) {
        output(outputIndex) = aggregate.eval(EmptyRow)
        outputIndex += 1
      }

      // Update aggregate and move to the next row
      aggregate.update(input(inputIndex))
      inputIndex += 1
    }
    
    // Write partially filled aggregate for all remaining rows.
    while (outputIndex < size) {
      output(outputIndex) = aggregate.eval(EmptyRow)
      outputIndex += 1
    }
    
    // Done
    output
  }
  
  def above(input: Seq[Row], factory: AggregateExpression, evaluator: BoundsEvaluator): Array[Any] = {
    val size = input.size
    val output = new Array[Any](size)
    val aggregate = factory.newInstance()
    var inputIndex = size - 1
    var outputIndex = size - 1
    while (inputIndex >= 0) {
      // Output the current aggregate value for all finished rows. A finished row can be recognized 
      // by the fact that their lowValue > currentValue
      while (outputIndex >= 0 && evaluator.evalLowerBound(inputIndex, outputIndex)) {
        output(outputIndex) = aggregate.eval(EmptyRow)
        outputIndex -= 1
      }

      // Update aggregate and move to the next row
      aggregate.update(input(inputIndex))
      inputIndex -= 1
    }
    
    // Write partially filled aggregate for all remaining rows.
    while (outputIndex >= 0) {
      output(outputIndex) = aggregate.eval(EmptyRow)
      outputIndex -= 1
    }
    
    // Done
    output
  }
}

private[execution] abstract class BoundsEvaluator {
  def evalLowerBound(input: Int, output: Int): Boolean
  def evalUpperBound(input: Int, output: Int): Boolean
}

private[execution] final class RowBoundsEvaluator(preceding: Int, following: Int) extends BoundsEvaluator {
  def evalLowerBound(input: Int, output: Int): Boolean = output - preceding <= input
  def evalUpperBound(input: Int, output: Int): Boolean = output + following < input
}

// TODO alot can be gained by CG'ing a ton of the 
private[execution] final class RangeBoundsEvaluator(preceding: Int, following: Int, values: Seq[Row], dataType: DataType) extends BoundsEvaluator {
  val comparator = dataType match {
    case a: AtomicType => a.ordering.asInstanceOf[Ordering[Any]]
  }
  val calculator = BoundReference(0, dataType, true)
  val lowBoundCalculator = Subtract(calculator, Cast(Literal.create(preceding, IntegerType), dataType))
  val highBoundCalculator = Add(calculator, Cast(Literal.create(following, IntegerType), dataType))
  def evalLowerBound(input: Int, output: Int): Boolean = {
    comparator.lteq(lowBoundCalculator.eval(values(output)), calculator.eval(values(input)))
  }
  def evalUpperBound(input: Int, output: Int): Boolean = {
    comparator.lt(highBoundCalculator.eval(values(output)), calculator.eval(values(input)))
  }
}