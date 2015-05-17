/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.EmptyRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.catalyst.expressions.PivotWindowExpression
import java.util.ArrayDeque


/**
 * Function for comparing boundary values.
 * The reason for not using a Function3 is performance. There are a number of things we try to 
 * achieve:
 * - Avoid boxing of the input arguments.
 * - Have InvokeVirtual instead of InvokeInterface calls to the compare method.
 * - Have a very shallow, package local & finalized class hierarchy in order to encourage the JIT
 *   to go native.
 *
 * TODO check the Runtime performance. Possibly revert measures if they don't work.
 */
private[execution] abstract class BoundOrdering {
  def compare(input:Seq[Row], inputIndex: Int, outputIndex: Int): Int
}

/**
 * Compare the input index to the bound of the output index.
 */
private[execution] final case class RowBoundOrdering(offset: Int) extends BoundOrdering {
  override def compare(input:Seq[Row], inputIndex: Int, outputIndex: Int): Int = inputIndex - (outputIndex + offset)
}

/**
 * Compare the value of the input index to the value bound of the output index.
 */
private[execution] final case class ValueBoundOrdering(
  ordering: Ordering[Row],
  current: Projection,
  bound: Projection) extends BoundOrdering {
  override def compare(input:Seq[Row], inputIndex: Int, outputIndex: Int): Int =
    ordering.compare(current(input(inputIndex)), bound(input(outputIndex)))
}

/**
 * A window function calculates the results of a number of window functions for a window frame. A 
 * window frame currently only offers access to the calculated results, how the window frame goes 
 * about calculating the result is an implementation specific detail.
 * 
 * TODO How to improve performance? A few thoughts:
 * - Window functions are expensive due to its distribution and ordering requirements. 
 *   Unfortunately it is up to the Spark engine to solve this. Improvements in the form of project
 *   Tungsten are on the way.
 * - The window frame processing bit can be improved though. But before we start doing that we 
 *   need to see how much of the time and resources are spent on partitioning and ordering, and 
 *   how much time and resources are spent processing the partitions. There are a couple ways to 
 *   improve on the current situation:
 *   - Reduce memory footprint by performing streaming calculations. This can only be done when 
 *     there are no Unbound/Pivot/Unbounded Following calculations present.
 *   - Use Thungsten style memory usage.
 *   - Use code generation in general, and use the approach to aggregation taken in the 
 *     AggregateEvaluation class in specific. This should work for all frame types except the Pivot 
 *     case.
 */
private[execution] abstract class WindowFunctionFrame {
  def count: Int
  def apply(row: Int, column: Int): Any
}

/**
 * The shifting window frame calculates frames with the following SQL form:
 * 
 * ...LEAD(1) OVER (PARTITION BY a ORDER BY b)
 * 
 * @param input rows, these are all the rows in the partition.
 * @param expressions who are shifting
 * @param offset the size (in rows) of the shift.
 */
private[execution] final class Shifting(
    rows: Seq[Row], 
    exprs: Array[Expression], 
    offset: Int) extends WindowFunctionFrame {
  val count = exprs.length
  def apply(row: Int, column: Int): Any = {
    val shiftedIndex = row + offset
    if (shiftedIndex >= 0 && shiftedIndex < rows.size) {
      exprs(column).eval(rows(shiftedIndex))
    }
    else null
  }
}

private[execution] abstract class AggregateWindowFunctionFrame(
    factories: Array[_ <: AggregateExpression]) extends WindowFunctionFrame {
  val count = factories.length 
  
  /** Create an array of aggregate functions. */
  final def create(): Array[AggregateFunction] = {
    val aggregates = new Array[AggregateFunction](count)
    var i = 0
    while (i < count) {
      aggregates(i) = factories(i).newInstance()
      i += 1
    }
    aggregates
  }
  
  /** Update an array of aggregate functions. */
  final def update(aggregates: Array[AggregateFunction], input: Row): Unit = {
    var i = 0
    while (i < count) {
      aggregates(i).update(input)
      i += 1
    }
  }

  /** Get the result from an array of aggregate functions. */
  final def eval(aggregates: Array[AggregateFunction]): Array[Any] = {
    val length = aggregates.length
    val result = new Array[Any](length)
    var i = 0
    while (i < count) {
      result(i) = aggregates(i).eval(EmptyRow)
      i += 1
    }
    result
  }
}

/**
 * The sliding window frame calculates frames with the following SQL form:
 * ... BETWEEN 1 PRECEDING AND 1 FOLLOWING
 * 
 * @param input rows, these are all the rows in the partition.
 * @param factories to create the aggregates with.
 * @param lbound comparator used to identify the lower bound of an output row.
 * @param ubound comparator used to identify the upper bound of an output row.
 */
private[execution] final class SlidingWindowFunctionFrame(
  input: Seq[Row],
  factories: Array[AggregateExpression],
  lbound: BoundOrdering,
  ubound: BoundOrdering) extends AggregateWindowFunctionFrame(factories) {
  val result = {
    val size = input.size
    val output = new Array[Array[Any]](size)
    val buffer = new ArrayDeque[Array[AggregateFunction]]
    var inputIndex = 0
    var bufferIndex = 0
    var outputIndex = 0
    while (inputIndex < size) {

      // Setup an aggregate for all (new) rows in scope. These rows can be recognized by the the 
      // fact that their currentValue >= lowValue. 
      while (bufferIndex < size && lbound.compare(input, inputIndex, bufferIndex) >= 0) {
        buffer.offer(create())
        bufferIndex += 1
      }

      // Output the current aggregate value for all finished rows. A finished row can be recognized 
      // by the fact that their currentValue > highValue.
      while (outputIndex < bufferIndex && ubound.compare(input, inputIndex, outputIndex) > 0) {
        output(outputIndex) = eval(buffer.pop())
        outputIndex += 1
      }

      // Update aggregates.
      val row = input(inputIndex)
      val iterator = buffer.iterator
      while (iterator.hasNext()) {
        update(iterator.next(), row)
      }
      
      // Move to the next row.
      inputIndex += 1
    }

    // Output the partially filled aggregates for all remaining rows.
    while (outputIndex < bufferIndex) {
      output(outputIndex) = eval(buffer.pop())
      outputIndex += 1
    }

    // Done
    output
  }
  
  def apply(row: Int, column: Int): Any = result(row)(column)
}

/**
 * The unbounded window frame calculates frames with the following SQL forms:
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 * 
 * Its results are  the same for each and every row in the partition. This class can be seen as a 
 * special case of a sliding window, but is optimized for the unbound case.
 * 
 * @param input rows, these are all the rows in the partition.
 * @param factories to create the aggregates with.
 */
private[execution] final class UnboundedWindowFunctionFrame(
    input: Seq[Row], 
    factories: Array[AggregateExpression]) extends AggregateWindowFunctionFrame(factories) {
  val result = {
    val aggregates = create()
    val iterator = input.iterator
    while (iterator.hasNext) {
      update(aggregates, iterator.next())
    }
    eval(aggregates)
  }
  def apply(row: Int, column: Int): Any = result(column)
}

/**
 * The pivot frame calculates a frame containing PivotWindowExpressions. Pivot Window Expressions
 * are in total control of their own processing, and no assumption can be made here. The main use 
 * case is processing Hive Pivotted UDAFs.
 * 
 * @param input rows, these are all the rows in the partition.
 * @param factories to create the aggregates with.
 */
private[execution] final class PivotWindowFunctionFrame(
    input: Seq[Row], 
    factories: Array[PivotWindowExpression]) extends AggregateWindowFunctionFrame(factories) {
  val result = {
    // Collect the data.
    val aggregates = create()
    val iterator = input.iterator
    while (iterator.hasNext) {
      update(aggregates, iterator.next())
    }
    // TODO we could remove the cast if we add parameters to the AggregateWindowFrameFunction
    eval(aggregates).map(_.asInstanceOf[Array[Any]])
  }
  def apply(row: Int, column: Int): Any = result(column)(row)
}

/**
 * The UnboundPreceding window frame calculates frames with the following SQL form:
 * ... BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 * 
 * There is only an upper bound. Very common use cases are for instance running sums or counts 
 * (row_number). Technically this is a special case of a sliding window. However a sliding window 
 * has to maintain aggregates for each row. This is not the case when there is no lower bound,
 * given the additive and communitative nature of most aggregates only one collection of aggregates 
 * needs to be maintained.
 * 
 * @param input rows, these are all the rows in the partition.
 * @param factories to create the aggregates with.
 * @param ubound comparator used to identify the upper bound of an output row.
 */
private[execution] final class UnboundedPrecedingWindowFunctionFrame(
  input: Seq[Row],
  factories: Array[AggregateExpression],
  ubound: BoundOrdering) extends AggregateWindowFunctionFrame(factories) {
  val result = {
    val size = input.size
    val output = new Array[Array[Any]](size)
    val aggregates = create()
    var inputIndex = 0
    var outputIndex = 0
    while (inputIndex < size) {
      // Output the current aggregate value for all finished rows. A finished row can be recognized 
      // by the fact that their currentValue > highValue.
      while (outputIndex < size && ubound.compare(input, inputIndex, outputIndex) > 0) {
        output(outputIndex) = eval(aggregates)
        outputIndex += 1
      }

      // Update aggregate.
      update(aggregates, input(inputIndex))
      
      // Move to the next row.
      inputIndex += 1
    }

    // Output the partially filled aggregate for all remaining rows.
    while (outputIndex < size) {
      output(outputIndex) = eval(aggregates)
      outputIndex += 1
    }

    // Done
    output
  }
  def apply(row: Int, column: Int): Any = result(row)(column)
}

/**
 * The UnboundPreceding window frame calculates frames with the following SQL form:
 * ... BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 * 
 * There is only a lower bound. Technically this is a special case of a sliding window. However a 
 * sliding window has to maintain aggregates for each row. This is not the case when there is no 
 * lower bound, given the additive and communitative nature of most aggregates only one collection 
 * of aggregates needs to be maintained. This class will process its input records in reverse 
 * order.
 * 
 * @param input rows, these are all the rows in the partition.
 * @param factories to create the aggregates with.
 * @param lbound comparator used to identify the lower bound of an output row.
 */
private[execution] final class UnboundedFollowingWindowFunctionFrame(
  input: Seq[Row],
  factories: Array[AggregateExpression],
  lbound: BoundOrdering) extends AggregateWindowFunctionFrame(factories) {
  val result = {
    val size = input.size
    val output = new Array[Array[Any]](size)
    val aggregates = create()
    var inputIndex = size - 1
    var outputIndex = size - 1
    while (inputIndex >= 0) {
      // Output the current aggregate value for all finished rows. A finished row can be recognized 
      // by the fact that their currentValue < lowValue 
      while (outputIndex >= 0 && lbound.compare(input, inputIndex, outputIndex) < 0) {
        output(outputIndex) = eval(aggregates)
        outputIndex -= 1
      }

      // Update aggregate and move to the next row
      update(aggregates, input(inputIndex))
      inputIndex -= 1
    }

    // Write partially filled aggregate for all remaining rows.
    while (outputIndex >= 0) {
      output(outputIndex) = eval(aggregates)
      outputIndex -= 1
    }

    // Done
    output
  }
  def apply(row: Int, column: Int): Any = result(row)(column)
}