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
import org.apache.spark.sql.catalyst.expressions.GenericRowLike

/**
 * Result of a function applied to a window.
 */
private[execution] abstract class WindowFunction {
  /**
   * Get the result for the row at the given index. This method is only required to behave well 
   * for the row indices that were in the original set. 
   */
  def apply(index: Int): Any
}

/**
 * Always NULL Result for a window. Mainly used for testing.
 */
private[execution] object EmptyWindowFunction extends WindowFunction {
  def apply(index: Int): Any = null
}

/**
 * 
 */
private[execution] final class ShiftingWindowFunction(val expr: Expression, val rows: Seq[Row], val offset: Int) extends WindowFunction {
  def apply(index: Int): Any = {
    val shiftedIndex = index + offset
    if (shiftedIndex >= 0 && shiftedIndex < rows.size) expr.eval(rows(shiftedIndex))
    else null
  }
}

private[execution] final class GlobalAggregateWindowFunction(factory: AggregateExpression, rows: Seq[Row]) extends WindowFunction {
  val result = {
    val aggregator = factory.newInstance()
    val iterator = rows.iterator
    while (iterator.hasNext) {
      aggregator.update(iterator.next())
    }
    aggregator.eval(EmptyRow)
  }

  def apply(index: Int): Any = result
}

private[execution] final class BelowAggregateWindowFunction(factory: AggregateExpression, rows: Seq[Row], offset: Int) extends WindowFunction {
  val result = {
    val aggregate = factory.newInstance()
    val output = new Array[Any](rows.size)
    var rowIndex = 0
    var outputIndex = -offset
    while (rowIndex < output.length) {
      aggregate.update(rows(rowIndex))
      if (outputIndex >= 0 && outputIndex < output.length) {
        output(outputIndex) = aggregate.eval(EmptyRow)
      }
      outputIndex += 1
      rowIndex += 1
    }
    output
  }

  def apply(index: Int): Any = result(index)
}

private[execution] final class AboveAggregateWindowFunction(factory: AggregateExpression, rows: Seq[Row], offset: Int) extends WindowFunction {
  val result = {
    val aggregate = factory.newInstance()
    val output = new Array[Any](rows.size)
    var rowIndex = rows.size - 1
    var outputIndex = rowIndex - offset
    while (rowIndex >= 0) {
      aggregate.update(rows(rowIndex))
      if (outputIndex >= 0 && outputIndex < output.length) {
        output(outputIndex) = aggregate.eval(EmptyRow)
      }
      outputIndex -= 1
      rowIndex -= 1
    }
    output
  }

  def apply(index: Int): Any = result(index)
}

private[execution] final class RangeAggregateWindowFunction(factory: AggregateExpression, rows: Seq[Row], low: Int, high: Int) extends WindowFunction {
  val result = {
    // Define the input length and index. Note that we prune the index.
    val rowLength = rows.size
    val startEmitRowIndex = math.min(high + 1, rowLength) 
    var rowIndex = math.max(0, low)

    // Define the result
    val output = new Array[Any](rowLength)

    // Make sure the window contains rows.
    if (rowIndex < rowLength) {
      // Define the output length and index. Note that we constrain the output by pruning the 
      // index and length.
      var outputIndex = math.max(0, -high)
      val outputLength = rowLength - rowIndex
      
      // Define the window.
      val aggregateWindowSize = high - low + 1
      val aggregates = Array.fill[AggregateFunction](aggregateWindowSize)(factory.newInstance())

      // Construct the aggregates and emit the required rows.
      while (outputIndex < outputLength) {
        // Accumulate 
        if (rowIndex < rowLength) {
          // TODO this might be expensive. Each separate aggregate will extract the same data from 
          // the input row. Possible improvements:
          // - Materialize the to-be aggregated row. The inputs of the aggregate need to be patched 
          //   in this case.
          // - Use Code Generation on the input expressions.
          // - Do both...
          val input = rows(rowIndex)
          var i = 0
          while (i < aggregateWindowSize) {
            aggregates(i).update(input)
            i += 1
          }
          rowIndex += 1
        }

        // Emit
        if (rowIndex >= startEmitRowIndex) {
          val aggregateWindowIndex = outputIndex % aggregateWindowSize
          output(outputIndex) = aggregates(aggregateWindowIndex).eval(EmptyRow)
          aggregates(aggregateWindowIndex) = factory.newInstance()
          outputIndex += 1
        }
      }
    }
    // Done.
    output
  }

  def apply(index: Int): Any = result(index)
}

private[execution] final class WindowFunctionRowView(val length: Int, val functions: Array[WindowFunction]) extends GenericRowLike {
  var index = -1
  def apply(ordinal: Int): Any = functions(ordinal)(index)
  def hasNext = index + 1 < length
  def next() = {
    index += 1
    this
  }
} 