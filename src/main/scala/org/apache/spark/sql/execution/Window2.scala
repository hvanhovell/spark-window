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

import java.util.ArrayDeque
import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.mutable.Buffer

/**
 * :: DeveloperApi ::
 * This class calculates and outputs (windowed) aggregates over the rows in a single sorted group.
 * The aggregates are calculated for each row in the group. An aggregate can take a few forms:
 * - Global: The aggregate is calculated for the entire group. Every row has the same value.
 * - Rows: The aggregate is calculated based on a subset of the window, and is unique for each
 *   row and depends on the position of the given row within the window. The group must be sorted
 *   for this to produce sensible output. Examples are moving averages, running sums and row
 *   numbers.
 * - Range: The aggregate is calculated based on a subset of the window, and is unique for each
 *   value of the order by clause and depends on its ordering. The group must be sorted for this to
 *   produce sensible output.
 * - Shifted: The aggregate is a displaced value relative to the position of the given row.
 *   Examples are Lead and Lag.
 *
 * This is quite an expensive operator because every row for a single group must be in the same
 * partition and partitions must be sorted according to the grouping and sort order. This can be
 * infeasible in some extreme cases. The operator does not repartition or sort itself, but requires
 * the planner to this.
 *
 * The current implementation is semi-blocking. The aggregates and final project are calculated one
 * group at a time, this is possible due to the aforementioned partitioning and ordering
 * constraints.
 */
@DeveloperApi
case class Window2(
  projectList: Seq[NamedExpression],
  spec: WindowSpecDefinition,
  child: SparkPlan)
  extends UnaryNode with Logging {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (spec.partitionSpec.isEmpty) {
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(spec.partitionSpec) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(spec.partitionSpec.map(SortOrder(_, Ascending)) ++ spec.orderSpec)

  // TODO check if this will match the requiredChildOrdering
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  @transient
  private[this] lazy val (factories, projection, factoryCount, windowFunctionCount) = {
    // Helper method for creating bound ordering objects.
    def createBoundOrdering(frameType: FrameType, offset: Int) = frameType match {
      case RangeFrame => {
        val currentExpr = spec.orderSpec.head
        val current = newMutableProjection(currentExpr :: Nil, child.output)()
        val boundExpr = Add(currentExpr, Cast(Literal.create(offset, IntegerType), currentExpr.dataType))
        val bound = newMutableProjection(boundExpr :: Nil, child.output)()
        val orderingExpr = AttributeReference("ordering", currentExpr.dataType, currentExpr.nullable)()
        val ordering = newOrdering(SortOrder(orderingExpr, Ascending) :: Nil, orderingExpr :: Nil)
        RangeBoundOrdering(ordering, current, bound)
      }
      case RowFrame => RowBoundOrdering(offset)
    }

    // Collect all window expressions
    val windowExprs = projectList.flatMap { expression =>
      expression.collect {
        case e: WindowExpression => e
      }
    }

    // Group the window expression by their processing frame.
    // TODO this gets a bit messy due to the different types of expressions we are considering.
    // TODO remove this when window functions are removed from the equation...
    val groupedEindowExprs = windowExprs.groupBy { e =>
      val tag = e.windowFunction match {
        case PatchedWindowFunction(_: PivotWindowExpression, _) => 'P'
        case PatchedWindowFunction(_: AggregateExpression, _) => 'A'
        case PatchedWindowFunction(_, _) => 'R'
      }
      (tag, e.windowSpec.frameSpecification)
    }

    // Create factories and collect unbound expressions for each frame.
    val factories = Buffer.empty[Seq[Row] => WindowFunctionFrame]
    val unboundExpressions = Buffer.empty[Expression]
    groupedEindowExprs.foreach {
      case (frame, unboundFrameExpressions) =>
        // Track the unbound expressions
        unboundExpressions ++= unboundFrameExpressions

        // Bind the expressions.
        val frameExpressions = unboundFrameExpressions.map { e =>
          // TODO remove this when window functions are removed from the equation...
          val functionExpression = e.windowFunction.asInstanceOf[PatchedWindowFunction].func
          BindReferences.bindReference(functionExpression, child.output)
        }.toArray
        def aggregateFrameExpressions = frameExpressions.map(_.asInstanceOf[AggregateExpression])

        // Create the factory
        val factory = frame match {
          // Shifting frame
          case ('R', SpecifiedWindowFrame(RowFrame, FrameBoundaryExtractor(low), FrameBoundaryExtractor(high))) if (low == high) => {
            input: Seq[Row] => new ShiftingWindowFunctionFrame(input, frameExpressions, low)
          }
          // Below
          case ('A', SpecifiedWindowFrame(frameType, UnboundedPreceding, FrameBoundaryExtractor(high))) => {
            val uBoundOrdering = createBoundOrdering(frameType, high)
            val factories = aggregateFrameExpressions
            input: Seq[Row] => new UnboundedPrecedingWindowFunctionFrame(input, factories, uBoundOrdering)
          }
          // Above
          case ('A', SpecifiedWindowFrame(frameType, FrameBoundaryExtractor(low), UnboundedFollowing)) => {
            val lBoundOrdering = createBoundOrdering(frameType, low)
            val factories = aggregateFrameExpressions
            input: Seq[Row] => new UnboundedFollowingWindowFunctionFrame(input, factories, lBoundOrdering)
          }
          // Sliding
          case ('A', SpecifiedWindowFrame(frameType, FrameBoundaryExtractor(low), FrameBoundaryExtractor(high))) => {
            val lBoundOrdering = createBoundOrdering(frameType, low)
            val uBoundOrdering = createBoundOrdering(frameType, high)
            val factories = aggregateFrameExpressions
            input: Seq[Row] => new SlidingWindowFunctionFrame(input, factories, lBoundOrdering, uBoundOrdering)
          }
          // Pivot
          case ('P', UnspecifiedFrame) => {
            val pivotFrameExpressions = frameExpressions.map(_.asInstanceOf[PivotWindowExpression])
            input: Seq[Row] => new PivotWindowFunctionFrame(input, pivotFrameExpressions)
          }
          // Global
          case ('A', UnspecifiedFrame) => {
            val factories = aggregateFrameExpressions
            input: Seq[Row] => new UnboundedWindowFunctionFrame(input, factories)
          }
        }
        factories += factory
    }

    // Create the schema projection.
    val unboundToAttr = unboundExpressions.map(e => (e, AttributeReference(s"aggResult:$e", e.dataType, e.nullable)()))
    val unboundToAttrMap = unboundToAttr.toMap
    val patchedProjectList = projectList.map(_.transform(unboundToAttrMap))
    val projection = newMutableProjection(patchedProjectList, child.output ++ unboundToAttr.map(_._2))

    // Done
    (factories.toArray, projection, factories.size, unboundExpressions.size)
  }

  protected override def doExecute(): RDD[Row] = {
    child.execute().mapPartitions { stream =>
      new Iterator[Row] {
        // Get all relevant projections.
        val result = projection()
        val grouping = newProjection(spec.partitionSpec, child.output)

        // Manage the stream and the grouping.
        var nextRow: Row = EmptyRow
        var nextGroup: Row = EmptyRow
        var nextRowAvailable: Boolean = false
        private[this] def fetchNextRow() {
          nextRowAvailable = stream.hasNext
          if (nextRowAvailable) {
            nextRow = stream.next()
            nextGroup = grouping(nextRow)
          } else {
            nextRow = EmptyRow
            nextGroup = EmptyRow
          }
        }
        fetchNextRow()

        // Manage the current partition.
        var rows: CompactBuffer[Row] = _
        var frames: Array[WindowFunctionFrame] = _
        private[this] def fetchNextPartition() {
          // Collect all the rows in the current partition.
          val currentGroup = nextGroup
          rows = new CompactBuffer
          while (nextRowAvailable && nextGroup == currentGroup) {
            rows += nextRow.copy()
            fetchNextRow()
          }

          // Setup the frames.
          frames = new Array[WindowFunctionFrame](factoryCount)
          var i = 0
          while (i < factoryCount) {
            frames(i) = factories(i)(rows)
            i += 1
          }

          // Setup iteration
          rowIndex = 0
          rowsSize = rows.size
        }

        // Iteration
        var rowIndex = 0
        var rowsSize = 0
        def hasNext: Boolean = {
          if (nextRowAvailable && rowIndex >= rowsSize) {
            fetchNextPartition()
          }
          rowIndex < rowsSize
        }

        val join = new JoinedRow6
        val windowFunctionResult = new GenericMutableRow(windowFunctionCount)
        def next(): Row = {
          if (hasNext) {
            // Get the results for the window functions.
            var i = 0
            var j = 0
            while (j < factoryCount) {
              val frame = frames(j)
              val frameSize = frame.count
              var k = 0
              while (k < frameSize) {
                windowFunctionResult.update(i, frame(rowIndex, k))
                k += 1
                i += 1
              }
              j += 1
            }

            // 'Merge' the input row with the window function result 
            join(rows(rowIndex), windowFunctionResult)
            rowIndex += 1

            // Return the projection.
            result(join)
          } else throw new NoSuchElementException
        }
      }
    }
  }
}

/**
 * Function for comparing boundary values.
 *
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
  def compare(input: Seq[Row], inputIndex: Int, outputIndex: Int): Int
}

/**
 * Compare the input index to the bound of the output index.
 */
private[execution] final case class RowBoundOrdering(offset: Int) extends BoundOrdering {
  override def compare(input: Seq[Row], inputIndex: Int, outputIndex: Int): Int = 
    inputIndex - (outputIndex + offset)
}

/**
 * Compare the value of the input index to the value bound of the output index.
 */
private[execution] final case class RangeBoundOrdering(
  ordering: Ordering[Row],
  current: Projection,
  bound: Projection) extends BoundOrdering {
  override def compare(input: Seq[Row], inputIndex: Int, outputIndex: Int): Int =
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
 *   - Use Tungsten style memory usage.
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
private[execution] final class ShiftingWindowFunctionFrame(
  rows: Seq[Row],
  exprs: Array[Expression],
  offset: Int) extends WindowFunctionFrame {
  val count = exprs.length
  def apply(row: Int, column: Int): Any = {
    val shiftedIndex = row + offset
    if (shiftedIndex >= 0 && shiftedIndex < rows.size) {
      exprs(column).eval(rows(shiftedIndex))
    } else null
  }
}

/**
 * Base class for dealing with aggregating window function frames.
 *
 * @param factories to create the aggregates with.
 */
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

      // Update aggregate.
      update(aggregates, input(inputIndex))
      
      // Move to the next row.
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

