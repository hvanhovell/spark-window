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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.expressions.EmptyRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.JoinedRow4
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.CustomWindowExpression
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.Sum

/**
 * :: DeveloperApi ::
 * This class calculates and outputs (windowed) aggregates over the rows in a single sorted group.
 * The aggregates are calculated for each row in the group. An aggregate can take a few forms:
 * - Global: The aggregate is calculated for the entire group. Every row has the same value.
 * - Range: The aggregate is calculated based on a subset of the window, and is unique for each 
 *   row and depends on the position of the given row within the window. The group must be sorted 
 *   for this to produce sensible output. Examples are moving averages, running sums and row 
 *   numbers. 
 * - Shifted: The aggregate is a displaced value relative to the position of the given row. 
 *   Examples are Lead and Lag. 
 * 
 * This is quite an expensive operator because every row for a single group must be in the same 
 * partition and partitions must be sorted according to the grouping and sort order. This can be 
 * infeasible in some extreme cases. The operator does not repartition or sort itself, but requires
 * the planner to this; this is currently the fastest approach (after micro benchmarking designs 
 * ranging from partition only to the current delegated full sorted design). 
 * 
 * The current implementation is semi-blocking. The aggregates and final project are calculated one 
 * group at a time, this is possible due to the aforementioned partitioning and ordering 
 * constraints. 
 * 
 * Parts of the projection manipulation scheme are borrowed from the 
 * org.apache.spark.sql.execution.Aggregate class.
 */
@DeveloperApi
case class WindowedAggregate(groupingExprs: Seq[Expression],
  order: Seq[SortOrder],
  aggregateExprs: Seq[NamedExpression],
  child: SparkPlan) extends UnaryNode {

  def output: Seq[Attribute] = aggregateExprs.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingExprs == Nil) AllTuples :: Nil
    else ClusteredDistribution(groupingExprs) :: Nil
  }

  def groupingOrder = groupingExprs.map(SortOrder(_, Ascending))
  
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(outputOrdering)
  
  override def outputOrdering: Seq[SortOrder] =  groupingOrder ++ order

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   * @param factory The factory used to calculate the result.
   */
  case class ComputedWindowExpression(
    unbound: Expression,
    attribute: AttributeReference,
    factory: Seq[Row] => WindowFunction)

  private[this] val computedWindowExpressions = {
    // Shorthand for computed expression
    def computedExpr(expr: Expression, factory: Seq[Row] => WindowFunction) = {
      ComputedWindowExpression(expr, AttributeReference(s"aggResult:expr", expr.dataType, expr.nullable)(), factory)
    }
    
    // Shorthand for bind references.
    def bind[E <: Expression](expr: E): E =  BindReferences.bindReference(expr, child.output)
    
    // Collect All Window Expressions
    aggregateExprs.flatMap { aggregateExpr =>
      // Collect WindowExpressions
      val windowExprs = aggregateExpr.collect {
        case anchor@CustomWindowExpression(expr: AggregateExpression, None, None) => 
            computedExpr(anchor, (rows: Seq[Row]) => new GlobalAggregateWindowFunction(bind(expr), rows))
          case anchor@CustomWindowExpression(expr: AggregateExpression, None, Some(offset)) => 
            computedExpr(anchor, (rows: Seq[Row]) => new BelowAggregateWindowFunction(bind(expr), rows, offset))
          case anchor@CustomWindowExpression(expr: AggregateExpression, Some(offset), None) => 
            computedExpr(anchor, (rows: Seq[Row]) => new AboveAggregateWindowFunction(bind(expr), rows, offset))
          case anchor@CustomWindowExpression(expr: AggregateExpression, Some(low), Some(high)) => 
            computedExpr(anchor, (rows: Seq[Row]) => new RangeAggregateWindowFunction(bind(expr), rows, low, high))
          case anchor@CustomWindowExpression(expr, Some(low), Some(high)) if (low == high) => 
            computedExpr(anchor, (rows: Seq[Row]) => new ShiftingWindowFunction(bind(expr), rows, low))
      }

      // TODO move the code below to the planner.
      // Collect all the Aggregate Expressions contained by a window.
      val windowContainedAggregateExprs = windowExprs.collect {
        case ComputedWindowExpression(CustomWindowExpression(aggregateExpr: AggregateExpression, _, _), _, _) => aggregateExpr
      }.toSet
      
      // Collect the bare Aggregate Expressions.
      val bareAggregateExprs = aggregateExpr.collect {
        case anchor: AggregateExpression if (!windowContainedAggregateExprs.contains(anchor)) => {
          computedExpr(anchor, (rows: Seq[Row]) => new GlobalAggregateWindowFunction(bind(anchor), rows))
        }
      }
      
      windowExprs ++ bareAggregateExprs
    }.toArray
  }

  private[this] val projectExpr = {
    // Replace the window expressions by their resulting attribute.
    val windowExpr2Attribute = computedWindowExpressions.map(cwe => cwe.unbound -> cwe.attribute).toMap
    aggregateExprs.map(_.transform(windowExpr2Attribute))
  }

  private[this] val projectSchema = child.output ++ computedWindowExpressions.map(_.attribute)

  def doExecute(): RDD[Row] = attachTree(this, "execute") {
    child.execute().mapPartitions(iterator => {
      new Iterator[Row] {
        val result = newMutableProjection(projectExpr, projectSchema)()
        val grouping = newProjection(groupingExprs, child.output)
        
        // Cache the iterator for group detection.
        val partitionIterator = iterator.buffered

        // Get the group of the next row.
        def peekNextGroup: Row = if (partitionIterator.hasNext) grouping(partitionIterator.head) else null

        // Construct the group iterator for the next group. 
        def constructGroupIterator: Iterator[Row] = {
          val group = peekNextGroup
          val buffer = CompactBuffer[Row]
          
          // Collect all rows for a group.
          var nextGroup = group
          while (partitionIterator.hasNext && group == nextGroup) {
            val row = partitionIterator.next().copy()
            buffer += row
            
            // Determine the next group
            nextGroup = peekNextGroup
          }
          
          // Calculate the aggregates.
          val aggregates = new Array[WindowFunction](computedWindowExpressions.length)
          var i = 0
          while (i < computedWindowExpressions.length) {
            aggregates(i) = computedWindowExpressions(i).factory(buffer)
            i += 1
          }
          
          // Return a iterator group rows augmented with the aggregates. 
          new Iterator[Row] {
            val inputRows = buffer.iterator
            val aggregateRows = new WindowFunctionRowView(buffer.size, aggregates)
            val join = new JoinedRow4(EmptyRow, EmptyRow)
            def hasNext = inputRows.hasNext
            def next(): Row = result(join(inputRows.next(), aggregateRows.next()))
          }
        }

        // Iterator producing the rows for one group.
        var groupIterator: Iterator[Row] = constructGroupIterator

        def hasNext = partitionIterator.hasNext || groupIterator.hasNext

        def next = {
          if (!groupIterator.hasNext) {
            groupIterator = constructGroupIterator
          }
          groupIterator.next()
        }
      }
    }, true)
  }
}





