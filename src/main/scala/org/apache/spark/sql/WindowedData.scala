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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Count
import org.apache.spark.sql.catalyst.expressions.CurrentRow
import org.apache.spark.sql.catalyst.expressions.DenseRank
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.FrameBoundary
import org.apache.spark.sql.catalyst.expressions.FrameType
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.PatchedWindowFunction
import org.apache.spark.sql.catalyst.expressions.RangeFrame
import org.apache.spark.sql.catalyst.expressions.Rank
import org.apache.spark.sql.catalyst.expressions.RowFrame
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame
import org.apache.spark.sql.catalyst.expressions.UnboundedFollowing
import org.apache.spark.sql.catalyst.expressions.UnboundedPreceding
import org.apache.spark.sql.catalyst.expressions.UnresolvedWindowSortOrder
import org.apache.spark.sql.catalyst.expressions.UnspecifiedFrame
import org.apache.spark.sql.catalyst.expressions.ValuePreceding
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowFrame
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition
import org.apache.spark.sql.catalyst.expressions.WindowSpec
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.ValueFollowing

// A small DSL for working with window functions.
object WindowedData {
  // Shifted values.
  def lead(expr: Expression, lead: Int = 1): Column = shift(expr, lead)
  def lag(expr: Expression, lag: Int = 1): Column = shift(expr, -lag)
  def shift(expr: Expression, offset: Int): Column = expr.over().shift(offset).toCol

  // Ranking functions.
  def rownumber(): Column = Count(Literal(1)).over().running().toCol
  def rank(): Column = Rank(UnresolvedWindowSortOrder).over().running().toCol
  def denseRank(): Column = DenseRank(UnresolvedWindowSortOrder).over().running().toCol
  
  // Create windows.
  def window(): WindowExpressionBuilder = Literal(1).over()
  def window(spec: WindowSpecDefinition): WindowExpressionBuilder = Literal(1).over(spec)

  implicit def toColumn(builder: WindowExpressionBuilder) = builder.toCol
  implicit def toBuilder(expr: Expression): ToWindowExpressionBuilder = ToWindowExpressionBuilder(Column(expr))
  implicit class ToWindowExpressionBuilder(val col: Column) {
    def over(newSpec: WindowSpecDefinition): WindowExpressionBuilder = {
      // Merge in the given specification into the existing aggregates
      val transformed = col.expr.transform {
        case WindowExpression(root, oldSpec) => {
          // Merge specifications! The New spec always has precedence of the old one.
          val partitionBy = if (!newSpec.partitionSpec.isEmpty) newSpec.partitionSpec
          else oldSpec.partitionSpec
          val orderBy = if (!newSpec.orderSpec.isEmpty) newSpec.orderSpec
          else oldSpec.orderSpec
          val frame = if (newSpec.frameSpecification != UnspecifiedFrame) newSpec.frameSpecification
          else oldSpec.frameSpecification

          // Create the window spec expression.
          WindowExpression(root, WindowSpecDefinition(partitionBy, orderBy, frame))
        }
      }

      // Make sure there is a window expression to manipulate.
      val wrapped = if (!transformed.fastEquals(col.expr)) transformed
      else WindowExpression(PatchedWindowFunction(col.expr), newSpec)

      // Return the builder.
      WindowExpressionBuilder(wrapped)
    }
    
    def over(other: WindowExpressionBuilder): WindowExpressionBuilder = {
      over(other.expr.collectFirst{
        case WindowExpression(_, spec) => spec
      }.get)
    }
    
    def over(): WindowExpressionBuilder = over(WindowSpecDefinition(Nil, Nil, UnspecifiedFrame))
  }

  case class WindowExpressionBuilder(val expr: Expression) {
    def partitionBy(partitionSpec: Column*): WindowExpressionBuilder =
      modWindowSpec(_.copy(partitionSpec = partitionSpec.map(_.expr)))

    def orderBy(orderSpec: Column*): WindowExpressionBuilder =
      modWindowSpec(_.copy(orderSpec = orderSpec.map { col =>
        col.expr match {
          case e: SortOrder => e
          case e => SortOrder(e, Ascending)
        }
      }))

    def running(): WindowExpressionBuilder =
      modFrameSpec(SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))

    def shift(offset: Int): WindowExpressionBuilder =
      modFrameSpec(SpecifiedWindowFrame(RowFrame, ValuePreceding(-offset), ValueFollowing(offset)))

    def rowsPreceding(rows: Int): WindowExpressionBuilder = preceding(RowFrame, rows)

    def rowsFollowing(rows: Int): WindowExpressionBuilder = following(RowFrame, rows)

    def valuesPreceding(values: Int): WindowExpressionBuilder = preceding(RangeFrame, values)

    def valuesFollowing(values: Int): WindowExpressionBuilder = following(RangeFrame, values)

    def unboundedPreceding(): WindowExpressionBuilder = modFrameSpec { frame =>
      frame match {
        case UnspecifiedFrame => frame
        case SpecifiedWindowFrame(_, _, UnboundedFollowing) =>
          UnspecifiedFrame
        case SpecifiedWindowFrame(frameType, _, following) =>
          SpecifiedWindowFrame(frameType, UnboundedPreceding, following)
      }
    }

    def unboundedFollowing(): WindowExpressionBuilder = modFrameSpec { frame =>
      frame match {
        case UnspecifiedFrame => frame
        case SpecifiedWindowFrame(_, UnboundedPreceding, _) =>
          UnspecifiedFrame
        case SpecifiedWindowFrame(frameType, preceding, _) =>
          SpecifiedWindowFrame(frameType, preceding, UnboundedFollowing)
      }
    }

    private[this] def preceding(frameType: FrameType, preceding: Int): WindowExpressionBuilder = modFrameSpec { frame =>
      val start = if (preceding == 0) CurrentRow else ValuePreceding(preceding)
      frame match {
        case UnspecifiedFrame => 
          SpecifiedWindowFrame(frameType, start, UnboundedFollowing)
        case SpecifiedWindowFrame(_, _, end) =>
          SpecifiedWindowFrame(frameType, start, end)
      }
    }
    
    private[this] def following(frameType: FrameType, following: Int): WindowExpressionBuilder = modFrameSpec { frame =>
      val end = if (following == 0) CurrentRow else ValueFollowing(following)
      frame match {
        case UnspecifiedFrame => 
          SpecifiedWindowFrame(frameType, UnboundedPreceding, end)
        case SpecifiedWindowFrame(_, start, _) =>
          SpecifiedWindowFrame(frameType, start, end)
      }
    }

    private[this] def modFrameSpec(newFrameSpec: WindowFrame): WindowExpressionBuilder =
      modWindowSpec(_.copy(frameSpecification = newFrameSpec))

    private[this] def modFrameSpec(f: WindowFrame => WindowFrame): WindowExpressionBuilder =
      modWindowSpec(w => w.copy(frameSpecification = f(w.frameSpecification)))

    private[this] def modWindowSpec(f: WindowSpecDefinition => WindowSpecDefinition): WindowExpressionBuilder = {
      val transformed = expr.transform {
        case e: WindowExpression => {
          val newSpec = f(e.windowSpec)
          if (newSpec == e.windowSpec) e
          else e.copy(windowSpec = newSpec)
        }
      }
      if (!transformed.fastEquals(expr)) WindowExpressionBuilder(transformed)
      else this
    }
    
    def toCol: Column = new Column(expr)
  }
}


