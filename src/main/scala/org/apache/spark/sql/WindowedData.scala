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

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.WindowedAggregate
import org.apache.spark.sql.catalyst.expressions.Count
import org.apache.spark.sql.catalyst.expressions.CustomWindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.analysis.ResolvedStar
import org.apache.spark.sql.catalyst.expressions.UnspecifiedFrame
import org.apache.spark.sql.catalyst.expressions.CurrentRow
import org.apache.spark.sql.catalyst.expressions.RowFrame
import org.apache.spark.sql.catalyst.expressions.UnboundedPreceding
import org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame
import org.apache.spark.sql.catalyst.expressions.UnboundedFollowing
import org.apache.spark.sql.catalyst.expressions.WindowFrame
import org.apache.spark.sql.catalyst.expressions.FrameBoundary
import org.apache.spark.sql.catalyst.expressions.FrameType
import org.apache.spark.sql.catalyst.expressions.ValuePreceding
import org.apache.spark.sql.catalyst.expressions.ValueFollowing
import org.apache.spark.sql.catalyst.expressions.ValuePreceding
import org.apache.spark.sql.catalyst.expressions.RangeFrame
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.Rank
import org.apache.spark.sql.catalyst.expressions.DenseRank
import org.apache.spark.sql.catalyst.expressions.UnresolvedWindowSortOrder
import org.apache.spark.sql.catalyst.expressions.PatchedWindowFunction

// A small DSL for working with window functions.
object WindowedData {
  implicit def colToExpr(col: Column) = col.expr
  implicit def exprToCol(expr: Expression) = Column(expr)

  // Shifted values.
  def lead(expr: Expression, lead: Int = 1): Expression = shift(expr, lead)
  def lag(expr: Expression, lag: Int = 1): Expression = shift(expr, -lag)
  def shift(expr: Expression, offset: Int): Expression = {
    // TODO move this to analyzer.
    if (offset == 0) expr
    else expr.over().shift(offset)
  }

  // Ranking functions.
  def rownumber(): Expression = Count(Literal(1)).over().running()
  def rank(): Expression = Rank(UnresolvedWindowSortOrder).over().running()
  def denseRank(): Expression = DenseRank(UnresolvedWindowSortOrder).over().running()
  
  // TODO add n_tile/percent_rank()/...
  
  def window(): WindowExpression = Literal(1).over()
  def window(spec: WindowSpecDefinition): WindowExpression = Literal(1).over(spec)
  def window(other: WindowExpression): WindowExpression = Literal(1).over(other)
  
  implicit def toWindowExpression(c: Column) = ToWindowExpression(c.expr)
  implicit class ToWindowExpression(val expr: Expression) {
    def over(newSpec: WindowSpecDefinition): WindowExpression = expr match {
      case WindowExpression(root, oldSpec) => {
        // Merge specifications! The New spec always has precedence of the old one.
        val partitionBy = if (!newSpec.partitionSpec.isEmpty) newSpec.partitionSpec 
        else oldSpec.partitionSpec
        val orderBy = if (!newSpec.orderSpec.isEmpty) newSpec.orderSpec 
        else oldSpec.orderSpec
        val frame = if (newSpec.frameSpecification != UnspecifiedFrame) newSpec.frameSpecification
        else oldSpec.frameSpecification
        
        // Create the window spec expression.
        WindowExpression(PatchedWindowFunction(expr), WindowSpecDefinition(partitionBy, orderBy, frame))
      }
      case _ => WindowExpression(PatchedWindowFunction(expr), newSpec)
    }
    def over(other: WindowExpression): WindowExpression = over(other.windowSpec)
    def over(): WindowExpression = over(WindowSpecDefinition(Nil, Nil, UnspecifiedFrame))
  }

  implicit class WindowExpressionDSL(val expr: WindowExpression) extends AnyVal {
    def partitionBy(partitionSpec: Expression*): WindowExpression =
      expr.copy(windowSpec = expr.windowSpec.copy(partitionSpec = partitionSpec))

    def orderBy(orderSpec: SortOrder*): WindowExpression =
      expr.copy(windowSpec = expr.windowSpec.copy(orderSpec = orderSpec))

    def running(): WindowExpression =
      modFrameSpec(SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))

    def shift(offset: Int): WindowExpression =
      modFrameSpec(SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))

    def rowsPreceding(rows: Int): WindowExpression = preceding(RowFrame, rows)

    def rowsFollowing(rows: Int): WindowExpression = following(RowFrame, rows)

    def valuesPreceding(values: Int): WindowExpression = preceding(RangeFrame, values)

    def valuesFollowing(values: Int): WindowExpression = following(RangeFrame, values)

    def unboundedPreceding(): WindowExpression = expr.windowSpec.frameSpecification match {
      case UnspecifiedFrame => expr
      case SpecifiedWindowFrame(_, _, UnboundedFollowing) =>
        modFrameSpec(UnspecifiedFrame)
      case SpecifiedWindowFrame(frameType, _, following) =>
        modFrameSpec(frameType, UnboundedPreceding, following)
    }

    def unboundedFollowing(): WindowExpression = expr.windowSpec.frameSpecification match {
      case UnspecifiedFrame => expr
      case SpecifiedWindowFrame(_, UnboundedPreceding, _) =>
        modFrameSpec(UnspecifiedFrame)
      case SpecifiedWindowFrame(frameType, preceding, _) =>
        modFrameSpec(frameType, preceding, UnboundedFollowing)
    }

    private[this] def preceding(frameType: FrameType, preceding: Int): WindowExpression = {
      val start = if (preceding == 0) CurrentRow else ValuePreceding(preceding)
      val end = specifyFrameSpec.frameEnd
      modFrameSpec(frameType, start, end)
    }

    private[this] def following(frameType: FrameType, following: Int): WindowExpression = {
      val start = specifyFrameSpec.frameStart
      val end = if (following == 0) CurrentRow else ValuePreceding(0)
      modFrameSpec(frameType, start, end)
    }

    private[this] def specifyFrameSpec: SpecifiedWindowFrame = expr.windowSpec.frameSpecification match {
      case UnspecifiedFrame => SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
      case frame: SpecifiedWindowFrame => frame
    }

    private[this] def modFrameSpec(frameType: FrameType, frameStart: FrameBoundary, frameEnd: FrameBoundary): WindowExpression =
      modFrameSpec(SpecifiedWindowFrame(frameType, frameStart, frameEnd))

    private[this] def modFrameSpec(newFrameSpec: WindowFrame): WindowExpression =
      expr.copy(windowSpec = expr.windowSpec.copy(frameSpecification = newFrameSpec))
  }
}


