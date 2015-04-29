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
import org.apache.spark.sql.catalyst.expressions.WindowExpression

// A number of the optimalizations here should be in the analyser...
object WindowedData {
  implicit def colToExpr(col: Column) = col.expr
  implicit def exprToCol(expr: Expression) = Column(expr)

  // Shifted values.
  def lead(expr: Expression, lead: Int = 1): Expression = shift(expr, lead)
  def lag(expr: Expression, lag: Int = 1): Expression = shift(expr, -lag)
  def shift(expr: Expression, offset: Int): Expression = {
    // TODO move this to analyzer.
    if (offset == 0) expr
    else WindowExpression(expr, Some(offset), Some(offset))
  }

  // Window Aggregates.
  def running(expr: Expression): Expression = below(expr, 0)
  def above(expr: Expression, offset: Int = 0): Expression = WindowExpression(expr, Some(offset), None)
  def below(expr: Expression, offset: Int = 0): Expression = WindowExpression(expr, None, Some(offset))
  def interval(expr: Expression, low: Int = 0, high: Int = 0): Expression = {
    if (high < low) Literal.create(null, expr.dataType)
    else if (high == low) shift(expr, low)
    else WindowExpression(expr, Some(low), Some(high))
  }

  // Row number
  // TODO RANK, DENSE_RANK(), N_TILE are (quite) a bit harder... 
  def rownumber(): Expression = running(Count(Literal(1)))

  /**
   * Add Window functionality to the data frame.
   */
  implicit class WindowDataFrame(val df: DataFrame) extends AnyVal {
    /**
     * Create a sorted window.
     */
    def window(groupingCols: Column*)(sortCols: Column*): WindowedData = {
      val groupingExprs = groupingCols.map(_.expr)
      val order = sortCols.map { sortCol =>
        sortCol.expr match {
          case expr: SortOrder => expr
          case expr: Expression => SortOrder(expr, Ascending)
        }
      }
      WindowedData(df, groupingExprs, order)
    }
  }
}

case class WindowedData(df: DataFrame, groupingExprs: Seq[Expression], order: Seq[SortOrder]) {
  @scala.annotation.varargs
  def select(expr: Column, exprs: Column*): DataFrame = {
    val namedExprs = (expr +: exprs).map(_.expr).map {
      case expr: NamedExpression => expr
      case expr: Expression => Alias(expr, expr.prettyString)()
    }
    DataFrame(df.sqlContext, WindowedAggregate(groupingExprs, order, namedExprs, df.logicalPlan))
  }
}