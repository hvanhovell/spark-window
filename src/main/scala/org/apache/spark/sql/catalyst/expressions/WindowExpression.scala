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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.trees.UnaryNode
import org.apache.spark.sql.catalyst.errors.`package`.TreeNodeException

/**
 * A window aggregate expression defines which relative range of input data should used to provide 
 * a value for the current row in the context of a WindowedAggregate operation.  
 * 
 * TODO Row orientation is currently implied. Value orientation also seems possible. Investigate 
 *      this.
 * TODO Add a full window specification to the expression. So include group by and order elements.
 * TODO Add documentation to explain the interactions between the three parameters.
 *      - An Expression is only possible when the window bounds are equal. In all other cases an 
 *        AggregateExpression required.
 *      - An AggregateExpression is pointless when the window bounds are equal. 
 *      - A WindowExpression is pointless when both the lower and upper windows bound are 0. 
 */
case class WindowExpression(child: Expression, lower: Option[Int], upper: Option[Int]) extends UnaryExpression {
  def nullable: Boolean = true
  def dataType = child.dataType
  def eval(input: Row = null): EvaluatedType = {
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")
  }
  override def toString = (lower, upper) match {
    case (None, None) => s"WINDOW($child)"
    case (Some(offset), None) => s"ABOVE($child, $offset)"
    case (None, Some(0)) => s"RUNNING($child)"
    case (None, Some(offset)) => s"BELOW($child, $offset)"
    case (Some(low), Some(high)) if (low == high) => s"SHIFT($child, $low)"
    case (Some(low), Some(high)) => s"RANGE($child, $low, $high)"
  }
}