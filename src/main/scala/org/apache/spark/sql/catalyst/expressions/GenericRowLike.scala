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

import scala.collection.mutable.ArraySeq
import org.apache.spark.sql.types.UTF8String
import scala.collection.mutable.WrappedArray

/**
 * Abstract Row Adapter class which only requires a valid apply and length method.
 * 
 * The implementation is an adapted version of {org.apache.spark.sql.catalyst.expressions.GenericRow}.  
 */
abstract class GenericRowLike extends Row {
  override def isNullAt(i: Int): Boolean = apply(i) == null

  override def getInt(i: Int): Int = {
    val raw = apply(i) 
    if (raw == null) sys.error("Failed to check null bit for primitive int value.")
    else raw.asInstanceOf[Int]
  }

  override def getLong(i: Int): Long = {
    val raw = apply(i) 
    if (raw == null) sys.error("Failed to check null bit for primitive long value.")
    else raw.asInstanceOf[Long]
  }

  override def getDouble(i: Int): Double = {
    val raw = apply(i) 
    if (raw == null) sys.error("Failed to check null bit for primitive double value.")
    else raw.asInstanceOf[Double]
  }

  override def getFloat(i: Int): Float = {
    val raw = apply(i) 
    if (raw == null) sys.error("Failed to check null bit for primitive float value.")
    else raw.asInstanceOf[Float]
  }

  override def getBoolean(i: Int): Boolean = {
    val raw = apply(i) 
    if (raw == null) sys.error("Failed to check null bit for primitive boolean value.")
    else raw.asInstanceOf[Boolean]
  }

  override def getShort(i: Int): Short = {
    val raw = apply(i) 
    if (raw == null) sys.error("Failed to check null bit for primitive short value.")
    else raw.asInstanceOf[Short]
  }

  override def getByte(i: Int): Byte = {
    val raw = apply(i) 
    if (raw == null) sys.error("Failed to check null bit for primitive byte value.")
    else raw.asInstanceOf[Byte]
  }

  override def getString(i: Int): String = {
    apply(i) match {
      case null => null
      case s: String => s
      case utf8: UTF8String => utf8.toString
    }
  }

  // TODO(davies): add getDate and getDecimal

  // Custom hashCode function that matches the efficient code generated version.
  override def hashCode: Int = {
    var result: Int = 37

    var i = 0
    while (i < length) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          apply(i) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte => b.toInt
            case s: Short => s.toInt
            case i: Int => i
            case l: Long => (l ^ (l >>> 32)).toInt
            case f: Float => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case other => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }

  override def equals(o: Any): Boolean = o match {
    case other: Row =>
      if (length != other.length) {
        return false
      }

      var i = 0
      while (i < length) {
        if (isNullAt(i) != other.isNullAt(i)) {
          return false
        }
        if (apply(i) != other.apply(i)) {
          return false
        }
        i += 1
      }
      true

    case _ => false
  }

  private def toArray: Array[Any] = Array.tabulate(length)(apply)
  override def copy(): Row = new GenericRow(toArray)
  override def toSeq: Seq[Any] = WrappedArray.make(toArray)
}