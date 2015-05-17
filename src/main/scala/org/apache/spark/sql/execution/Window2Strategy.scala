package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.ExtraSQLStrategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.{Window => LWindow}
import org.apache.spark.sql.catalyst.expressions.RankLikeExpression
import org.apache.spark.sql.catalyst.expressions.UnresolvedWindowSortOrder
import org.apache.spark.sql.catalyst.expressions.NamedExpression

case class Window2Strategy(sql: SQLContext) extends ExtraSQLStrategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case LWindow(projectList, windowExprList, spec, child) => {
        // Replace Unresolved Sort Orders with the actual orders.
        val patchedWindowExprList = windowExprList.map { e =>
          e.transform{
            case rnk:RankLikeExpression if (rnk.children == UnresolvedWindowSortOrder) => {
              rnk.withNewChildren(spec.orderSpec)
            }
          }.asInstanceOf[NamedExpression]
        }
        
        // Create the Window2 Strategy.
        Window2(projectList ++ patchedWindowExprList, spec, planLater(child)) :: Nil
      }
      case _ => Nil
    }
  }
}