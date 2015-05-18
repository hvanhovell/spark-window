package org.apache.spark.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.execution.ExtractPythonUdfs
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.sources.PreInsertCastAndRename
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.RankLikeExpression
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.catalyst.expressions.UnresolvedWindowFunction
import org.apache.spark.sql.catalyst.expressions.PatchedWindowFunction
import org.apache.spark.sql.catalyst.expressions.Count
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.expressions.DenseRank
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.Window2
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.UnspecifiedFrame
import org.apache.spark.sql.catalyst.expressions.Rank

class WindowTestHiveContext(sc: SparkContext) extends HiveContext(sc) {
  protected[sql] def useWindow2Processor: Boolean =
    getConf("spark.sql.useWindow2Processor", "true") == "true"

  @transient
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
          catalog.CreateTables ::
          catalog.PreInsertionCasts ::
          ExtractPythonUdfs ::
          ResolveWindowOrderExpressions ::
          ResolvePatchedWindowFunctions ::
          ResolveHiveWindowFunction ::
          PreInsertCastAndRename ::
          Nil
    }

  // Replace window sort orders of all rank functions.
  object ResolveWindowOrderExpressions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case window @ Window(_, _, spec, _) =>
        window transformExpressions {
          case re: RankLikeExpression => re.withNewChildren(spec.orderSpec)
        }
    }
  }

  // Make DSL expression usable in the current implementation. 
  object ResolvePatchedWindowFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case p: LogicalPlan if (!useWindow2Processor) =>
        p transformExpressions {
          case WindowExpression(PatchedWindowFunction(Count(Literal(lit, _)), _), spec) if (lit != null) => 
            WindowExpression(UnresolvedWindowFunction("row_number", Nil), spec.copy(frameSpecification = UnspecifiedFrame))  
          case WindowExpression(PatchedWindowFunction(DenseRank(_), _), spec) => 
            WindowExpression(UnresolvedWindowFunction("dense_rank", Nil), spec.copy(frameSpecification = UnspecifiedFrame))
          case WindowExpression(PatchedWindowFunction(Rank(_), _), spec) => 
            WindowExpression(UnresolvedWindowFunction("rank", Nil), spec.copy(frameSpecification = UnspecifiedFrame))
          case PatchedWindowFunction(expr, children) => UnresolvedWindowFunction(expr.nodeName.toLowerCase(), children)
        }
    }
  }
  
  // Add the Window2 Strategy...
  object WindowStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Window(projectList, windowExprList, spec, child) if (useWindow2Processor) => {
        Window2(projectList ++ windowExprList, spec, planner.plan(child).next()) :: Nil
      }
      case _ => Nil
    }
  }
  
  experimental.extraStrategies = experimental.extraStrategies :+ WindowStrategy
}