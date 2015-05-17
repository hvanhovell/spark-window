package org.apache.spark.sql.execution

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.util.collection.CompactBuffer

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
 *
 * Parts of the core code borrowed from the org.apache.spark.sql.execution.Window class.
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
        ValueBoundOrdering(ordering, current, bound)
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
        case PatchedWindowFunction(_: PivotWindowExpression) => 'P'
        case PatchedWindowFunction(_: AggregateExpression) => 'A'
        case PatchedWindowFunction(_) => 'R'
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
         val functionExpression = e.windowFunction.asInstanceOf[PatchedWindowFunction].child
         BindReferences.bindReference(functionExpression, child.output)
        }.toArray
        def aggregateFrameExpressions = frameExpressions.map(_.asInstanceOf[AggregateExpression]) 

        // Create the factory
        val factory = frame match {
          // Shifting frame
          case ('R', SpecifiedWindowFrame(RowFrame, FrameBoundaryExtractor(low), FrameBoundaryExtractor(high))) if (low == high) => {
            input: Seq[Row] => new Shifting(input, frameExpressions, low)
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
    val projection = newMutableProjection(projectList, child.output ++ unboundToAttr.map(_._2))

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
 * Helper extractor for making working with frame boundaries easier.
 */
private[execution] object FrameBoundaryExtractor {
  def unapply(boundary: FrameBoundary): Option[Int] = boundary match {
    case CurrentRow => Some(0)
    case ValuePreceding(offset) => Some(-offset)
    case ValueFollowing(offset) => Some(offset)
    case _ => None
  }
}