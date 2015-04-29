# SPARK Window
This project implements Window Functions for Spark SQL. In it is currently Work in Progress. This 
implementation requires functionality provided by SPARK 1.4.0.

## Usage
- Have a build of SPARK 1.4.0 ready, and make sure the JARS are installed to your local maven repo.
- Compile and package the code using SBT.
```
sbt package
```
- Launch spark shell with the produced JAR (target/scala-2.10/spark-window_2.10-0.01.jar) attached.
- Execute the following code
```scala
import com.databricks.spark.csv._
import org.apache.spark.sql.WindowedData._
import org.apache.spark.sql.ExtraSQLStrategy
import org.apache.spark.sql.execution.WindowedAggregateStrategy

// Add the windowed aggregate functionality to the context 
ExtraSQLStrategy.add(sqlContext)(WindowedAggregateStrategy)

// Conversion function for YES NO fields
val yesNoToInt = udf((raw: String) => if (raw == "YES") 1 else 0)

// Load a CSV file
val airlines = sqlContext.csvFile("src/test/resources/allyears2k_headers.csv")

// Perform a windowed aggregation on the flights:
// - Filter flight from SAN.
// - Group by destination and order by date and time. 
// - Count how many delays there were in the past 3 Flights
// - Add a unique sequence number for the current group. 
val flightsSAN = airlines.filter($"Origin" === "SAN").
                          window($"Dest")($"Year", $"Month", $"DayOfMonth", $"Deptime").
                          select(
  $"Origin"
  ,$"Dest"
  ,$"UniqueCarrier"
  ,$"FlightNum"
  ,$"TailNum"
  ,yesNoToInt($"IsDepDelayed").as("Delay")
  ,coalesce(interval(sum(yesNoToInt($"IsDepDelayed")), -3, -1), lit(0)).as("Past3Delays")
  ,rownumber().as("FlightSeqNr")
)
flightsSAN.show(100)
```

## TODO

### General
- Documentation
- Performance might improve as soon as Exchange starts supporting secondary sort.
- Integrate this effort with [PR5604](https://github.com/apache/spark/pull/5604).
- Create a true PR :)...
- Test execution phase.

### Logical
- Add value ranges and shift functionality to the Window Expression.
- Potentially add a window definition to the Window Expression. 
- Introduce functions:
  - ROW_NUMBER() Currently Below(Count(Literal(1)), 0) 
  - RANK() [Sort order based]
  - DENSE_RANK() [Sort order based]
  - PERCENT_RANK()
  - NTILE()
  - PERCENTILE()

### Execution
- The current implementation is semi-blocking. It processes all the rows of the same group at a 
  time. There are a number of scenario's (i.e. Row Numbers or Running Sums) in which we can do true
  or buffered (in case of positive shifts) streaming.   
- Add CodeGeneration for feeding the aggregate expressions.

### Optimizer
- Prune columns
- DO NOT push through filters.

### Analyzer 
- Add * resolution.
- Add sort expression to the to-be processed result. This is needed for functionality such a rank().
- Eliminate aggregates with a negative interval.
- Eliminate zero shifts (is the row itself).
- Eliminate entire WindowAggregate when nothing gets aggregated. Project instead.
- Convert Project into WindowedAggregate if there are any Window Expressions. This is only 
  recommended when we can express the window definition in the windowed expression. 
- Prevent layered (windowed) aggregates.
- Revert singular range (low == high) or shifted simple aggregates (SUM, AVG, MAX, MIN) into the 
  backing expression. 
- Fail when a range window aggregates without an aggregating expression is encountered. Another 
  option would be to construct an array.

### Parser
- Add window expressions to the parser.
