# SPARK Window
This project aims to improve Window Functions for Spark SQL. It is currently Work in Progress. This 
implementation requires functionality provided by SPARK 1.4.0. 

## Improvements
The advantages over the current implementation are:
- Native Spark-SQL, the current implementation relies only on Hive UDAFs. The new 
  implementation uses Spark SQL Aggregates. Hive UDAF's are still supported though.
- Much better performance (10x) in running cases (e.g. BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  and UNBOUDED FOLLOWING cases.
- The ability to process all Frame Specifications within a single Window execution phase. This 
  improves memory usage and processing by a bit.  
- A lot of the staging code has been moved from the execution phase to the 'initialization' phase.
- Increased optimization opportunities. AggregateEvaluation style optimization is possible for in 
  frame processing. Tungsten might also provide interesting optimization opportunities.

## Usage
- Have a build of SPARK 1.4.0 ready, and make sure the JARS are installed to your local maven repo.
- Create an assembly using SBT.
```
sbt assembly
```
- Launch spark shell with the produced JAR (target/scala-2.10/spark-window_2.10-0.01.jar) attached.
- Execute the following code
```scala
import com.databricks.spark.csv._
import org.apache.spark.sql.WindowedData._
import org.apache.spark.sql.hive.WindowTestHiveContext

// Create a Hive Context with the enabled feature. 
val sqlContext = new WindowTestHiveContext(sc)
sqlContext.setConf("spark.sql.useWindow2Processor", "true")

// Load a CSV file
val airlines = sqlContext.csvFile("src/test/resources/allyears2k_headers.csv")

// Perform a windowed aggregation on the flights:
// - Filter flight from SAN.
// - Group by destination and order by date and time. 
// - Count how many delays there were in the past 3 Flights
// - Add a unique sequence number for the current group.
val w = window().partitionBy($"Dest").orderBy($"Year", $"Month", $"DayOfMonth", $"Deptime")
val flightsSAN = airlines.filter($"Origin" === "SAN").select(
  $"Origin"
  ,$"Dest"
  ,$"UniqueCarrier"
  ,$"FlightNum"
  ,$"TailNum"
  ,when($"IsDepDelayed" === "YES",1).otherwise(0).as("Delay")
  ,coalesce(sum(when($"IsDepDelayed" === "YES",1).otherwise(0)).over(w).rowsPreceding(3).rowsFollowing(-1), lit(0)).as("Past3Delays")
  ,rownumber().over(w).as("FlightSeqNr")
)
flightsSAN.show(100)
```
## Performance Comparison current Window implementation
One of the major improvements of this implementation is the performance gain in a number of cases. 
In order to do some proper comparison we need a bigger dataset. We are using the On Time Perfmance 
data for march 2015. You can download this dataset like this:
```
wget http://www.transtats.bts.gov/Download/On_Time_On_Time_Performance_2015_3.zip -P src/test/resources/
unzip src/test/resources/On_Time_On_Time_Performance_2015_3.zip -d src/test/resources/
```

### Initialization code
Execute the following code in your spark session:
```scala
import com.databricks.spark.csv._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.WindowedData._
import org.apache.spark.sql.hive.WindowTestHiveContext

// Create a Hive Context with the enabled feature. 
val sqlContext = new WindowTestHiveContext(sc)

// Load, Prune, Type & Cache the dataset
val otp = sqlContext.csvFile("src/test/resources/On_Time_On_Time_Performance_2015_3.csv").select(
   $"Origin"
   ,$"Dest"
   ,$"UniqueCarrier"
   ,$"FlightNum"
   ,$"TailNum"
   ,$"FlightDate".cast("date").as("FlightDate")
   ,$"CRSDepTime".cast("int").as("CRSDepTime")
   ,$"DepTime".cast("int").as("DepTime")
   ,$"DepDelayMinutes".cast("int").as("DepDelayMinutes")
   ,$"DepDel15".cast("int").as("DepDel15")
) 
otp.cache().count()

```
### Non-Pathlogical case
In this case we are comparing a sliding window, a total window and a row_number(). The 
performance is roughly equal.
```scala
// Define Query 1
def query1: DataFrame = {
  val window1 = window().partitionBy($"Origin").orderBy($"FlightDate", $"CRSDeptime")
  otp.select (
    $"Origin"
    ,$"Dest"
    ,$"UniqueCarrier"
    ,$"FlightNum"
    ,$"TailNum"
    ,$"FlightDate"
    ,$"CRSDepTime"
    ,$"DepTime"
    ,$"DepDelayMinutes"
    ,sum($"DepDel15").over(window1).rowsPreceding(3).rowsFollowing(-1).as("Past3DepDel15")
    ,sum($"DepDel15").over(window1).as("TotalDepDelay15")
    ,rownumber().over(window1).as("OriginFlightSeqNr")
  )
}

// Execute using new implementation (2 seconds)
sqlContext.setConf("spark.sql.useWindow2Processor", "true")
val flightsW1_new = query1.cache()
flightsW1_new.count

// Execute using current implementation (2 seconds)
sqlContext.setConf("spark.sql.useWindow2Processor", "false")
val flightsW1_current = query1.cache()
flightsW1_current.count

// Drop cached queries
flightsW1_new.unpersist()
flightsW1_current.unpersist()
```
### Pathological case
In this case we have tested a running sum. The performance of the current implementation is a 
factor 20 worse.
```scala
def query2: DataFrame = {
  val window1 = window().partitionBy($"Origin").orderBy($"FlightDate", $"CRSDeptime")
  otp.select (
    $"Origin"
    ,$"Dest"
    ,$"UniqueCarrier"
    ,$"FlightNum"
    ,$"TailNum"
    ,$"FlightDate"
    ,$"CRSDepTime"
    ,$"DepTime"
    ,$"DepDelayMinutes"
    ,sum($"DepDel15").over(window1).unboundedPreceding().rowsFollowing(0).as("RunningDepDelay15")
    ,sum($"DepDelayMinutes").over(window1).unboundedPreceding().rowsFollowing(0).as("RunningDepDelayMinutes")
  )  
}

// Execute using new implementation (2 seconds)
sqlContext.setConf("spark.sql.useWindow2Processor", "true")
val flightsW2_new = query2.cache()
flightsW2_new.count

// Execute using current implementation (41 seconds)
sqlContext.setConf("spark.sql.useWindow2Processor", "false")
val flightsW2_current = query2.cache()
flightsW2_current.count

// Drop cached queries
flightsW2_new.unpersist()
flightsW2_current.unpersist()

```

## TODO

### General
- [BLOCKING] Create PR
- [BLOCKING] Documentation
- [BLOCKING] Test execution phase.
- [BLOCKING] Adapt code to match Spark style guide.
- [BLOCKING] Drop own WindowDSL in favour of the one currently in development.
- [FOLLOWUP PR] Performance might improve as soon as Exchange starts supporting secondary sort.

### Logical
- [BLOCKING] Remove the WindowFunction concept. This interface exposes quite a bit of 
  implementation detail to catalyst.
- [BLOCKING] Introduce Hive Pivot Window Function
- [NICE TO HAVE] Introduce more window functions (can be made available using HiveUDAFS):
  - PERCENT_RANK()
  - NTILE()
  - PERCENTILE()
  - ... other pivotted Hive UDAFs
- [NICE TO HAVE] Find out if Row_Number warrants its own expression, currently implement as 
  Count(Literal(1)) over a running window.

### Execution
- [NICE TO HAVE] The current implementation is semi-blocking. It processes all the rows of the same 
  group at a time. There are a number of scenario's (i.e. Row Numbers or Running Sums) in which we 
  can do true or buffered (in case of positive shifts) streaming. 
- [FOLLOWUP PR] Add CodeGeneration where possible.

### Optimizer
- [FOLLOWUP PR] Prune columns
- [FOLLOWUP PR] Push filters through which have been defined on the partitioning columns.

### Analyzer 
- [BLOCKING] Allow the extract window expression to only create windows for different group by / 
  order by  clauses. 
- [NICE TO HAVE] Eliminate aggregates with a negative interval.
- [NICE TO HAVE] Eliminate zero shifts (is the row itself).
- [NICE TO HAVE] Eliminate entire WindowAggregate when nothing gets aggregated. Project instead.
- [NICE TO HAVE] Prevent layered (windowed) aggregates.
- [NICE TO HAVE] Revert singular range (low == high) or shifted simple aggregates (SUM, AVG, MAX, 
  MIN) into the backing expression. 
- [NICE TO HAVE] Fail when a range window without an aggregating expression is encountered.

### Parser
- [BLOCKING] Add proper function resolution to the HiveQL parser.
- [FOLLOWUP PR] Add window expressions to the regular parser.
