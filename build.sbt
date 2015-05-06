name := "spark-window"

version := "0.01"

organization := "nl.questtec"

scalaVersion := "2.10.4"

// Add dependency graph
net.virtualvoid.sbt.graph.Plugin.graphSettings

isSnapshot := true

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.4.0-SNAPSHOT" % "provided",
	"org.apache.spark" %% "spark-sql" % "1.4.0-SNAPSHOT" % "provided",
	"org.apache.spark" %% "spark-hive" % "1.4.0-SNAPSHOT" % "provided",
	"org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
	"com.google.protobuf" % "protobuf-java" % "2.5.0" % "provided",
	"net.java.dev.jets3t" % "jets3t" % "0.9.3" % "provided",
	"com.databricks" %% "spark-csv" % "1.0.3",
	"org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

resolvers ++= Seq(
	"Akka Repository" at "http://repo.akka.io/releases/",
	Resolver.mavenLocal
)

compileOrder := CompileOrder.JavaThenScala

// Make eclipse also download sources.
EclipseKeys.withSource := true

// Put back compile time "provided" dependencies
// http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
