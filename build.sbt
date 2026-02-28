name := "Nothor"
version := "1.0"
scalaVersion := "2.12.21"

// Main class
Compile / mainClass := Some("Main")

// Fork JVM for run
run / fork := true

// JVM options for Spark + Java 17
javaOptions ++= Seq(
  "-Djava.io.tmpdir=./tmp",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)

// Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % "3.3.0",
  "org.apache.spark" %% "spark-sql"   % "3.3.0",
  "org.apache.spark" %% "spark-mllib" % "3.3.0"
)