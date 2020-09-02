name := "SparkSampleProject"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.0",
    "org.apache.spark" %% "spark-sql" % "2.3.0",
    "org.apache.spark" %% "spark-hive" % "2.3.0",
    "com.typesafe" % "config" % "1.3.0",
    "org.scalatest" %% "scalatest" % "3.0.0" % Test,
    "org.scalamock" %% "scalamock" % "4.4.0" % Test,
    "org.apache.spark" %% "spark-core" % "2.3.0" % Provided,
    "org.apache.spark" %% "spark-core" % "2.3.0" % Test,
    "org.apache.spark" %% "spark-core" % "2.3.0" % Test classifier "tests",
    "org.apache.spark" %% "spark-sql" % "2.3.0" % Provided,
    "org.apache.spark" %% "spark-sql" % "2.3.0" % Test,
    "org.apache.spark" %% "spark-sql" % "2.3.0" % Test classifier "tests",
    "org.apache.spark" %% "spark-catalyst" % "2.3.0" % Test,
    "org.apache.spark" %% "spark-catalyst" % "2.3.0" % Test classifier "tests",
    "org.apache.spark" %% "spark-hive" % "2.3.0" % Test,
    "org.apache.spark" %% "spark-hive" % "2.3.0" % Test classifier "tests",
)