name := "sparklyr"

version := "0.1"

scalaVersion := "2.11.8"

scalaSource in Compile := baseDirectory.value / "spark-1.5.2"

unmanagedSourceDirectories in Compile += baseDirectory.value / "spark-1.6.0"

unmanagedSourceDirectories in Compile += baseDirectory.value / "spark-2.0.0"

libraryDependencies += "io.netty" % "netty-all" % "4.0.43.Final"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0"
