ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "ClickstreamProject",

    // Dependencies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.7",
      "org.apache.spark" %% "spark-sql" % "2.4.7"),
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test,
    libraryDependencies += "com.typesafe" % "config" % "1.4.1",


  )

// Main class declaration
//specifying the main class for your project when you create an assembly JAR.
mainClass in(assembly) := Some("mainPackage.ClickStreamPipeLineMain")
















assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard //Discard metadata files while merging in "META-INF,"
  case "log4j.properties" => MergeStrategy.last //. This ensures that the final JAR includes the log4j properties from the last JAR encountered.
  case x => MergeStrategy.first // case x => MergeStrategy.first: For any other file (x represents a wildcard), // choose the first encountered
  // version during the merge. This means if there are conflicting files, the one from the first encountered JAR will be included.
}


// it's telling the build tool that when you create a JAR with all your
// project dependencies bundled (assembly JAR), the main class to run should
// be mainPackage.ClickStreamPipeLineMain.





