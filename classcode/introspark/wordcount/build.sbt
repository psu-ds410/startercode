lazy val root = (project in file("."))
  .settings(
      name := "word count",
      version := "1.0",
      scalaVersion := "2.11.12"
   )
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.2" % "provided"
)
