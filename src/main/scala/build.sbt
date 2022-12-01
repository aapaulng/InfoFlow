name                := "InfoFlow"
version             := "1.1.1"
scalaVersion := "2.11.12"
parallelExecution   := false
libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "2.1.1",
        "org.scalatest" %% "scalatest" % "3.0.1" % "test",
        "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"
)
