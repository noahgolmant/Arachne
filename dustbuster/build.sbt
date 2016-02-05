val spark = "org.apache.spark" %% "spark-core" % "1.6.0"

lazy val root = (project in file(".")).
    settings(
        name := "dustbuster",
        version := "0.10",
        scalaVersion := "2.10.6",
        libraryDependencies += spark
    )

