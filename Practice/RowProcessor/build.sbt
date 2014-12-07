lazy val root = (project in file(".")).
    settings(
        name := "RowProcessor",
        version := "1.0",
        scalaVersion := "2.10.4",
        libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.1"
    )
