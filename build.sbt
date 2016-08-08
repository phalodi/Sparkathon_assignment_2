name := "assignment_ds"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
                      "org.apache.spark" %% "spark-core" % "2.0.0",
       		              "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
                        "org.apache.spark" %% "spark-hive" % "2.0.0"

)
