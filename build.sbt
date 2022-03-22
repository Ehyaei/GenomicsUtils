version := "0.1"
scalaVersion := "2.12.3"
assemblyJarName := "GenomicsUtils.jar"
val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "io.projectglow" %% "glow-spark3" % "1.1.2" % "provided",
  "org.ddahl" %% "rscala" % "3.2.19",
  "io.delta" %% "delta-core" % "1.0.1",
  "org.rogach" %% "scallop" % "3.5.1"
)

assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}