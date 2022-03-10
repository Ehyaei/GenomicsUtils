package io.hlsg.utils
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ArgsDFMerger(arguments: Seq[String]) extends ScallopConf(arguments) {
  val csvPath: ScallopOption[String] = opt[String](required = true, descr = "CSV File Path")
  val deltaPath: ScallopOption[String] = opt[String](required = true, descr = "Delta File Path")
  val byX: ScallopOption[String] = opt[String](required = true, descr = "Name of First data column Used for Merging")
  val byY: ScallopOption[String] = opt[String](required = true, descr = "Name of Second data column Used for Merging")
  val savePath: ScallopOption[String] = opt[String](required = true, descr = "CSV Save Path")
  verify()
}

object CSVJoinDelta {
  def main(arguments:Array[String]): Unit = {

    val args = new ArgsDFMerger(arguments)

    val spark = SparkSession
      .builder()
      .appName("VCF2Delta")
      .master("local[4]")
      .getOrCreate()

    println("Read CSV Data")
    val dfCSV = spark.read.format("csv").option("header","true").load(args.csvPath())
    dfCSV.printSchema()

    println("Load Delta Data")
    val dfVCF = spark.read.format("delta").load(args.deltaPath())
    dfVCF.printSchema()

    println("Remove Array columns to write as csv")
    val arrayColumn = for(c <- dfVCF.columns if dfVCF.schema(c).dataType.typeName == "array") yield c
    val dfVCFUnArray = dfVCF.drop(arrayColumn:_*)

    println("Start Merging and Writing ...")

    dfCSV
      .join(dfVCFUnArray, dfCSV(args.byX()) === dfVCF(args.byY()), joinType = "left")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .csv(args.savePath())

  }
}
