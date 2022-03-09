package io.hlsg.utils

import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}
import io.projectglow.Glow

class ArgsVCF2Delta(arguments: Seq[String]) extends ScallopConf(arguments) {
  val vdfPath: ScallopOption[String] = opt[String](required = true, descr = "VCF File Path")
  val savePath: ScallopOption[String] = opt[String](required = true, descr = "Delta Save Path")
  verify()
}


object VCF2Delta {
  def main(arguments:Array[String]): Unit = {

    val args = new ArgsVCF2Delta(arguments)

    val spark = SparkSession
      .builder()
      .appName("VCF2Delta")
      .master("local[4]")
      .getOrCreate()

    Glow.register(spark)

    val df = spark.read.format("vcf")
      .option("includeSampleIds", "false")
      .option("flattenInfoFields", "true")
      .load(args.vdfPath())

    df.printSchema()
    df.show(20, truncate = false)

    df
      .write
      .mode("overwrite")
      .format("delta")
      .save(args.savePath())
  }

}
