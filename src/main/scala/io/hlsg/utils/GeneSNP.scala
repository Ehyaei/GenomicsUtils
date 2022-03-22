package io.hlsg.utils

import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}
import io.projectglow.Glow
import org.apache.spark.sql.functions._

class ArgsGeneSNP(arguments: Seq[String]) extends ScallopConf(arguments) {
  val geneBankPath: ScallopOption[String] = opt[String](descr = "Gene Bank Read Path Directory")
  val geneBankSavePath: ScallopOption[String] = opt[String](descr = "Gene Bank Save Path Directory (Delta Format)")
  val geneTableSavePath: ScallopOption[String] = opt[String](required = true, descr = "Gene Table Save Path Directory (Delta Format)")
  val sbpBankPath: ScallopOption[String] = opt[String](descr = "SNP Bank Read Path Directory")
  val snpTableSavePath: ScallopOption[String] = opt[String](required = true, descr = "Gene Bank Save Path Directory (Delta Format)")
  val geneDiameter: ScallopOption[Int] = opt[Int](default = Some(10000), required = true, descr = "Gene Bank Save Path Directory (Delta Format)")
  val snpGeneTableSavePath: ScallopOption[String] = opt[String](required = true, descr = "SNP Gene Bank Save Path Directory (Delta Format)")
  val createDB: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Gene Bank Save Path Directory (Delta Format)")

  verify()
}

object GeneSNP {
  def main(arguments:Array[String]): Unit = {
    val args = new ArgsGeneSNP(arguments)

    val spark = SparkSession
      .builder()
      .appName("VCF2Delta")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    Glow.register(spark)


    if(args.createDB()){
      // Read and Save geneBank
      spark.read.format("gff")
        .load(args.geneBankPath())
        .write.mode("overwrite")
        .format("delta")
        .save(args.geneBankSavePath())

      // Read and Save geneBank
      val geneColumns = Array("ID", "Name","source", "type","seqId",  "start", "end", "strand", "Parent")
      spark.read.parquet(args.geneBankSavePath())
        .filter($"source" =!= "GRCh38")
        .withColumn("Parent", explode_outer($"Parent"))
        .withColumnRenamed("type","subType")
        .withColumn("type",split(col("ID"),":")(0))
        .withColumn("type",
          when($"subType" === "exon", "exon")
            .when($"subType" === "biological_region", "region")
            .when($"subType" === "three_prime_UTR", "UTR")
            .when($"subType" === "five_prime_UTR", "UTR")
            .otherwise($"type"))
        .withColumn("ID",split(col("ID"),":")(1))
        .withColumn("ID",when(isnull($"ID"), split(col("Parent"),":")(1)).otherwise($"ID"))
        .withColumn("ID",when(isnull($"ID"), $"Name").otherwise($"ID"))
        .withColumn("LID",concat($"seqId",lit("_"), $"start",lit("_"), $"end"))
        .select(("LID"+:geneColumns).map(col):_*)
        .write.mode("overwrite")
        .format("delta")
        .save(args.geneTableSavePath())


      // Create SNP Table
      spark.read.format("vcf")
        .load(args.sbpBankPath())
        .withColumn("ID",explode(col("names")))
        .select(
          col("ID"),
          col("contigName").as("CH"),
          col("start").as("POS"),
          col("end").as("TAIL"))
        .repartition(1000)
        .write.mode("overwrite")
        .format("delta")
        .save(args.snpTableSavePath())
    }


    val diam = args.geneDiameter()
    val range = 1000000

    val dbGene = spark.read.parquet(args.geneTableSavePath())
      .filter($"type" === "gene")
      .select($"LID",$"seqId".as("CH"), $"start", $"end")
      .withColumn("lblock", floor(($"start"-diam)/range))
      .withColumn("rblock", floor(($"end"+diam)/range))
      .distinct
      .cache

    val dbSNP = spark.read.parquet(args.snpTableSavePath())
      .select($"ID", $"CH", $"POS", $"END".as("Finish"))
      .withColumn("lblock", floor($"POS"/range))
      .withColumn("rblock", floor($"Finish"/range))
      .distinct
      .cache


    val long_Gene  = dbGene.withColumn("length", $"end"-$"start")
      .filter($"length">=(range-2*diam)).drop("length")

    dbGene.count
    dbSNP.count

    // Left Matching

    val snp_gene_left = dbSNP
      .join(dbGene, Seq("CH","lblock"))
      .filter($"POS">= $"start"-diam &&  $"Finish"<= $"end"+diam)
      .drop("lblock","rblock")

    // Right Matching
    val snp_gene_right = dbSNP
      .join(dbGene, Seq("CH","rblock"))
      .filter($"POS">= $"start"-diam &&  $"Finish"<= $"end"+diam)
      .drop("lblock","rblock")

    // Long Matching
    val snp_gene_long =  dbSNP
      .join(long_Gene, Seq("CH"))
      .filter($"POS">= $"start"-diam &&  $"Finish"<= $"end"+diam)
      .drop("lblock","rblock")

    // Long Matching
    val snp_gene = snp_gene_left.union(snp_gene_right).union(snp_gene_long).distinct

    val subdbGene = spark.read.parquet(args.geneTableSavePath())
      .filter($"type" === "gene")
      .drop("seqId", "start", "end").withColumnRenamed("ID","GID")

    snp_gene.join(subdbGene, Seq("LID"))
      .drop("LID")
      .withColumn("diameter", array_max(array(lit(0),array_max(array($"start"-$"POS",$"Finish"-$"end")))))
      .write.mode("overwrite")
      .format("delta")
      .save(args.snpGeneTableSavePath())

  }
}
