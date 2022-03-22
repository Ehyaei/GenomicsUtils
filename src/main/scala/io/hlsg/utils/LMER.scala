package io.hlsg.utils

import org.apache.spark.sql.SparkSession
import org.ddahl.rscala.RClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.rogach.scallop.{ScallopConf, ScallopOption}


class ArgsLMER(arguments: Seq[String]) extends ScallopConf(arguments) {
  val phenoPath: ScallopOption[String] = opt[String](required = true, descr = "Pheno Type File Path")
  val genoPath: ScallopOption[String] = opt[String](required = true, descr = "Geno Type File Path")
  val baseDataPath: ScallopOption[String] = opt[String](required = true, descr = "Model Base Data in rds Type")
  val partitionsPath: ScallopOption[String] = opt[String](required = true, descr = "Directory Path for Data Partitions")
  val partitionsModel: ScallopOption[String] = opt[String](required = true, descr = "Directory Path for Model Coefficient Partitions")
  verify()
}

object LMER extends Serializable{
  def main(arguments:Array[String]): Unit = {

    val args = new ArgsLMER(arguments)


    // Turn off logs
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("HelloR")
      .getOrCreate()

    import spark.implicits._

    val pheno_df = spark.read.option("header","true")
      .csv(args.phenoPath())
      .filter(!isnull(col("MMSE")))
      .cache()

    val phenoType = pheno_df.select(
      pheno_df.columns
        .map(colName => col(s"`$colName`").as(colName.replaceAll("\\.", "_").replaceAll(" ", "_"))): _*
    )
    phenoType.count()

    val genoType = spark.read.option("header","true")
      .csv(args.genoPath())
      .cache()
    genoType.count()

    val phenoCols = phenoType.columns
    val genoCols = genoType.columns

    val win = Window.partitionBy("dummy").orderBy("dummy")

    val phenoGeno =  phenoType
      .join(genoType, Seq("PTID"))
      .withColumn("dummy",lit("!"))
      .withColumn("ID", row_number().over(win).cast("String"))
      .drop("dummy")

    val snpData = phenoGeno.select({"ID"+:genoCols}.map(col):_*)
    val baseData = phenoGeno.select({"ID"+:phenoCols}.map(col):_*)
      .collect.map(x => {0 to phenoCols.length}.map(x.getString).toArray)

    val R = RClient()
    R.eval("base_data = %-", baseData)
    R.eval("base_names = %-", "ID"+:phenoCols)
    R.eval("base_data_path = %-", args.baseDataPath())

    R.eval("""
    suppressMessages(library(lmerTest))
    suppressMessages(library(dplyr))
    options(warn=-1)
    options(message=-1)
    base_data = as.data.frame(base_data)
    colnames(base_data) = base_names

    base_data = base_data %>%
        mutate(
        AGE = as.numeric(AGE),
        PTEDUCAT = as.numeric(PTEDUCAT),
        MMSE = as.numeric(MMSE),
        MMSE_bl = as.numeric(MMSE_bl),
        ID = as.numeric(ID)
        ) %>%
        arrange("ID")

    readr::write_rds(base_data, file = base_data_path)
""")
    R.quit()

    snpData
      .select(
        col("ID"),
        explode(array(
          snpData.columns.filter(_.contains("rs")).map(c => struct(lit(c).as("SNP"), col(c).alias("number"))): _*
        ))
      )
      .select("ID", "col.*")
      .repartition(100, $"SNP")
      .write.option("header","true")
      .mode("overwrite").csv(args.partitionsPath())

    val snpPartitions = spark.read.option("header","true").csv(args.partitionsPath())
    //    val colNumber = snpPartitions.columns.length


    val model = snpPartitions.rdd.mapPartitionsWithIndex{ (index, iterator) =>
      val R = RClient()
      //      val partition = iterator.map(it => Array( {0 until colNumber}.map(it.getString):_*)).toArray
      R.eval("base_data_path = %-", args.baseDataPath())
      R.eval("partitions_path = %-", args.partitionsPath())
      R.eval("partitions_model = %-", args.partitionsModel())
      R.eval("partition_number= %-", index)
      //      R.eval("partition = %-", partition)
      R.eval(
        """
          |suppressMessages(library(dplyr))
          |suppressMessages(library(lmerTest))
          |options(warn=-1)
          |options(message=-1)
          |
          |file_name =  sprintf("part-%s", stringr::str_pad(partition_number, 5, "left",pad = "0"))
          |partition = list.files(partitions_path, full.names = T, pattern = file_name) %>% readr::read_csv()
          |base_data <- readr::read_rds(base_data_path)
          |model_data <- partition %>% tidyr::spread(key = "SNP", value = "number") %>%
          |  merge(base_data,by = "ID")
          |
          |fit =  lmerTest::lmer(MMSE~MMSE_bl+PTEDUCAT+AGE+(1|PTID), REML = TRUE,data = model_data,
          |                      control = lmerControl(calc.derivs = FALSE, optimizer = "bobyqa"))
          |
          |geno_terms = unique(partition$SNP)
          |
          |geno_to_coef = function(x){
          |  tryCatch({
          |    summary(update(fit, eval(paste(". ~ . +",x))))$coefficients %>%
          |      as_tibble() %>% slice(n()) %>%
          |      mutate(SNP = x) %>%
          |      select("SNP", "Estimate", std = "Std. Error", "df",
          |             t_value = "t value", p_value = "Pr(>|t|)")
          |  },
          |  error = function(e){return(NULL)}
          |  )
          |}
          |
          |coef_table = lapply(geno_terms, geno_to_coef) %>%
          |  bind_rows()
          |readr::write_csv(coef_table,file = paste0(partitions_model,file_name,".csv"))
          |
          |""".stripMargin
      )
      R.quit()
      iterator
    }
    model.count()
  }
}