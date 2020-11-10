package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import org.apache.spark.sql.catalyst.ScalaReflection
import scopt.OParser
import org.apache.hadoop.fs._
import org.tpch.tablereader._

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    val items = className.split("\\.")
    val last = items(items.length-1)
    println(last)
    last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  private val sparkConf = new SparkConf().setAppName("Simple Application")
  private val sparkContext = new SparkContext(sparkConf)

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(schemaProvider: TpchSchemaProvider, 
                     queryNum: Int): ListBuffer[(String, Float)] = {

    val OUTPUT_DIR: String = "file:///build/tpch-test-output"

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"main.scala.Q${queryNo}%02d")
                       .newInstance.asInstanceOf[TpchQuery]

      outputDF(query.execute(sparkContext, schemaProvider), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)

    }

    return results
  }

  case class Config(
    start: Int = 0,
    var end: Int = -1,
    partitions: Int = 0,
    var fileType: FileType = CSVS3,
    test: String = "csvS3",
    var init: Boolean = false,
    var s3Options: TpchS3Options = new TpchS3Options(false, false, false),
    s3Select: Boolean = false,
    s3Filter: Boolean = false,
    s3Project: Boolean = false,
    s3Aggregate: Boolean = false,
    verbose: Boolean = false,
    quiet: Boolean = false,
    kwargs: Map[String, String] = Map())
  
  val maxTests = 22
  def parseArgs(args: Array[String]): Config = {
  
    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("TCPH Benchmark"),
        head("tpch-test", "0.1"),
        opt[Int]('n', "num")
          .action((x, c) => c.copy(start = x.toInt))
          .text("start test number"),
         opt[Int]('p', "partitions")
          .action((x, c) => c.copy(partitions = x.toInt))
          .text("partitions to use"),
        opt[String]("test")
          .required
          .action((x, c) => c.copy(test = x))
          .text("test to run (csvS3, csvFile, tblS3, tblPartS3, tblFile, init"),
        opt[Unit]("s3Select")
          .action((x, c) => c.copy(s3Select = true))
          .text("Enable s3Select pushdown (filter, project), default is disabled."),
        opt[Unit]("s3Filter")
          .action((x, c) => c.copy(s3Filter = true))
          .text("Enable s3Select pushdown of filter, default is disabled."),
        opt[Unit]("s3Project")
          .action((x, c) => c.copy(s3Project = true))
          .text("Enable s3Select pushdown of project, default is disabled."),
        opt[Unit]("s3Aggregate")
          .action((x, c) => c.copy(s3Aggregate = true))
          .text("Enable s3Select pushdown of aggregate, default is disabled."),
        opt[Unit]("verbose")
          .action((x, c) => c.copy(verbose = true))
          .text("Enable verbose Spark output (TRACE log level )."),
        opt[Unit]('q', "quiet")
          .action((x, c) => c.copy(quiet = true))
          .text("Limit output (WARN log level)."),
        help("help").text("prints this usage text"),
      )
    }
    // OParser.parse returns Option[Config]
    val config = OParser.parse(parser1, args, Config())
            
    config match {
        case Some(config) =>
          config.test match {
            case "csvS3" => config.fileType = CSVS3
            case "csvFile" => config.fileType = CSVFile
            case "tblFile" => config.fileType = TBLFile
            case "tblS3" => config.fileType = TBLS3
            case "tblPartS3" => config.fileType = TBLS3
            case "init" => {
              config.init = true
              config.fileType = TBLFile
            }
          }
          config.end = config.start
          if (config.s3Select) {
            config.s3Options = TpchS3Options(true, true, true)
          } else {
            config.s3Options = TpchS3Options(config.s3Filter,
                                             config.s3Project,
                                             config.s3Aggregate)
          }
          config
        case _ =>
          // arguments are bad, error message will have been displayed
          System.exit(1)
          new Config
    }
  }
  val inputTblPath = "file:///tpch-data"
  def benchmark(config: Config): Unit = {
    val inputPath = 
      config.test match { case x if x == "csvS3" || x == "tblS3" => "s3a://tpch-test" 
                          case x if x == "tblPartS3" => "s3a://tpch-test-part" 
                          case _ => inputTblPath }

    val schemaProvider = new TpchSchemaProvider(sparkContext, inputPath, 
                                                config.s3Options, config.fileType,
                                                config.partitions)
    for (i <- config.start to config.end) {
      val output = new ListBuffer[(String, Float)]

      val start = System.currentTimeMillis()
      output ++= executeQueries(schemaProvider, i)
      val end = System.currentTimeMillis()

      val seconds = (end - start) / 1000.0 //BigDecimal((end-start)/1000.0).setScale(3, BigDecimal.RoundingMode.HALF_DOWN).toFloat
      println("Query Time " + seconds)
      val outFile = new File("TIMES" + i + ".txt")
      val bw = new BufferedWriter(new FileWriter(outFile, true))

      output.foreach {
        case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
      }

      bw.close()
    }
  }

  val initTblPath = "file:///build/tpch-data"
  def init(config: Config): Unit = {
    val schemaProvider = new TpchSchemaProvider(sparkContext, initTblPath, 
                                                config.s3Options, config.fileType,
                                                config.partitions)

    for ((name, df) <- schemaProvider.dfMap) {
      val outputFolder = "/build/tpch-data/" + name + "raw"
      df.repartition(1)
        .write
        .option("header", true)
        .option("partitions", "1")
        .format("csv")
        .save(outputFolder)

      val fs = FileSystem.get(sparkContext.hadoopConfiguration)
      val file = fs.globStatus(new Path(outputFolder + "/part-0000*"))(0).getPath().getName()
      println(outputFolder + "/" + file + "->" + "/build/tpch-data/" + name + ".csv")
      fs.rename(new Path(outputFolder + "/" + file), new Path("/build/tpch-data/" + name + ".csv"))
      println("Finished writing " + name + ".csv")
    }
    println("Finished converting *.tbl to *.csv")
  }

  def main(args: Array[String]): Unit = {

    var s3Select = false;
    val config = parseArgs(args) 
    println("s3Select: " + config.s3Select)
    println("s3Options: " + config.s3Options)
    println("test: " + config.test)
    println("fileType: " + config.fileType)
    println("start: " + config.start)
    println("end: " + config.end)
    if (config.quiet) {
      sparkContext.setLogLevel("WARN")
    } else if (config.verbose) {
      sparkContext.setLogLevel("TRACE")
    }
    if (config.test == "init") {
      init(config)
    } else {
      benchmark(config)
    }
  }
}
