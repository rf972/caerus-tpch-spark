package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.text.NumberFormat.getIntegerInstance
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import org.apache.spark.sql.catalyst.ScalaReflection
import scopt.OParser
import org.apache.hadoop.fs._
import com.github.s3datasource.store._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.tpch.tablereader._
import org.tpch.tablereader.hdfs._
import org.tpch.filetype._
import org.tpch.s3options._
import org.tpch.jdbc.TpchJdbc

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

  def outputDF(df: DataFrame, outputDir: String, className: String,
               config: Config): Unit = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else {
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      
      val castColumns = (df.schema.fields map { x =>
        if (x.dataType == DoubleType) {
          format_number(bround(col(x.name), 3), 2)
          //col(x.name).cast(DecimalType(38,2))
        } else {
          col(x.name)
        }      
      }).toArray

      if (!className.contains("17") && config.checkResults) {       
        df.sort((df.columns.toSeq map { x => col(x) }).toArray:_*)
            .select(castColumns:_*)
            .repartition(1)
            .write.mode("overwrite")
            .format("csv")
            .option("header", "true")
            .option("partitions", "1")
            .save(outputDir + "/" + className)
      } else {
        df.repartition(1)
          .write.mode("overwrite")
          .format("csv")
          .option("header", "true")
          .option("partitions", "1")
          .save(outputDir + "/" + className)
      }
    }
  }

  def executeQueries(schemaProvider: TpchSchemaProvider, 
                     queryNum: Int,
                     config: Config): ListBuffer[(String, Float)] = {

    val OUTPUT_DIR: String = { 
      var outputDir = "file:///build/tpch-results/latest/" + config.test
      if (config.partitions != 0) {
        outputDir += "-partitions-1"
      }
      if (config.s3Filter && config.s3Project) {
        outputDir += "-PushdownFilterProject"
      } else if (config.s3Select) {
        outputDir += "-PushdownAgg"
      } else if (config.s3Filter) {
        outputDir += "-PushdownFilter"
      } else if (config.s3Project) {
        outputDir += "-PushdownProject"
      }
      outputDir
    }
    val results = new ListBuffer[(String, Float)]

    val t0 = System.nanoTime()

    val query = Class.forName(f"main.scala.Q${queryNum}%02d")
                      .newInstance.asInstanceOf[TpchQuery]
    val df = query.execute(sparkContext, schemaProvider)
    if (config.explain) {
      df.explain(true)
      //println("Num Partitions: " + df.rdd.partitions.length)
    }
    outputDF(df, OUTPUT_DIR, query.getName(), config)

    val t1 = System.nanoTime()

    val elapsed = (t1 - t0) / 1000000000.0f // second
    results += new Tuple2(query.getName(), elapsed)
    return results
  }

  case class Config(
    var start: Int = 0,
    testNumbers: String = "",
    var end: Int = -1,
    var testList: ArrayBuffer[Integer] = ArrayBuffer.empty[Integer],
    repeat: Int = 0,
    partitions: Int = 0,
    checkResults: Boolean = false,
    var fileType: FileType = CSVS3,
    test: String = "",
    var init: Boolean = false,
    var s3Options: TpchS3Options = new TpchS3Options(false, false, false, false),
    s3Select: Boolean = false,
    s3Filter: Boolean = false,
    s3Project: Boolean = false,
    s3Aggregate: Boolean = false,
    verbose: Boolean = false,
    explain: Boolean = false,
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
        opt[String]('n', "num")
          .action((x, c) => c.copy(testNumbers = x))
          .text("test numbers"),
         opt[Int]('p', "partitions")
          .action((x, c) => c.copy(partitions = x.toInt))
          .text("partitions to use"),
        opt[String]("test")
          .required
          .action((x, c) => c.copy(test = x))
          .text("test to run (csvS3, csvFile, tblS3, tblPartS3, tblFile, jdbc, init," +
                "tblHdfs, v1CsvHdfs, v2CsvHdfs"),
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
        opt[Unit]("check")
          .action((x, c) => c.copy(checkResults = true))
          .text("Enable checking of results."),
        opt[Unit]("verbose")
          .action((x, c) => c.copy(verbose = true))
          .text("Enable verbose Spark output (TRACE log level )."),
        opt[Unit]("explain")
          .action((x, c) => c.copy(explain = true))
          .text("Run explain on the df prior to query."),
        opt[Unit]('q', "quiet")
          .action((x, c) => c.copy(quiet = true))
          .text("Limit output (WARN log level)."),
        opt[Int]('r', "repeat")
          .action((x, c) => c.copy(repeat = x.toInt))
          .text("Number of times to repeat test"),
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
            case "tblHdfs" => config.fileType = TBLHdfs
            case "v1CsvHdfs" => config.fileType = V1CsvHdfs
            case "v2CsvHdfs" => config.fileType = V2CsvHdfs
            case "tblS3" => config.fileType = TBLS3
            case "tblPartS3" => config.fileType = TBLS3
            case "jdbc" => config.fileType = JDBC
            case test if test == "init" || test == "initJdbc" => {
              config.init = true
              config.fileType = TBLFile
            }
          }
          if (config.testNumbers != "") {
            val ranges = config.testNumbers.split(",")
            for (r <- ranges) {
              if (r.contains("-")) {
                val numbers = r.split("-")
                if (numbers.length == 2) {
                  for (i <- numbers(0).toInt to numbers(1).toInt) {
                    config.testList += i
                  }
                }
              } else {
                config.testList += r.toInt
              }
            }
          }
          if (config.s3Select) {
            config.s3Options = TpchS3Options(true, true, true, config.explain)
          } else {
            config.s3Options = TpchS3Options(config.s3Filter,
                                             config.s3Project,
                                             config.s3Aggregate,
                                             config.explain)
          }
          config
        case _ =>
          // arguments are bad, error message will have been displayed
          System.exit(1)
          new Config
    }
  }

  case class TpchTestResult (
    test: Integer,
    seconds: Double,
    bytesTransferred: Double)

  private def showResults(results: ListBuffer[TpchTestResult]) : Unit = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    println("Test Results")
    println("Test    Time (sec)             Bytes")
    println("------------------------------------")
    for (r <- results) {
      val bytes = formatter.format(r.bytesTransferred)
      println(f"${r.test}%4d, ${r.seconds}%10.3f," +
              f" ${r.bytesTransferred}%20.0f")
    }
  }
  private val tpchPathMap = Map("jdbc" -> "file://tpch-data/tpch-test-jdbc",
                                "tblFile" -> "file:///tpch-data/tpch-test",
                                "s3" -> "s3a://tpch-test",
                                "csvFile" -> "file:///tpch-data/tpch-test-csv",
                                "tblPartS3" -> "s3a://tpch-test-part",
                                "tblHdfs" -> "hdfs://dikehdfs:9000/tpch-test/",
                                "csvHdfs" -> "hdfs://dikehdfs:9000/tpch-test-csv/")
  def inputPath(config: Config) = {
      config.test match { 
        case x if x == "csvS3" || x == "tblS3" => tpchPathMap("s3")
        case x@"jdbc"    => tpchPathMap(x)
        case x@"tblFile" => tpchPathMap(x)
        case x@"csvFile" => tpchPathMap(x)
        case x@"tblHdfs" => tpchPathMap(x)
        case x if x == "v1CsvHdfs" || x == "v2CsvHdfs" => tpchPathMap("csvHdfs")
        case x if x == "tblPartS3" => tpchPathMap(x)
      }
  }
  def benchmark(config: Config): Unit = {
    var totalMs: Long = 0
    var results = new ListBuffer[TpchTestResult]
   
   if (config.fileType == TBLHdfs ||
       config.fileType == V1CsvHdfs ||
       config.fileType == V2CsvHdfs) {
     TpchTableReaderHdfs.init(config.fileType)
   } else {
    S3StoreCSV.resetTransferLength
   }
    val schemaProvider = new TpchSchemaProvider(sparkContext, inputPath(config), 
                                                config.s3Options, config.fileType,
                                                config.partitions)
    for (r <- 0 to config.repeat) {
      for (i <- config.testList) {
        val output = new ListBuffer[(String, Float)]
        println("Starting Q" + i)
        val start = System.currentTimeMillis()
        output ++= executeQueries(schemaProvider, i, config)
        val end = System.currentTimeMillis()
        val ms = (end - start)
        if (r != 0) totalMs += ms
        val seconds = ms / 1000.0
        if (config.fileType == TBLHdfs ||
            config.fileType == V1CsvHdfs ||
            config.fileType == V2CsvHdfs) {
          results += TpchTestResult(i, seconds, TpchTableReaderHdfs.getStats.getBytesRead)
        } else if (config.fileType == TBLFile) {
          results += TpchTestResult(i, seconds, TpchSchemaProvider.transferBytes)
        } else {
          results += TpchTestResult(i, seconds, S3StoreCSV.getTransferLength)
        }
        S3StoreCSV.resetTransferLength
        println("Query Time " + seconds)
        val outFile = new File("TIMES" + i + ".txt")
        val bw = new BufferedWriter(new FileWriter(outFile, true))

        output.foreach {
          case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
        }

        bw.close()
        showResults(results)
      }
  }
  if (config.repeat > 1) {
    val averageSec = (totalMs / 1000.0) / config.repeat
    println("Average Seconds per Test: " + averageSec)
  }
}

  val initTblPath = "file:///tpch-data/tpch-test"
  val h2Database = "file:///tpch-data/tpch-jdbc/tpch-h2-database"
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
  def initJdbc(config: Config): Unit = {
    val schemaProvider = new TpchSchemaProvider(sparkContext, initTblPath, 
                                                config.s3Options, config.fileType,
                                                config.partitions)
    TpchJdbc.setupDatabase()
    for ((name, df) <- schemaProvider.dfMap) {
        TpchJdbc.writeDf(df, name, h2Database)
        //TpchJdbc.readDf(name).show()
    }
    println("Finished converting *.tbl to jdbc:h2 format")
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
    config.test match {
      case "init" => init(config)
      case "initJdbc" => initJdbc(config)
      case _ => benchmark(config)
    }
  }
}
