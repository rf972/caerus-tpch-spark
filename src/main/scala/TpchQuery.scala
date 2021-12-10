package main.scala

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.net.URI
import java.text.NumberFormat.getIntegerInstance

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

import com.github.datasource.hdfs.HdfsStore
// import com.github.datasource.s3.S3StoreCSV

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetRecordReader
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs._
import org.slf4j.LoggerFactory
import org.tpch.config.Config
import org.tpch.jdbc.TpchJdbc
import org.tpch.pushdown.options.TpchPushdownOptions
import org.tpch.tablereader._
import org.tpch.tablereader.hdfs._
import scopt.OParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class TpchTestResult(test: String,
                          seconds: Double,
                          bytesTransferred: Double,
                          status: Boolean = true,
                          var cpuTime: Double = 0,
                          cores: Integer = 0) {
  override def toString(): String =  {
    val formatter = java.text.NumberFormat.getIntegerInstance
    val bytes = formatter.format(bytesTransferred)
    (f"${test}%4s,   ${cores}%2d, ${seconds}%10.3f," +
     f" ${bytesTransferred}%20.0f," +
     f" ${cpuTime}%10.2f," +
     f" ${if (status) { "OK"} else { "FAILED" }}%12s")
  }
}

object TpchTestResult {
  def empty: Unit = new TpchTestResult("", 0, 0)
}
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

  private val logger = LoggerFactory.getLogger(getClass)
  private var sparkConf = new SparkConf().setAppName("Simple Application")
                                .set("spark.scheduler.mode", "FAIR")
  private var sparkContext = new SparkContext(sparkConf)
  sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")

  def newContext: Unit = sparkContext = new SparkContext(sparkConf)
  // sparkConf.set("spark.scheduler.mode", "FAIR")

  /** Writes the dataframe to disk.
   *
   *  @param df - the dataframe to output
   *  @param outputDir - path to use in output
   *  @param className - the name of the test class
   *  @param config - The configuration of the tst.
   *  @return String - Path to output results.
   */
  def outputDF(df: DataFrame, outputDir: String, className: String,
               config: Config): Unit = {

    /* if (!config.checkResults) {
      /* When we are  not checking results, we want to
       * execute as quickly as possible.
       */
      df.count()
    } else */ if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else {
      val castColumns = (df.schema.fields map { x =>
        if (x.dataType == DoubleType) {
          format_number(bround(col(x.name), 3), 2)
        } else {
          col(x.name)
        }
      }).toArray
      val columns = (df.schema.fields map { x => col(x.name) }).toArray

      if (!className.contains("17") && config.checkResults) {
        df
          .select(castColumns:_*)
          .repartition(1)
          .orderBy((df.columns.toSeq map { x => col(x) }).toArray:_*)
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

  /** Fetches the directory name to be used for output of the resultant
   *  dataframe.
   *
   *  @param config - The configuration of the tst.
   *  @return String - Path to output results.
   */
  def getOutputDir(config: Config): String = {
    var outputDir =
      if (config.outputLocation == "file")
        s"file:///build/tpch-results/latest/${config.mode.toString}"
      else
        s"hdfs://${config.server}:9000/tpch-results/latest/${config.mode.toString}"
    outputDir += s"${config.datasource}-${config.protocol}-${config.format}"
    if (config.threadNum != 0) outputDir += s"-thread${config.threadNum}"
    if (config.partitions != 0) {
      outputDir += "-partitions-1"
    }
    if (config.filePart) {
      outputDir += "-filePart"
    }
    if (config.options != "") {
      outputDir += s"-${config.options}"
    }
    if (config.pushRule) {
      outputDir += "-Rule"
    }
    if (config.pushFilter && config.pushProject) {
      outputDir += "-PushdownFilterProject"
    } else if (config.pushdown) {
      outputDir += "-PushdownAgg"
    } else if (config.pushFilter) {
      outputDir += "-PushdownFilter"
    } else if (config.pushProject) {
      outputDir += "-PushdownProject"
    }
    outputDir += "-W" + config.workers
    outputDir
  }
  def runQuery(schemaProvider: TpchSchemaProvider,
               queryNum: Int,
               config: Config): TpchTestResult = {

    val outputDir: String = getOutputDir(config)
    val query = Class.forName(f"main.scala.Q${queryNum}%02d")
                     .newInstance.asInstanceOf[TpchQuery]
    val df = {
      logger.info(f"Pushdown test start: Q${queryNum}%02d")
      if (config.sql)
        schemaProvider.runQuery(queryNum)
      else
        query.execute(sparkContext, schemaProvider)
    }
    val t0 = System.nanoTime()
    val startBytes = getBytes(config)
    var status = true
    if (config.explain) {
      df.explain(true)
      //println("Num Partitions: " + df.rdd.partitions.length)
    }
    try {
      outputDF(df, outputDir, query.getName(), config)
    } catch {
      case t : Throwable => println("Exception occurred running outputDF")
      println(org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(t))
      status = false
    }
    logger.info(f"Pushdown test end Q${queryNum}%02d")
    var t1 = System.nanoTime()
    val seconds = (t1 - t0) / 1000000000.0f // second
    val statsType = config.protocol
    val bytes = {
      if (config.bytesServer != "") {
        calcBytes(config, startBytes)
      } else if (statsType.contains("hdfs")) {
        TpchTableReaderHdfs.getBytesRead(statsType)
      } else if (statsType == "file") {
        TpchSchemaProvider.transferBytes
      } /* else if (statsType == "s3") {
        TpchTestResult(query.getName(), seconds, S3StoreCSV.getTransferLength,
                       status)
      } */ else {
        0
      }
    }
    val result = TpchTestResult(query.getName(), seconds, bytes,
                                status, cores=config.workers)
    // S3StoreCSV.resetTransferLength
    println("Query Time " + seconds + " bytes " + bytes)
    result
  }
  def getBytes(config: Config) : Array[Long] = {
    import scala.sys.process._
    val bytes = new ListBuffer[Long]()
    if (config.bytesServer != "") {
      val server = config.bytesServer.split(",")(0)
      var cmd = "ssh ${server} "
      if (server == "local") {
        cmd = ""
      }
      val adapters = config.bytesServer.split(",")(1).split(":")
      println("server: " + server + " adapters " + adapters.mkString(", "))
      for (a <- adapters) {
        val output = s"${cmd}sudo ifconfig ${a}".!!
        val bytesOutput = {
          var currentBytes = ""
          for (l <- output.split("\n")) {
            if (l.contains("TX packets")) {
              currentBytes = l.split(" ").filter(_.nonEmpty)(4)
            }
          }
          currentBytes
        }
        bytes += bytesOutput.toLong
      }
    }
    bytes.toArray
  }
  def calcBytes(config: Config, startBytes : Array[Long]) : Long = {
    import scala.sys.process._
    var totalBytes: Long = 0
    val bytes = new ListBuffer[Long]()
    if (config.bytesServer != "") {
      val server = config.bytesServer.split(",")(0)
      val adapters = config.bytesServer.split(",")(1).split(":")
      val endBytes = getBytes(config)
      println(startBytes)
      println(endBytes)
      for (i <- 0 until endBytes.length) {
        totalBytes += endBytes(i) - startBytes(i)
      }
    }
    totalBytes
  }
  def executeQueries(schemaProvider: TpchSchemaProvider,
                     queryNum: Int,
                     config: Config): TpchTestResult = {

    val spark = SparkSession
      .builder()
      .getOrCreate()
    var result = {
      if (config.metrics == "stage") {
        val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
        stageMetrics.begin()
        val result = runQuery(schemaProvider, queryNum, config)
        stageMetrics.end()
        val nameTempView = "PerfStageMetrics"
        stageMetrics.createStageMetricsDF(nameTempView)
        val aggregateDF = stageMetrics.aggregateStageMetrics(nameTempView)
        val times = aggregateDF.select("executorRunTime", "executorCpuTime").take(1)(0)
        result.cpuTime = times.getAs[Long](0)

        /* stageMetrics.printReport()
        stageMetrics.printAccumulables()
        // save session metrics data
        val stageDF = stageMetrics.createStageMetricsDF("PerfStageMetrics")
        stageMetrics.saveData(stageDF.orderBy("jobId", "stageId"), "/build/tpch-results/stagemetrics_test1")
        val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
        stageMetrics.saveData(aggregatedDF, "/build/tpch-results/stagemetrics_report_test2") */
        result
      } else if (config.metrics == "task") {
        val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark, true)
        taskMetrics.begin()
        val result = runQuery(schemaProvider, queryNum, config)
        taskMetrics.end()
        val nameTempView = "PerfTaskMetrics"
        taskMetrics.createTaskMetricsDF(nameTempView)
        val aggregateDF = taskMetrics.aggregateTaskMetrics(nameTempView)
        val times = aggregateDF.select("executorRunTime", "executorCpuTime").take(1)(0)
        //println("executorRunTime " + times.getAs[Long](1).asInstanceOf[Double] +
        //        "executorCPUTime " + times.getAs[Long](0))
        // result.utilization = (times.getAs[Long](1).asInstanceOf[Double] / times.getAs[Long](0)) * 100
        result.cpuTime = times.getAs[Long](0)
        /* taskMetrics.printReport()
        taskMetrics.printAccumulables()
        val taskDf = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")
        taskMetrics.saveData(taskDf.orderBy("jobId", "stageId", "index"), "/build/tpch-results/taskmetrics_test3")
        */
        result
      } else {
        runQuery(schemaProvider, queryNum, config)
      }
    }
    result
  }
  /** Validates and processes args related to the type of test.
   *
   *  @param config - The program config to be validated.
   *  @return Boolean - true if valid, false if invalid config.
   */
  def processTestMode(config: Config): Boolean = {
    if (config.mode == "initCsv" || config.mode == "initParquet") {
      config.format = "tbl" // we are converting tbl to csv
    }
    config.datasource match {
      case "ndp" if (config.protocol.contains("s3") &&
                     config.format == "csv") => true
      case "spark" if (config.protocol == "file" &&
                     config.format == "csv") => true
      case "spark" if (config.protocol == "file" &&
                     config.format == "parquet") => true
      case "spark" if (config.protocol == "file" &&
                     config.format == "tbl") => true
      case "spark" if (config.protocol == "hdfs" &&
                     config.format == "csv") => true
      case "spark" if (config.protocol == "hdfs" &&
                     config.format == "tbl") => true
      case "spark" if (config.protocol == "hdfs" &&
                       config.format == "parquet") => true
      case "ndp" if (config.protocol == "hdfs" &&
                     config.format == "csv") => true
      case "ndp" if (config.protocol == "hdfs" &&
                     config.format == "tbl")  => true
      case "ndp" if (config.protocol == "hdfs" &&
                     config.format == "parquet") => true
      case "ndp" if (config.protocol == "webhdfs" &&
                     config.format == "csv") => true
      case "ndp" if (config.protocol == "webhdfs" &&
                     config.format == "tbl") => true
      case "ndp" if (config.protocol == "ndphdfs" &&
                     config.format == "csv") => true
      case "ndp" if (config.protocol == "ndphdfs" &&
                     config.format == "tbl") => true
      case "ndp" if (config.protocol == "ndphdfs" &&
                     config.format == "parquet") => true
      case "spark" if (config.protocol == "webhdfs" &&
                     config.format == "tbl") => true
      case "spark" if (config.protocol == "webhdfs" &&
                     config.format == "tbl") => true
      case "ndp" if (config.protocol == "s3" && config.format == "tbl" &&
                     config.filePart == true) => true
      case "ndp" if (config.protocol == "s3" && config.format == "tbl") => true
      case ds if config.protocol == "jdbc" => true
      case ds if config.mode == "initCsv" || config.mode == "initParquet" ||
                 config.mode == "initJdbc" || config.mode == "clearAll" => {
        true
      }
      case test => println(s"Unsupported test configuration: test: ${test} " +
                           s"format: ${config.format} protocol: ${config.protocol}" +
                           s" datasource: ${config.datasource}")
                   false
    }
  }
  /** Parse the test numbers argument and generate a list of integers
   *  with the test numbers to run.
   *  @param config - The configuration of the tst.
   *  @return Boolean - true on success, false, validation failed.
   */
  def processTestNumbers(config: Config) : Boolean = {
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
          val test = r.toInt
          config.testList += test
        }
      }
    }
    for (t <- config.testList) {
      if (t < 1 || t > 122) {
        println(s"test numbers must be 1..22.  ${t} is not a valid test")
        return false
      }
    }
    true
  }
  /** Parse the pushdown related arguments and generate
   *  the config.pushdownOptions.
   *
   *  @param config - The configuration of the tst.
   *  @return Unit
   */
  def processPushdownOptions(config: Config) : Unit = {

    if (config.pushUDF) {
      config.pushdownOptions = TpchPushdownOptions(true, true, true, true, config.explain)
    } else if (config.pushdown) {
      config.pushdownOptions = TpchPushdownOptions(true, true, true, false, config.explain)
    } else {
      config.pushdownOptions = TpchPushdownOptions(config.pushFilter,
                                                   config.pushProject,
                                                   config.pushAggregate,
                                                   config.pushUDF,
                                                   config.explain)
    }
  }
  /** Handle the options as they relate to other params.
   *  @param config The configuration to process.
   */
  private def processOptions(config: Config): Boolean = {
    config.options match {
      // With minio we do not support multiple partitions.
      case "minio" => config.partitions = 1
      config.s3HostName = "minioserver:9000"
      case _ =>
    }
    true
  }
  private val usageInfo = """The program has two main modes, one where we are using
  *) --mode init or --mode initJdbc.  In this case
     the test is initializing a database for example to
     convert the database to .csv or to a JDBC format.
  *) otherwise the program will be running the tpch benchmark
     and the parameters below determine the test to run, and
     with which configuration to use such as:
     --format (csv | tbl | parquet)
     --protocol (file | s3 | hdfs | webhdfs | ndphdfs | jdbc)
     --datasource (spark | ndp)
     -t (test number)"""
  /** Parses all the test arguments and forms the
   *  config object, which is used to convey the test parameters.
   *
   *  @param args - The test arguments from the user.
   *  @return Config - The object representing all program params.
   */
  def parseArgs(args: Array[String]): Config = {

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("TCPH Benchmark"),
        head("tpch-test", "0.1"),
        note(usageInfo + sys.props("line.separator")),
        opt[String]('t', "test")
          .action((x, c) => c.copy(testNumbers = x))
          .valueName("<test number>")
          .text("test numbers. e.g. 1,2-5,6,7,9-11,16-22"),
         opt[Int]('p', "partitions")
          .action((x, c) => c.copy(partitions = x.toInt))
          .valueName("<number of partitions>")
          .text("partitions to use"),
         opt[Int]('w', "workers")
          .action((x, c) => c.copy(workers = x.toInt))
          .valueName("<number of spark workers>")
          .text("workers being used"),
         opt[String]('z', "options")
          .action((x, c) => c.copy(options = x))
          .valueName("<options>")
          .text("optional config parameters (e.g. minio)")
          .validate( options =>
            options match {
              case "minio" => success
              case _ => failure("mode must be minio")
            }),
        opt[String]('s', "server")
          .action((x, c) => c.copy(server = x))
          .valueName("<server ip>")
          .text("ip for NDP storage server."),
        opt[String]('s', "compression")
          .action((x, c) => c.copy(compression = x))
          .valueName("<compression alg>")
          .text("compression algorithm (lz4, None)"),
        opt[String]('l', "compLevel")
          .action((x, c) => c.copy(compLevel = x))
          .valueName("<compression level>")
          .text("degree to which we compress(-200-20)"),
        opt[String]("bytesServer")
          .abbr("bs")
          .action((x, c) => c.copy(bytesServer = x))
          .text("get bytes xferred from server/adpater(s) (server,adapter1:adapter2)"),
        opt[String]("mode")
          .action((x, c) => c.copy(mode = x))
          .valueName("<test mode>")
          .text("test mode (initCsv, initParquet, initJdbc, clearAll)")
          .validate( mode =>
            mode match {
              case "initCsv" => success
              case "initParquet" => success
              case "clearAll" => success
              case "parallel" => success
              case _ => failure("mode must be initCsv, initParquet, clearAll, parallel")
            }),
        opt[String]('f', "format")
          .action((x, c) => c.copy(format = x))
          .valueName("<file format>")
          .text("file format to use (csv, tbl")
          .validate(f =>
            f match {
              case "tbl" => success
              case "csv"  => success
              case "parquet"  => success
              case format => failure(s"ERROR: format: ${format} not suported")
            }),
        opt[String]("outputFormat")
          .action((x, c) => c.copy(outputFormat = x))
          .valueName("<outputformat>")
          .text("format for ndp to return (csv, parquet)")
          .validate(f =>
            f match {
              case "csv"  => success
              case "parquet"  => success
              case "binary"  => success
              case format => failure(s"ERROR: format: ${format} not suported")
            }),
        opt[String]("datasource")
          .abbr("ds")
          .valueName("<datasource>")
          .action((x, c) => c.copy(datasource = x))
          .text("datasource to use (spark, ndp)")
          .validate(datasource =>
            datasource match {
              case "spark" => success
              case "ndp"  => success
              case ds => failure(s"datasource: ${ds} not suported")
            }),
        opt[String]('r', "protocol")
          .action((x, c) => c.copy(protocol = x))
          .valueName("<protocol>")
          .text("server protocol to use (file, s3, hdfs, webhdfs, ndphdfs, jdbc)")
          .validate(protocol =>
            protocol match {
              case "file" => success
              case "jdbc" => success
              case "s3" => success
              case "hdfs" => success
              case "webhdfs" => success
              case "ndphdfs" => success
              case protocol => failure(s"ERROR: protocol: ${protocol} not suported")
            }),
        opt[String]("path")
          .action((x, c) => c.copy(path = x))
          .valueName("<path>")
          .text("root path of files"),
        opt[String]('m', "metrics")
          .action((x, c) => c.copy(metrics = x))
          .valueName("<stage or task>")
          .text("Generate metrics for stage or task"),
        opt[Unit]("filePart")
          .action((x, c) => c.copy(filePart = true))
          .text("Use file based partitioning."),
        opt[Unit]("pushRule")
          .action((x, c) => c.copy(pushRule = true))
          .text("Use a pushdown rule."),
        opt[Unit]("pushdown")
          .action((x, c) => c.copy(pushdown = true))
          .text("Enable all pushdowns (filter, project, aggregate), default is disabled."),
        opt[Unit]("pushFilter")
          .action((x, c) => c.copy(pushFilter = true))
          .text("Enable pushdown of filter, default is enabled."),
        opt[Unit]("pushProject")
          .action((x, c) => c.copy(pushProject = true))
          .text("Enable pushdown of project, default is enabled."),
        opt[Unit]("pushAggregate")
          .action((x, c) => c.copy(pushAggregate = true))
          .text("Enable pushdown of aggregate, default is enabled."),
        opt[Unit]("disablePushFilter")
          .action((x, c) => c.copy(pushFilter = false))
          .text("Disable pushdown of filter, default is enabled."),
        opt[Unit]("disablePushAggregate")
          .action((x, c) => c.copy(pushAggregate = false))
          .text("Disable pushdown of aggregate, default is enabled."),
        opt[Unit]("pushUDF")
          .action((x, c) => c.copy(pushUDF = true))
          .text("Enable pushdown of User Defined Functions, default is disabled."),
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
        opt[Unit]("normal")
          .action((x, c) => c.copy(normal = true))
          .text("Normal log output (INFO log level)."),
        opt[Unit]("debugData")
          .action((x, c) => c.copy(debugData = true))
          .text("For debugging, copy the data output to file."),
        opt[Int]('r', "repeat")
          .action((x, c) => c.copy(repeat = x.toInt))
          .valueName("<repeat count>")
          .text("Number of times to repeat test"),
        opt[Unit]("sql")
          .abbr("sql")
          .action((x, c) => c.copy(sql = true))
          .text("use sql query string"),
        opt[String]("outputLocation")
          .abbr("ol")
          .action((x, c) => c.copy(outputLocation = x))
          .text("Location to output results (file, hdfs)"),
        opt[String]("fileInfo")
          .abbr("fi")
          .action((x, c) => c.copy(fileInfo = x))
          .text("show info on this file (parquet)"),
        help("help").text("prints this usage text"),
        checkConfig(
          c => {
            var status: Boolean = processTestMode(c)
            status &= processOptions(c)
            status &= processTestNumbers(c)
            processPushdownOptions(c)
            if (!status) {
              failure("Validation failed.")
            } else {
              if ((c.mode == "") && (c.testNumbers == "") && (c.fileInfo == "")) {
                failure("must select either --mode or --test")
              } else {
                success
            }}})
      )
    }
    // OParser.parse returns Option[Config]
    val config = OParser.parse(parser1, args, Config())

    config match {
        case Some(config) => config
        case _ =>
          // arguments are bad, error message will have been displayed
          System.exit(1)
          new Config
    }
  }

  /** Shows the results from a ListBuffer[TpchTestResult]
   *
   * @param results - The test results.
   * @return Unit
   */
  private def showResults(results: ListBuffer[TpchTestResult]) : Unit = {
    println("Test Results")
    println("Test    Cores  Time (sec)             Bytes      CPU Time      Status")
    println("---------------------------------------------------------------------")
    for (r <- results) {
      println(r)
    }
  }
  /** Fetch the path to be used to input data.
   *
   *  @param config - The test configuration.
   *  @return Unit
   */
  def inputPath(config: Config) = {
      config.datasource match {
        case ds if (config.protocol == "hdfs" && config.format == "parquet" &&
                    config.path != "") => s"hdfs://${config.server}:9000/${config.path}/"
        case ds if (ds == "spark" && config.format == "tbl" &&
                    config.protocol == "file") => "file:///tpch-data/tpch-test"
        case ds if (ds == "spark" && config.format == "csv" &&
                    config.protocol == "file") => "file:///tpch-data/tpch-test-csv"
        case ds if (ds == "spark" && config.format == "parquet" &&
                    config.protocol == "file") => "file:///tpch-data/tpch-test-parquet"
        case ds if (ds == "ndp" && config.format == "tbl" &&
                    config.protocol == "hdfs") => s"hdfs://${config.server}/tpch-test/"
        case ds if (ds == "ndp" && config.format == "csv" &&
                    config.protocol == "hdfs") => s"hdfs://${config.server}/tpch-test-csv/"
        case ds if (ds == "ndp" && config.format == "parquet" &&
                    config.protocol == "hdfs") => s"hdfs://${config.server}/tpch-test-parquet/"
        case ds if (ds == "ndp" && config.format == "tbl" &&
                    config.protocol == "webhdfs") => s"webhdfs://${config.server}/tpch-test/"
        case ds if (ds == "ndp" && config.format == "csv" &&
                    config.protocol == "webhdfs") => s"webhdfs://${config.server}/tpch-test-csv/"

        case ds if (ds == "spark" && config.format == "tbl" &&
                    config.protocol == "hdfs") => s"hdfs://${config.server}:9000/tpch-test/"
        case ds if (ds == "spark" && config.format == "csv" &&
                    config.protocol == "hdfs") => s"hdfs://${config.server}:9000/tpch-test-csv/"
        case ds if (ds == "spark" && config.format == "tbl" &&
                    config.protocol == "webhdfs") => s"webhdfs://${config.server}:9870/tpch-test/"
        case ds if (ds == "spark" && config.format == "csv" &&
                    config.protocol == "webhdfs") => s"webhdfs://${config.server}:9870/tpch-test-csv/"
        case ds if (ds == "spark" && config.format == "parquet" &&
                    config.protocol == "hdfs") => s"hdfs://${config.server}:9000/tpch-test-parquet/"

        case ds if (ds == "ndp" && config.format == "tbl" &&
                    config.protocol == "ndphdfs") => s"ndphdfs://${config.server}/tpch-test/"
        case ds if (ds == "ndp" && config.format == "csv" &&
                    config.protocol == "ndphdfs") => s"ndphdfs://${config.server}/tpch-test-csv/"
        case ds if (ds == "ndp" && config.format == "parquet" &&
                    config.protocol == "ndphdfs") => s"ndphdfs://${config.server}/tpch-test-parquet/"

        case ds if (ds == "ndp" && config.format == "tbl" &&
                    config.filePart) => "s3a://tpch-test-part"
        case ds if (ds == "ndp" && config.format == "csv" &&
                    config.filePart) => "s3a://tpch-test-csv-part"
        case ds if (ds == "ndp" && config.format == "tbl" &&
                    config.protocol.contains("s3")) => "s3a://tpch-test"
        case ds if (ds == "ndp" && config.format == "csv" &&
                    config.protocol.contains("s3")) => "s3a://tpch-test-csv"

        case x if config.protocol == "jdbc" => "file://tpch-data/tpch-test-jdbc"
      }
  }

  /** Sets the file to be used to output when we are debugging data.
   *
   * @param config - test configuration.
   * @param test - name of the test
   * @return Unit
   */
  def setDebugFile(config: Config, test: String) : Unit = {
    if (config.debugData) {
      val outputDir = "/build/tpch-results/data/"
      val directory = new File(outputDir)
      if (! directory.exists()) {
        directory.mkdir()
        println("creating data dir")
      }
      // RowIterator.setDebugFile(outputDir + config.format + "-" + test)
    }
  }

  /** Runs the benchmark, and displayes the results.
   *
   * @param config - test configuration.
   * @return Unit
   */
  def benchmark(config: Config, testList: ArrayBuffer[Integer]): Unit = {
    var totalMs: Long = 0
    var results = new ListBuffer[TpchTestResult]

    if (config.protocol.contains("hdfs")) {
      TpchTableReaderHdfs.init(TpchReaderParams(config))
    } /* else {
     S3StoreCSV.resetTransferLength
    } */
    println(s"InputPath: ${inputPath(config)}")
    config.inputDir = inputPath(config)
    for (r <- 0 to config.repeat) {
      for (i <- testList) {
        config.currentTest = f"TPC-H Test Q${i}%02d"
        val schemaProvider = new TpchSchemaProvider(sparkContext,
                                                    TpchReaderParams(config))
        val output = new ListBuffer[(String, Float)]
        setDebugFile(config, i.toString)
        HdfsStore.sendClearAll(s"ndphdfs://dikehdfs/${config.path}/lineitem.${config.format}")
        results += executeQueries(schemaProvider, i, config)
        showResults(results)
      }
    }
    if (config.repeat > 1) {
      val averageSec = (totalMs / 1000.0) / config.repeat
      println("Average Seconds per Test: " + averageSec)
    }
  }

  val localFsPath = "/tpch-data/"
  val initTblPath = s"file://${localFsPath}tpch-test"
  val initTblPartPath = s"file://${localFsPath}tpch-test-part"

  /** Fetches the path to use for writing.
   * @param config The configuration of test.
   * @return String of the path.
   */
  def getOutputPath(config: Config): String = {

    config.protocol match {
      case "hdfs" => s"hdfs://${config.server}:9000/"
      case "file" => "file:///tpch-data/"
      case _ => ""
    }
  }
  /** Initializes a new database using csv.
   *
   * @param config - test configuration.
   * @return Unit
   */
  def initCsv(config: Config): Unit = {
    val csvPath = "tpch-test-csv/"

    val outputPath = getOutputPath(config)
    config.inputDir = initTblPath
    val schemaProvider = new TpchSchemaProvider(sparkContext,
                                                TpchReaderParams(config))
    for ((name, df) <- schemaProvider.dfMap) {
      val outputFilePath = outputPath + csvPath + name + ".csv"
      df.repartition(1)
        .write
        .option("header", true)
        .option("partitions", "1")
        .format("csv")
        .save(outputFilePath)

      val fs = FileSystem.get(URI.create(outputPath), sparkContext.hadoopConfiguration)
      fs.delete(new Path(outputFilePath + "/_SUCCESS"), true)
      println("Finished writing " + name + ".csv")
    }
    println("Finished converting *.tbl to *.csv")
  }
  /** Initializes a new database using parquet.
   *
   * @param config - test configuration.
   * @return Unit
   */
  def initParquet(config: Config): Unit = {
    val parquetPath = "tpch-test-parquet/"

    val outputPath = getOutputPath(config)
    config.inputDir = initTblPath
    val schemaProvider = new TpchSchemaProvider(sparkContext,
                                                TpchReaderParams(config))
    // sparkContext.hadoopConfiguration.setInt("parquet.block.size", 32 * 1024 * 1024)
    for ((name, df) <- schemaProvider.dfMap) {
      val outputFilePath = outputPath + parquetPath + name + ".parquet"
      df.repartition(1)
        .write
        .option("header", true)
        .option("partitions", "1")
        .format("parquet")
        .save(outputFilePath)

      val fs = FileSystem.get(URI.create(outputPath), sparkContext.hadoopConfiguration)
      fs.delete(new Path(outputFilePath + "/_SUCCESS"), true)
      println("Finished writing " + name + ".parquet")
    }
    println("Finished converting *.tbl to *.parquet")
  }

  /** Initializes a new database with partitions using csv.
   *
   * @param config - test configuration.
   * @return Unit
   */
  def initCsvPart(config: Config): Unit = {
    val csvPartPath = "tpch-test-csv-part/"
    val outputPath = getOutputPath(config)
    config.inputDir = initTblPath
    val schemaProvider = new TpchSchemaProvider(sparkContext, TpchReaderParams(config))
    for ((name, df) <- schemaProvider.dfMap) {
      val inputFs = FileSystem.get(sparkContext.hadoopConfiguration)
      val fs = FileSystem.get(URI.create(outputPath), sparkContext.hadoopConfiguration)
      val outputFilePath = outputPath + csvPartPath + name + ".csv"

      val inputPath = new Path(initTblPartPath + s"/${name}.tbl.*")
      val status = inputFs.globStatus(inputPath)
      val partitions = if (status.length == 0) 1 else status.length
      println(s"input: ${initTblPartPath}/${name}.tbl partitions: ${partitions}")
      df.repartition(partitions)
        .write
        .option("header", true)
        .format("csv")
        .save(outputFilePath)
      println("Finished writing " + name + ".csv")
      fs.delete(new Path(outputFilePath + "/_SUCCESS"), true)
    }
    println("Finished converting *.tbl to *.csv partitions")
  }
  /** Initializes a JDBC H2 database with content from a tpch database.
   *  This reads in a database (for exmaple from .tbl files)
   *  and then writes it out into the JDBC database.
   *
   * @param config - The configuration of the test.
   * @return Unit
   */
  def initJdbc(config: Config): Unit = {
    val h2Database = "file:///tpch-data/tpch-jdbc/tpch-h2-database"
    config.inputDir = initTblPath
    val schemaProvider = new TpchSchemaProvider(sparkContext,
                                                TpchReaderParams(config))
    TpchJdbc.setupDatabase()
    for ((name, df) <- schemaProvider.dfMap) {
        TpchJdbc.writeDf(df, name, h2Database)
        //TpchJdbc.readDf(name).show()
    }
    println("Finished converting *.tbl to jdbc:h2 format")
  }

  /** Show information about this file.
   *
   * @param config - The configuration of the test.
   * @return Unit
   */
  def fileInfo(config: Config): Unit = {

    var configuration = new Configuration

    var options: ParquetReadOptions = HadoopReadOptions
      .builder(configuration)
      .build();
    val reader = new ParquetFileReader(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(config.fileInfo),
                                                                         configuration), options)
    println(reader.getFileMetaData().toString)
    val parquetBlocks = reader.getFooter.getBlocks
    // Generate one partition per row Group.
    for (i <- 0 to parquetBlocks.size - 1) {
      val parquetBlock = parquetBlocks.get(i)
      println(s"Row Group ${i}")
      println(s"  Row Count ${parquetBlock.getRowCount()}")
      println(s"  Bytes ${parquetBlock.getTotalByteSize()}")
      println(s"  Compressed Bytes ${parquetBlock.getCompressedSize()}")
    }
  }
  /** This is the main entry point of the program,
   *  see above parseArgs for more usage information.
   *
   * @param config - The configuration of the test.
   * @return Unit
   */
  def main(args: Array[String]): Unit = {

    val config = parseArgs(args)
    println("args: " + args.mkString(" "))
    println("pushdown: " + config.pushdown)
    println("datasource: " + config.datasource)
    println("protocol: " + config.protocol)
    println("format: " + config.format)
    println("output format: " + config.outputFormat)
    println("pushdown options: " + config.pushdownOptions)
    println("workers: " + config.workers)
    println("mode: " + config.mode)
    println("start: " + config.start)
    println("end: " + config.end)
    println(s"inputPath: ${inputPath(config)}")

    if (config.verbose) {
      sparkContext.setLogLevel("TRACE")
    } else if (config.quiet) {
      sparkContext.setLogLevel("WARN")
    } else if (config.normal) {
      sparkContext.setLogLevel("INFO")
    }
    config.mode match {
      case "initCsv" => {
        if (config.filePart) {
          initCsvPart(config)
        } else {
          initCsv(config)
        }
      }
      case "clearAll" =>
        println(s"InputPath: ${inputPath(config)}")
        config.inputDir = inputPath(config)
        val file = config.inputDir + "/lineitem." + config.format
        logger.info(s"Send clearAll to $file")
        HdfsStore.sendClearAll(file)
      case "initParquet" => initParquet(config)
      case "initJdbc" => initJdbc(config)
      case _ if (config.fileInfo != "") => fileInfo(config)
      case _ if (config.mode == "parallel") => {
        var i = 0
        val threads = config.testList.map(t => {
          println(s"Parallel mode starting ${t}")
          i += 1
          new BenchRunner(config.copy(threadNum=i), ArrayBuffer[Integer](t))
        })
        threads.foreach(r => r.start())
        threads.foreach(r => r.join())
        // newContext
      }
      case _  => benchmark(config, config.testList)
    }
  }
}

class BenchRunner(c: Config, testList: ArrayBuffer[Integer]) extends Thread {
  override def run: Unit = {
    TpchQuery.benchmark(c, testList)
  }
}
