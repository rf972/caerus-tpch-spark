package org.tpch.tablereader.hdfs

import scala.reflect.runtime.universe._
import scala.collection.JavaConversions.mapAsScalaMap
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, Row}
import org.tpch.tablereader._
import org.tpch.pushdown.options.TpchPushdownOptions

/** Represents a tableReader, which can read in a dataframe
 * from a hdfs datasource.
 */
object TpchTableReaderHdfs {
  
  private val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("TpchProvider")
      .getOrCreate()

  def getStats(name: String): org.apache.hadoop.fs.FileSystem.Statistics = {
    val stats = FileSystem.getStatistics
    stats(name)
  }
  def resetStats(): Unit = {
    FileSystem.clearStatistics()
  }
  def showStats() : Unit = {
    val conf = new Configuration()
    val hdfsCoreSitePath = new Path("/home/rob/config/core-site.xml")
    conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    conf.set("dfs.client.cache.readahead", "0")
    conf.addResource(hdfsCoreSitePath)
    val fs = FileSystem.get(URI.create("hdfs://dikehdfs:9000/"), conf)
    var stats = FileSystem.getStatistics
    println(stats)
    val files = fs.listStatus(new Path("hdfs://dikehdfs:9000/tpch-test"))
    for (f <- files) {
      println(f.getPath().getName())
    }
  }
  def init(params: TpchReaderParams) : Unit = {
    val hadoopConfig: Configuration = sparkSession.sparkContext.hadoopConfiguration
    if (params.config.format == "csv") {
      // Force use of V2 data source.
      println("Using V2 Spark CSV Data Source.")
      sparkSession.conf.set("spark.sql.sources.useV1SourceList", "")
      //sparkSession.conf.set("spark.sql.files.maxPartitionBytes", "1000000000000")
    }
    resetStats()
  }
  def readTable[T: WeakTypeTag]
               (name: String, params: TpchReaderParams)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

    if (params.config.datasource != "ndp") {
      sparkSession.read
        .format(params.config.format)
        .schema(schema)
        .load(params.inputDir + "/" + name + "." + params.config.format)
    } else if (params.config.format == "parquet") {
      sparkSession.read
        .format("com.github.datasource")
        .option("format", "parquet")
        .option("outputFormat", params.config.outputFormat)
        .schema(schema)
        .option((if (params.pushOpt.enableFilter) "Enable" else "Disable") + "FilterPush", "")
        .option((if (params.pushOpt.enableProject) "Enable" else "Disable") + "ProjectPush", "")
        .option((if (params.pushOpt.enableAggregate) "Enable" else "Disable") + "AggregatePush", "")
        .option("partitions", params.partitions)
        .load(params.inputDir + "/" + name + params.config.format)
    } else {
      sparkSession.read
        .format("com.github.datasource")
        .option("format", params.config.format)
        .option("outputFormat", params.config.outputFormat)
        .option("header", (if (params.config.format == "tbl") "false" else "true"))
        .schema(schema)
        .option((if (params.pushOpt.enableFilter) "Enable" else "Disable") + "FilterPush", "")
        .option((if (params.pushOpt.enableProject) "Enable" else "Disable") + "ProjectPush", "")
        .option((if (params.pushOpt.enableAggregate) "Enable" else "Disable") + "AggregatePush", "")
        .option("partitions", params.partitions)
        .load(params.inputDir + "/" + name + "." + params.config.format)
    }
  }
}
