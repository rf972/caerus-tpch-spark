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

import org.tpch.filetype._
import org.tpch.s3options._

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
  def init(fileType: FileType) : Unit = {
    val hadoopConfig: Configuration = sparkSession.sparkContext.hadoopConfiguration
    //hadoopConfig.set("fs.dikehdfs.impl", classOf[org.apache.hadoop.hdfs.web.DikeHdfsFileSystem].getName)
    //println("set fs.dikehdfs.impl")
    if (fileType == CSVHdfs) {
      // Force use of V2 data source.
      sparkSession.conf.set("spark.sql.sources.useV1SourceList", "")
    }
    if (fileType == TBLHdfsDs || fileType == CSVHdfsDs) {
      //sparkSession.conf.set("spark.datasource.pushdown.endpoint", "dikehdfs")
    }
    resetStats()
  }
  def readTable[T: WeakTypeTag]
               (name: String, inputDir: String,
                s3Options: TpchS3Options, partitions: Int, fileType: FileType)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    
    if (!FileType.isDataSource(fileType)) {
        //(fileType == CSVHdfs || fileType == CSVWebHdfs)
      sparkSession.read
        .format("csv")
        .schema(schema)
        .load(inputDir + "/" +  name + (if (FileType.isTbl(fileType)) ".tbl" else ".csv"))
    } else {
      sparkSession.read
        .format("com.github.datasource")
        .option("format", (if (FileType.isTblToDs(fileType)) "tbl" else "csv"))
        .schema(schema)
        .option((if (FileType.isDisableProcessor(fileType)) "Disable" else "Enable") + "Processor", "")
        .option((if (s3Options.enableFilter) "Enable" else "Disable") + "FilterPush", "")
        .option((if (s3Options.enableProject) "Enable" else "Disable") + "ProjectPush", "")
        .option((if (s3Options.enableAggregate) "Enable" else "Disable") + "AggregatePush", "")
        .option("partitions", partitions)
        .load(inputDir + "/" +  name + (if (FileType.isTbl(fileType)) ".tbl" else ".csv"))
    }
  }
}
