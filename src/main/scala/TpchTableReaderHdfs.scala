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

  def getStats(): org.apache.hadoop.fs.FileSystem.Statistics = {
    val conf = new Configuration()
    val hdfsCoreSitePath = new Path("/home/rob/config/core-site.xml")
    conf.set("dfs.datanode.drop.cache.behind.reads", "true")
    conf.set("dfs.client.cache.readahead", "0")
    conf.addResource(hdfsCoreSitePath)
    val fs = FileSystem.get(URI.create("hdfs://dikehdfs:9000/"), conf)
    val stats = FileSystem.getStatistics
    stats("hdfs")
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
    if (fileType == V2CsvHdfs) {
      // Force use of V2 data source.
      sparkSession.conf.set("spark.sql.sources.useV1SourceList", "")
    }
    resetStats()
  }
  def readTable[T: WeakTypeTag]
               (name: String, inputDir: String,
                s3Options: TpchS3Options, partitions: Int)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    
    val df = sparkSession.read
        .format("csv")
        .schema(schema)
        .load(inputDir + "/" +  name + ".csv")
        // df.show()
    df
  }
}