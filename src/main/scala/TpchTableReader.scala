package org.tpch.tablereader

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, Row}
import scala.reflect.runtime.universe._
import org.tpch.filetype._
import org.tpch.s3options._
import org.tpch.jdbc.TpchJdbc
import main.scala.TpchSchemaProvider
import com.github.datasource.parse._

object TpchTableReaderS3 {
  
  private val s3IpAddr = "minioserver"
  private val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("TpchProvider")
      .config("spark.datasource.pushdown.endpoint", s"""http://$s3IpAddr:9000""")
      .config("spark.datasource.pushdown.accessKey", "admin")
      .config("spark.datasource.pushdown.secretKey", "admin123")
      .getOrCreate()

  def readTable[T: WeakTypeTag]
               (name: String, inputDir: String,
                s3Options: TpchS3Options, partitions: Int)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    if (s3Options.isPushdownEnabled()) {
      val df = sparkSession.read
        .format("com.github.datasource")
        .option("format", "csv")
        .option("partitions", partitions)
        .schema(schema)
        .load(inputDir + "/" +  name)
        df
    } else {
      val df = sparkSession.read
        .format("com.github.datasource")
        .option("format", "csv")
        .option("partitions", partitions)
        .option((if (s3Options.enableFilter) "Enable" else "Disable") + "FilterPush", "")
        .option((if (s3Options.enableProject) "Enable" else "Disable") + "ProjectPush", "")
        .option((if (s3Options.enableAggregate) "Enable" else "Disable") + "AggregatePush", "")
        .schema(schema)
        .load(inputDir + "/" +  name)
        df
    }
  }
}
object TpchTableReaderFile {
  
  private val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("TpchProvider")
      // Force use of V2 data source.
      .config("spark.sql.sources.useV1SourceList", "")
      .getOrCreate()
  // this is used to implicitly convert an RDD to a DataFrame.
  val sparkContext = sparkSession.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
  import sqlContext.implicits._

  def readTable[T: WeakTypeTag]
               (name: String, inputDir: String, fileType: FileType)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    
    if (fileType == CSVFile) {
      sparkSession.read
        .format("csv")
        .schema(schema)
        .load(inputDir + "/" +  name + ".csv")
    } else {
      /* This will create a data frame out of a list of Row objects. 
       */
      sqlContext.createDataFrame(sparkContext.textFile(inputDir + "/" + name + ".tbl*").map(l => {
          TpchSchemaProvider.transferBytes += l.size
          RowIterator.parseLine(l, schema, '|')
          }), StructType(schema))
    }
  }
}
