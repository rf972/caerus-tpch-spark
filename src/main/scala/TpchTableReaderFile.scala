package org.tpch.tablereader

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, Row}
import scala.reflect.runtime.universe._
import org.tpch.pushdown.options.TpchPushdownOptions
import org.tpch.jdbc.TpchJdbc
import main.scala.TpchSchemaProvider
import com.github.datasource.parse._

/** Represents a tableReader, which can read in a dataframe
 * from a standard file using the spark datasource.
 */
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
               (name: String, params: TpchReaderParams)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

    if (params.config.format == "csv") {
      sparkSession.read
        .format(params.config.format)
        .schema(schema)
        .option("header", (if (params.config.format == "tbl") "false" else "true"))
        .load(params.inputDir + "/" +  name + ".csv")
    } else if (params.config.format == "parquet") {
      sparkSession.read
        .format(params.config.format)
        .load(params.inputDir + "/" +  name + ".parquet")
    } else {
      /* This will create a data frame out of a list of Row objects.
       */
      sqlContext.createDataFrame(sparkContext.textFile(
        params.inputDir + "/" + name + ".tbl*").map(l => {
          TpchSchemaProvider.transferBytes += l.size
          RowIterator.parseLine(l, schema, '|')
          }), StructType(schema))
    }
  }
}
