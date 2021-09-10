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

object TpchTableReaderS3 {

  private def sparkSession(hostName: String) = SparkSession.builder
      .master("local[2]")
      .appName("TpchProvider")
      .config("spark.datasource.pushdown.endpoint", s"""http://$hostName""")
      .config("spark.datasource.pushdown.accessKey", System.getProperty("user.name"))
      .config("spark.datasource.pushdown.secretKey", "admin123")
      .getOrCreate()

  def enableOptions(name: String, spark: SparkSession): Unit = {
    name match {
      case "minio" => {
        /* Minio does not support many options. */
        spark.conf.set("spark.datasource.pushdown.DisableGroupbyPush", "")
        spark.conf.set("spark.datasource.pushdown.DisableSupportsIsNull", "")
        spark.conf.set("spark.datasource.pushdown.DisabledCasts", "NUMERIC")
        /* By default we split into N partitions of max size
         * conf.filesMaxPartitionBytes.
         * However, minio does not support ranges yet, so until it does
         * we can only use partitions == 1.
         */
        spark.conf.set("spark.datasource.pushdown.partitions", "1")
      }
      case _ =>
    }
  }
  def readTable[T: WeakTypeTag]
               (name: String, params: TpchReaderParams)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    val spark = sparkSession(params.config.s3HostName)
    // Allow better progress tracking when we are with normal verbosity or higher.
    if (params.config.verbose || params.config.normal) {
      spark.conf.set("spark.datasource.pushdown.EnableProgress", "")
    }
    enableOptions(params.options, spark)
    if (params.pushOpt.isPushdownEnabled()) {
      val df = spark.read
        .format("com.github.datasource")
        .option("format", "csv")
        .option("partitions", params.partitions)
        .schema(schema)
        .load(params.inputDir + "/" +  name + s".${params.config.format}" + "/")
        df
    } else {
      val df = spark.read
        .format("com.github.datasource")
        .option("format", "csv")
        .option("partitions", params.partitions)
        .option((if (params.pushOpt.enableFilter) "Enable" else "Disable") + "FilterPush", "")
        .option((if (params.pushOpt.enableProject) "Enable" else "Disable") + "ProjectPush", "")
        .option((if (params.pushOpt.enableAggregate) "Enable" else "Disable") + "AggregatePush", "")
        .schema(schema)
        .load(params.inputDir + "/" + name + s".${params.config.format}" + "/")
        df
    }
  }
}
