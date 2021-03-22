package org.tpch.tablereader

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, Row}
import scala.reflect.runtime.universe._
import org.tpch.filetype._
import org.tpch.pushdown.options.TpchPushdownOptions
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
                pushOpt: TpchPushdownOptions, partitions: Int)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    if (pushOpt.isPushdownEnabled()) {
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
        .option((if (pushOpt.enableFilter) "Enable" else "Disable") + "FilterPush", "")
        .option((if (pushOpt.enableProject) "Enable" else "Disable") + "ProjectPush", "")
        .option((if (pushOpt.enableAggregate) "Enable" else "Disable") + "AggregatePush", "")
        .schema(schema)
        .load(inputDir + "/" +  name)
        df
    }
  }
}