package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.defineMacros._
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 16
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q16 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._
    if (schemaProvider.pushUDF) {
      schemaProvider.spark.registerMacro("polishedUDF", spark.udm((x: String) => {
        x.startsWith("MEDIUM POLISHED")
      }))

      schemaProvider.spark.registerMacro("complainsUDF", spark.udm((x: String) => {
        x.contains("Customer") && x.contains("Complaints")
      }))
    }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith("MEDIUM POLISHED") }
    val numbers = udf { (x: Int) => x.toString().matches("49|14|23|45|19|3|36|9") }

    val fparts =
      if (schemaProvider.pushUDF) {
        part.filter(($"p_brand" !== "Brand#45") && !callUDF("polishedUDF", $"p_type") &&
                    numbers($"p_size"))
            .select($"p_partkey", $"p_brand", $"p_type", $"p_size")
      } else {
        part.filter(($"p_brand" !== "Brand#45") && !polished($"p_type") &&
                    numbers($"p_size"))
            .select($"p_partkey", $"p_brand", $"p_type", $"p_size")
      }

    val suppFilt =
      if (schemaProvider.pushUDF) {
        supplier.filter(!callUDF("complainsUDF", $"s_comment"))
      } else {
        supplier.filter(!complains($"s_comment"))
      }
    suppFilt
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")
  }

}
