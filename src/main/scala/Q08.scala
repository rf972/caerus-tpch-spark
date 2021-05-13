package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.defineMacros._
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 8
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q08 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._

    if (schemaProvider.pushUDF) {
      schemaProvider.spark.registerMacro("decreaseUDF", spark.udm((x: Double, y: Double) => {
        x * (1 - y)
      }))
    }
    val getYear = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val isBrazil = udf { (x: String, y: Double) => if (x == "BRAZIL") y else 0 }

    val fregion = region.filter($"r_name" === "AMERICA")
    val forder = order.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
    val fpart = part.filter($"p_type" === "ECONOMY ANODIZED STEEL")

    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val line = if (schemaProvider.pushUDF) {
      lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
        callUDF("decreaseUDF", $"l_extendedprice", $"l_discount").as("volume"))
        .join(fpart, $"l_partkey" === fpart("p_partkey"))
        .join(nat, $"l_suppkey" === nat("s_suppkey"))
    } else {
      lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("volume")).
        join(fpart, $"l_partkey" === fpart("p_partkey"))
        .join(nat, $"l_suppkey" === nat("s_suppkey"))
    }

    nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
      .select($"n_nationkey")
      .join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"c_custkey")
      .join(forder, $"c_custkey" === forder("o_custkey"))
      .select($"o_orderkey", $"o_orderdate")
      .join(line, $"o_orderkey" === line("l_orderkey"))
      .select(getYear($"o_orderdate").as("o_year"), $"volume",
        isBrazil($"n_name", $"volume").as("case_volume"))
      .groupBy($"o_year")
      .agg(sum($"case_volume") / sum("volume"))
      .sort($"o_year")
  }

}
