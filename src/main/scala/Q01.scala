package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.defineMacros._
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q01 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._

    if (schemaProvider.pushUDF) {
      schemaProvider.spark.registerMacro("decreaseUDM", spark.udm((x: Double, y: Double) => {
       x * (1 - y)
      }))
      schemaProvider.spark.registerMacro("increaseUDM", spark.udm((x: Double, y: Double) => {
       x * (1 + y)
      }))
      schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
        .groupBy($"l_returnflag", $"l_linestatus")
        .agg(sum($"l_quantity"), sum($"l_extendedprice"),
          sum(callUDF("decreaseUDM",$"l_extendedprice", $"l_discount")),
          sum(callUDF("increaseUDM",callUDF("decreaseUDM", $"l_extendedprice", $"l_discount"), $"l_tax")),
          avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
        .sort($"l_returnflag", $"l_linestatus")
    } else {
      val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
      val increase = udf { (x: Double, y: Double) => x * (1 + y) }

      schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
        .groupBy($"l_returnflag", $"l_linestatus")
        .agg(sum($"l_quantity"), sum($"l_extendedprice"),
          sum(decrease($"l_extendedprice", $"l_discount")),
          sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
          avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
        .sort($"l_returnflag", $"l_linestatus")
    }
  }
}
