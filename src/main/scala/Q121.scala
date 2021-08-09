package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions._
/**
 * TPC-H Query 21 modified for query pushdown.
 *
 */
class Q121 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._

    val plineitem = lineitem.filter($"l_receiptdate" > "1992-01-30").select($"l_suppkey", $"l_orderkey", $"l_receiptdate", $"l_commitdate")

    val line1 = plineitem // .groupBy($"l_orderkey")
      .agg(min($"l_suppkey"), max($"l_orderkey"), max($"l_suppkey"))
    line1
  }

}
