package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, Row}
import org.tpch.tablereader._
import org.tpch.jdbc.TpchJdbc
import org.tpch.tablereader.hdfs._
import org.tpch.pushdown.options.TpchPushdownOptions

// TPC-H table schemas
case class Customer(
  c_custkey: Long,
  c_name: String,
  c_address: String,
  c_nationkey: Long,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Long,
  l_partkey: Long,
  l_suppkey: Long,
  l_linenumber: Long,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Long,
  n_name: String,
  n_regionkey: Long,
  n_comment: String)

case class Order(
  o_orderkey: Long,
  o_custkey: Long,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Long,
  o_comment: String)

case class Part(
  p_partkey: Long,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Long,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Long,
  ps_suppkey: Long,
  ps_availqty: Long,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Long,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Long,
  s_name: String,
  s_address: String,
  s_nationkey: Long,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)


class TpchSchemaProvider(sc: SparkContext, params: TpchReaderParams) {
  val dfMap = 
    if (params.config.protocol == "s3") 
      Map(
          "customer" -> TpchTableReaderS3.readTable[Customer]("customer", params),
          "lineitem" -> TpchTableReaderS3.readTable[Lineitem]("lineitem", params),
          "nation" -> TpchTableReaderS3.readTable[Nation]("nation", params),
          "region" -> TpchTableReaderS3.readTable[Region]("region", params),
          "orders" -> TpchTableReaderS3.readTable[Order]("orders", params),
          "part" -> TpchTableReaderS3.readTable[Part]("part", params),
          "partsupp" -> TpchTableReaderS3.readTable[Partsupp]("partsupp", params),
          "supplier" -> TpchTableReaderS3.readTable[Supplier]("supplier", params) )
    else if (params.config.protocol == "jdbc")
      Map(
          "customer" -> TpchJdbc.readTable[Customer]("customer", params),
          "lineitem" -> TpchJdbc.readTable[Lineitem]("lineitem", params),
          "nation" -> TpchJdbc.readTable[Nation]("nation", params),
          "region" -> TpchJdbc.readTable[Region]("region", params),
          "orders" -> TpchJdbc.readTable[Order]("orders", params),
          "part" -> TpchJdbc.readTable[Part]("part", params),
          "partsupp" -> TpchJdbc.readTable[Partsupp]("partsupp", params),
          "supplier" -> TpchJdbc.readTable[Supplier]("supplier", params) )
    else if (params.config.protocol.contains("hdfs"))
      Map(
          "customer" -> TpchTableReaderHdfs.readTable[Customer]("customer", params),
          "lineitem" -> TpchTableReaderHdfs.readTable[Lineitem]("lineitem", params),
          "nation" -> TpchTableReaderHdfs.readTable[Nation]("nation", params),
          "region" -> TpchTableReaderHdfs.readTable[Region]("region", params),
          "orders" -> TpchTableReaderHdfs.readTable[Order]("orders", params),
          "part" -> TpchTableReaderHdfs.readTable[Part]("part", params),
          "partsupp" -> TpchTableReaderHdfs.readTable[Partsupp]("partsupp", params),
          "supplier" -> TpchTableReaderHdfs.readTable[Supplier]("supplier", params) )
    else
      /* CSVFile, TBLFile, TBLHdfs */
      Map(
          "customer" -> TpchTableReaderFile.readTable[Customer]("customer", params),
          "lineitem" -> TpchTableReaderFile.readTable[Lineitem]("lineitem", params),
          "nation" -> TpchTableReaderFile.readTable[Nation]("nation", params),
          "region" -> TpchTableReaderFile.readTable[Region]("region", params),
          "orders" -> TpchTableReaderFile.readTable[Order]("orders", params),
          "part" -> TpchTableReaderFile.readTable[Part]("part", params),
          "partsupp" -> TpchTableReaderFile.readTable[Partsupp]("partsupp", params),
          "supplier" -> TpchTableReaderFile.readTable[Supplier]("supplier", params) )

  // for implicits
  val customer = dfMap.get("customer").get
  val lineitem = dfMap.get("lineitem").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("orders").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get
  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
  val pushUDF = params.pushOpt.enableUDF
  val spark = SparkSession.builder
      .appName("TpchProvider")
      .getOrCreate()
}

object TpchSchemaProvider {

  var transferBytes: Long = 0
}