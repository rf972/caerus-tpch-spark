package org.tpch.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties


import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe._
import org.tpch.tablereader._
import org.tpch.pushdown.options.TpchPushdownOptions

/** Allows for using JDBC with the tpch test.
 *
 */
object TpchJdbc {
 
  private val clName = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"
  private val dbPath = "file://tpch-data/tpch-jdbc/tpch-h2-database"
  private val url = s"jdbc:h2:${dbPath};user=testUser;password=testPass"
  val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("example")
      .config("spark.sql.catalog.h2", clName)
      .config("spark.sql.catalog.h2.url", url)
      .config("spark.sql.catalog.h2.driver", "org.h2.Driver")
      .getOrCreate()

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }
  def readTable[T: WeakTypeTag]
               (name: String, params: TpchReaderParams)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
      readDf(name)
  }
  def readDf(name: String) : DataFrame = {
    import sparkSession.implicits._ 
    sparkSession.table("h2.tpch." + name)
  }
  def writeDf(df: DataFrame, name: String, outputPath: String) : Unit = {

    withConnection { conn =>
      for (row <- df.rdd.collect) {
        var rowString: String = ""
        var colIndex = 0
        row.toSeq.foreach( col => {
          if (colIndex != 0) rowString += ","
          col match {
            case s: String => rowString += s"""'${col}'"""
            case _ => rowString += col.toString
          }
          colIndex += 1
        })
        conn.prepareStatement(s"""INSERT INTO "tpch"."${name}" VALUES (${rowString})""").executeUpdate()
      }
    }
  }

  /** Creates a new database with the schema for the tpch test.
   */
  def setupDatabase(): Unit = {
    withConnection { conn =>
      conn.prepareStatement("CREATE SCHEMA \"tpch\"").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"empty_table\" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)")
        .executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"customer\" (c_custkey INTEGER, c_name TEXT(100), c_address TEXT(100)," +
        " c_nationkey INTEGER, c_phone TEXT(32), c_acctbal NUMERIC(20,2), c_mktsegment TEXT, c_comment TEXT)"
        ).executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"nation\" (n_nationkey INTEGER, n_name TEXT, n_regionkey INTEGER," + 
        "  n_comment TEXT)").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"orders\" (o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus TEXT," +
        " o_totalprice NUMERIC(20,2), o_orderdate TEXT, o_orderpriority TEXT, o_clerk TEXT," +
        " o_shippriority INTEGER, o_comment TEXT)").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"part\" (p_partkey INTEGER, p_name TEXT, p_mfgr TEXT, p_brand TEXT," +
        " p_type TEXT, p_size INTEGER, p_container TEXT, p_retailprice NUMERIC(20,2), p_comment TEXT)"
        ).executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"partsupp\" (ps_partkey INTEGER, ps_suppkey INTEGER," +
        "  ps_availqty INTEGER, ps_supplycost NUMERIC(20,2), ps_comment TEXT)"
        ).executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"region\" (r_regionkey INTEGER, r_name TEXT," + 
        " r_comment TEXT)").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"supplier\" (s_suppkey INTEGER, s_name TEXT, s_address TEXT," +
        "  s_nationkey INTEGER, s_phone TEXT, s_acctbal NUMERIC(20,2), s_comment TEXT)"
        ).executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"tpch\".\"lineitem\" (l_orderkey INTEGER,l_partkey INTEGER,l_suppkey INTEGER," +
        " l_linenumber INTEGER,l_quantity NUMERIC(20,2),l_extendedprice NUMERIC(20,2)," +
        " l_discount NUMERIC(20,2),l_tax NUMERIC(20,2),l_returnflag TEXT,l_linestatus TEXT," +
        " l_shipdate TEXT,l_commitdate TEXT,l_receiptdate TEXT,l_shipinstruct TEXT,l_shipmode TEXT," +
        " l_comment TEXT)"
        ).executeUpdate()
    }
  }
}
