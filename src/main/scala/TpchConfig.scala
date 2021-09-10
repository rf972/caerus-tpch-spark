package org.tpch.config

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

import com.github.datasource.parse._
import org.apache.hadoop.fs._
import org.tpch.config
import org.tpch.jdbc.TpchJdbc
import org.tpch.pushdown.options.TpchPushdownOptions

/* This is the object containing the configuration for the test.
 */
case class Config(
    var start: Int = 0,
    testNumbers: String = "",
    var end: Int = -1,
    var currentTest: String = "",
    var testList: ArrayBuffer[Integer] = ArrayBuffer.empty[Integer],
    repeat: Int = 0,
    var partitions: Int = 0,
    options: String = "",
    var server: String = "dikehdfs",
    var compression: String = "None",
    var compLevel: String = "-100",
    workers: Int = 1,
    checkResults: Boolean = false,
    mode: String = "",  // The mode of the test.
    var format: String = "csv",
    var outputFormat: String = "csv",
    datasource: String = "spark",
    protocol: String = "file",
    var s3HostName: String = "dikehdfs:9858",
    var inputDir: String = "",
    filePart: Boolean = false,
    fileInfo: String = "",
    var pushdownOptions: TpchPushdownOptions =
        new TpchPushdownOptions(false, false, false, false, false),
    pushdown: Boolean = false,
    pushUDF: Boolean = false,
    pushFilter: Boolean = false,
    pushProject: Boolean = false,
    pushAggregate: Boolean = false,
    pushRule: Boolean = false,
    debugData: Boolean = false,
    verbose: Boolean = false,
    explain: Boolean = false,
    quiet: Boolean = false,
    normal: Boolean = false,
    metrics: String = "task",
    kwargs: Map[String, String] = Map())
