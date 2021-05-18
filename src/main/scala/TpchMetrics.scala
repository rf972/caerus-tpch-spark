package org.tpch.spark.metrics

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class Metrics() {
  // Create spark session.
  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("MetricsBuilder")
    .getOrCreate()

  def getMetrics(appID: String, historyPath: String): (List[Long], Long) = {
    val metricFile = historyPath + Path.SEPARATOR + appID
    val metricDF = spark.read.json(metricFile)
    metricDF.createOrReplaceTempView("metrics")
    val executionTimes: List[Long] = spark
      .sql(
        """SELECT -MAX(`Submission Time`)+MAX(`Completion Time`) FROM metrics WHERE (Event=='SparkListenerJobStart' OR Event=='SparkListenerJobEnd') AND `Job ID` > 0 GROUP BY `Job ID` ORDER BY `Job ID` """.stripMargin)
      .collect()
      .map(_(0))
      .toList
      .asInstanceOf[List[Long]]

    println("Execution Times: %s".format(executionTimes.mkString(",")))

    val stageInfoDF = metricDF.filter("Event='SparkListenerStageCompleted'").select("`Stage Info`.*")
    stageInfoDF.createOrReplaceTempView("stageInfo")
    val stageAccumDF = spark
      .sql("select `Stage ID`, temp.col.* from stageInfo lateral view explode(Accumulables) temp")
    val readSizes: List[Long] = stageAccumDF
      .collect
      .filter(x => x(5).asInstanceOf[String] == "internal.metrics.input.bytesRead")
      .map(x => x(6).asInstanceOf[String].toLong)
      .toList

    println("Read Sizes: %s".format(readSizes.mkString(",")))

    (executionTimes, readSizes.last)
  }

  def close(): Unit = {
    spark.close()
  }
}