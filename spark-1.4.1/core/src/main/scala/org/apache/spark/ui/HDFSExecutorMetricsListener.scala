package org.apache.spark.ui

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{HDFSExecutorMetrics, SparkListener}

import scala.collection.mutable.ListBuffer

/**
 * Created by johngouf on 11/08/15.
 */
@DeveloperApi
class HDFSExecutorMetricsListener() extends SparkListener with Logging {

  val hdfsExecutorMetricsData = new ListBuffer[HDFSExecutorMetrics]

  override def onHDFSExecutorMetrics(hdfsExecutorMetrics: HDFSExecutorMetrics) : Unit = synchronized {
    hdfsExecutorMetricsData += hdfsExecutorMetrics;
  }

}
