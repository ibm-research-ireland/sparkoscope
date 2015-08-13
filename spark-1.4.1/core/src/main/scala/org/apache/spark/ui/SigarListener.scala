package org.apache.spark.ui

import java.util

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.scheduler.{SigarMetrics, SparkListener}
import scala.collection.mutable.HashMap

import scala.collection.mutable.ListBuffer

/**
 * Created by johngouf on 11/08/15.
 */
@DeveloperApi
class SigarListener() extends SparkListener with Logging {

  val sigarMetricsData = new ListBuffer[SigarMetrics]

  override def onSigarMetrics(sigarMetrics: SigarMetrics) : Unit = synchronized {
    System.out.println(sigarMetrics.bytesRxPerSecond+","+sigarMetrics.bytesTxPerSecond+","+sigarMetrics.host+","+sigarMetrics.timestamp)
    sigarMetricsData += sigarMetrics
  }

}
