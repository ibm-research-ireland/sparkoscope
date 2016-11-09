package org.apache.spark.metrics.sink

/**
 * Created by Yiannis Gkoufas on 17/09/15.
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry

import org.apache.spark.{HDFSReporter, SecurityManager}
import org.apache.spark.metrics.MetricsSystem

private[spark] class HDFSSink(val property: Properties, val registry: MetricRegistry,
                             securityMgr: SecurityManager) extends Sink {
  val HDFS_KEY_PERIOD = "period"
  val HDFS_KEY_UNIT = "unit"
  val HDFS_KEY_DIR = "dir"

  val HDFS_DEFAULT_PERIOD = 10
  val HDFS_DEFAULT_UNIT = "SECONDS"
  val HDFS_DEFAULT_DIR = "hdfs://localhost:9000/custom-metrics/"

  val pollPeriod = Option(property.getProperty(HDFS_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => HDFS_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(HDFS_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(HDFS_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val pollDir = Option(property.getProperty(HDFS_KEY_DIR)) match {
    case Some(s) => s
    case None => HDFS_DEFAULT_DIR
  }

  val reporter: HDFSReporter = HDFSReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build(pollDir)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
