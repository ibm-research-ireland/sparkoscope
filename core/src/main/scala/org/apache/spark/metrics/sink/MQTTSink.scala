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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry

import org.apache.spark.{MQTTReporter, SecurityManager}
import org.apache.spark.metrics.MetricsSystem

private[spark] class MQTTSink(val property: Properties, val registry: MetricRegistry,
                              securityMgr: SecurityManager) extends Sink {
  val MQTT_KEY_PERIOD = "pollPeriod"
  val MQTT_KEY_UNIT = "unit"
  val MQTT_KEY_HOST = "host"
  val MQTT_KEY_PORT = "port"

  val MQTT_DEFAULT_PERIOD = 10
  val MQTT_DEFAULT_UNIT = "SECONDS"
  val MQTT_DEFAULT_HOST = "localhost"
  val MQTT_DEFAULT_PORT = 1883

  val pollPeriod = Option(property.getProperty(MQTT_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => MQTT_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(MQTT_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(MQTT_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val masterHost = Option(property.getProperty(MQTT_KEY_HOST)) match {
    case Some(s) => s
    case None => MQTT_DEFAULT_HOST
  }

  val masterPort = Option(property.getProperty(MQTT_KEY_PORT)) match {
    case Some(s) => s.toInt
    case None => MQTT_DEFAULT_PORT
  }

  val reporter: MQTTReporter = MQTTReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build(masterHost, masterPort)

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
