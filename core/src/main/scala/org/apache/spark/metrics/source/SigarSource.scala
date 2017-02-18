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

package org.apache.spark.metrics.source

import java.util.Date

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.hyperic.sigar.Sigar
import org.slf4j.LoggerFactory

private[spark] class SigarSource() extends Source {
  override def sourceName: String = "sigar"

  var LOGGER = LoggerFactory.getLogger(classOf[SigarSource]);

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  case class NetworkMetrics(bytesRx: Long, bytesTx: Long)
  case class DiskMetrics(bytesWritten: Long, bytesRead: Long)

  val sigar = new Sigar()

  var initialNetworkMetrics: NetworkMetrics = getNetworkMetrics()
  var initialDiskMetrics: DiskMetrics = getDiskMetrics()

  var previousBytesTxCalculation: Long = new Date().getTime
  var previousBytesTx: Long = initialNetworkMetrics.bytesTx
  var previousBytesTxMeasurement: Double = 0.0

  var previousBytesRxCalculation: Long = new Date().getTime
  var previousBytesRx: Long = initialNetworkMetrics.bytesRx
  var previousBytesRxMeasurement: Double = 0.0

  var previousBytesWrittenCalculation: Long = new Date().getTime
  var previousBytesWritten: Long = initialDiskMetrics.bytesWritten
  var previousBytesWrittenMeasurement: Double = 0.0

  var previousBytesReadCalculation: Long = new Date().getTime
  var previousBytesRead: Long = initialDiskMetrics.bytesRead
  var previousBytesReadMeasurement: Double = 0.0

  metricRegistry.register(MetricRegistry.name("sigar.kBytesTxPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics: NetworkMetrics = getNetworkMetrics()
      val currentDate: Long = new Date().getTime
      val currentBytesTx: Long = currentMetrics.bytesTx

      if (currentDate - previousBytesTxCalculation < 10000) previousBytesTxMeasurement / 1000.0
      else {
        val diffSeconds = (currentDate - previousBytesTxCalculation) / 1000.0
        val diffBytes = currentBytesTx - previousBytesTx

        previousBytesTxCalculation = currentDate
        previousBytesTx = currentBytesTx

        if (diffBytes == 0) previousBytesTxMeasurement = 0.0
        else previousBytesTxMeasurement = diffBytes / diffSeconds

        previousBytesTxMeasurement / 1000.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("sigar.kBytesRxPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics: NetworkMetrics = getNetworkMetrics()
      val currentDate: Long = new Date().getTime
      val currentBytesRx: Long = currentMetrics.bytesRx

      if (currentDate - previousBytesRxCalculation < 10000) previousBytesRxMeasurement / 1000.0
      else {
        val diffSeconds = (currentDate - previousBytesRxCalculation) / 1000.0;
        val diffBytes = currentBytesRx - previousBytesRx

        previousBytesRxCalculation = currentDate
        previousBytesRx = currentBytesRx

        if (diffBytes == 0) previousBytesRxMeasurement = 0.0
        else previousBytesRxMeasurement = diffBytes / diffSeconds

        previousBytesRxMeasurement / 1000.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("sigar.kBytesWrittenPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics: DiskMetrics = getDiskMetrics()
      val currentDate: Long = new Date().getTime
      val currentBytesWritten: Long = currentMetrics.bytesWritten

      if (currentDate - previousBytesWrittenCalculation < 10000) {
        previousBytesWrittenMeasurement / 1000.0
      } else {
        val diffSeconds = (currentDate - previousBytesWrittenCalculation) / 1000.0;
        val diffBytes = currentBytesWritten - previousBytesWritten

        previousBytesWrittenCalculation = currentDate
        previousBytesWritten = currentBytesWritten

        if (diffBytes == 0) previousBytesWrittenMeasurement = 0.0
        else previousBytesWrittenMeasurement = diffBytes / diffSeconds;

        previousBytesWrittenMeasurement / 1000.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("sigar.kBytesReadPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics: DiskMetrics = getDiskMetrics()
      val currentDate: Long = new Date().getTime
      val currentBytesRead: Long = currentMetrics.bytesRead

      if (currentDate - previousBytesReadCalculation < 10000) previousBytesReadMeasurement / 1000.0
      else {
        val diffSeconds = (currentDate - previousBytesReadCalculation) / 1000.0;
        val diffBytes = currentBytesRead - previousBytesRead

        previousBytesReadCalculation = currentDate
        previousBytesRead = currentBytesRead

        if (diffBytes == 0) previousBytesReadMeasurement = 0.0
        else previousBytesReadMeasurement = diffBytes / diffSeconds;

        previousBytesReadMeasurement / 1000.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("sigar.cpu"), new Gauge[Double] {
    override def getValue: Double = {
      try {
      sigar.getCpuPerc.getCombined*100.0
      } catch {
        case e: Exception =>
          e.printStackTrace()
          LOGGER.error("Sigar couldn't get cpu utilization, error: " + e.getMessage)
          0.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("sigar.ram"), new Gauge[Double] {
    override def getValue: Double = {
      try {
      sigar.getMem.getUsedPercent
      } catch {
        case e: Exception =>
          LOGGER.error("Sigar couldn't get memory utilization, error: " + e.getMessage)
          0.0
      }
    }
  });

  def getNetworkMetrics(): NetworkMetrics = {
    var bytesReceived = 0L
    var bytesTransmitted = 0L

    sigar.getNetInterfaceList.foreach(interface => {
      try
      {
        val netInterfaceStat = sigar.getNetInterfaceStat(interface)
        bytesReceived += netInterfaceStat.getRxBytes
        bytesTransmitted += netInterfaceStat.getTxBytes
      } catch {
        case e: Exception =>
          LOGGER.error("Sigar couldn't get network metrics for interface " +interface +", error: "
            + e.getMessage)
      }
    })
    NetworkMetrics(bytesReceived, bytesTransmitted)
  }

  def getDiskMetrics(): DiskMetrics = {
    var bytesWritten = 0L
    var bytesRead = 0L

    sigar.getFileSystemList.foreach(fileSystem => {
      try {
        val diskUsage = sigar.getFileSystemUsage(fileSystem.getDirName)
        val systemBytesWritten = diskUsage.getDiskWriteBytes
        val systemBytesRead = diskUsage.getDiskReadBytes
        if (systemBytesWritten > 0) {
          bytesWritten += systemBytesWritten
        }
        if (systemBytesRead > 0) {
          bytesRead += systemBytesRead
        }
      } catch {
        case e: Exception =>
          LOGGER.error("Sigar couldn't get filesystem usage for filesystem "
            +fileSystem.getDevName +" mounted at "
            +fileSystem.getDirName +", error: " + e.getMessage)
      }
    })
    DiskMetrics(bytesWritten, bytesRead)
  }
}
