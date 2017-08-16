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

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val LOGGER = LoggerFactory.getLogger(classOf[SigarSource])
  val sigar = new Sigar

  def pid : Long = sigar.getPid

  register(metricRegistry, new StatefulMetric {
    override val name = "network.sent_per_second"
    override def momentaryValue : Float = networkMetrics().bytesTx
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "network.received_per_second"
    override def momentaryValue : Float = networkMetrics().bytesRx
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "disk.written_per_second"
    override def momentaryValue : Float = diskMetrics.bytesWritten
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "disk.read_per_second"
    override def momentaryValue : Float = diskMetrics.bytesRead
  })

  register(metricRegistry, new Metric[Double] {
    override def name : String = "cpu.host.count"
    override def value : Double = sigar.getCpuInfoList.length
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.host.sys"
    override def momentaryValue : Float = sigar.getCpu.getSys
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.host.user"
    override def momentaryValue : Float = sigar.getCpu.getUser
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.host.wait"
    override def momentaryValue : Float = sigar.getCpu.getWait
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.host.total"
    override def momentaryValue : Float = sigar.getCpu.getTotal
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.process.sys"
    override def momentaryValue : Float = sigar.getProcCpu(pid).getSys
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.process.user"
    override def momentaryValue : Float = sigar.getProcCpu(pid).getUser
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.process.total"
    override def momentaryValue : Float = sigar.getProcCpu(pid).getTotal
  })

  register(metricRegistry, new Metric[Long] {
    override def name : String = "memory.host.total"
    override def value : Long = sigar.getMem.getTotal
  })

  register(metricRegistry, new Metric[Long] {
    override def name : String = "memory.host.used"
    override def value : Long = sigar.getMem.getUsed
  })

  register(metricRegistry, new Metric[Long] {
    override def name : String = "memory.host.free"
    override def value : Long = sigar.getMem.getFree
  })

  register(metricRegistry, new Metric[Long] {
    override def name : String = "memory.process.total"
    override def value : Long = sigar.getProcMem(pid).getSize
  })

  case class NetworkMetrics(bytesRx: Long, bytesTx: Long)

  case class DiskMetrics(bytesWritten: Long, bytesRead: Long)

  def networkMetrics(): NetworkMetrics = {
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
          LOGGER.error("Sigar couldn't get network metrics for interface " +interface +
            ", error: " + e.getMessage)
      }
    })
    NetworkMetrics(bytesReceived, bytesTransmitted)
  }

  def diskMetrics(): DiskMetrics = {
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
          LOGGER.error("Sigar couldn't get filesystem usage for filesystem " +
            fileSystem.getDevName +" mounted at " +fileSystem.getDirName +", error: " +
            e.getMessage)
      }
    })
    DiskMetrics(bytesWritten, bytesRead)
  }

  def register[T](registry: MetricRegistry, metric: Metric[T]) : Gauge[T] =
  {metricRegistry.register(metric.name, metric.gauge)}

  trait Metric[T] {
    def value: T
    def name: String

    def gauge: Gauge[T] = new Gauge[T] {
      override def getValue = value
    }
  }

  trait StatefulMetric extends Metric[Float] {
    var lastProbeTimestamp: Long = System.currentTimeMillis
    var lastValue: Float = momentaryValue

    def value : Float = synchronized {
      val now = System.currentTimeMillis
      val timeWindowInSec = (now - lastProbeTimestamp) / 1000f
      lastProbeTimestamp = now

      val newValue = momentaryValue
      val valueDiff = newValue - lastValue
      lastValue = newValue

      valueDiff / timeWindowInSec
    }

    def momentaryValue: Float
  }
}
