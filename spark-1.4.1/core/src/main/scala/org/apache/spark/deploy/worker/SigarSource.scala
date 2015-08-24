package org.apache.spark.deploy.worker

import java.util.Date

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.metrics.source.Source
import org.hyperic.sigar.{DiskUsage, Sigar}

/**
 * Created by johngouf on 06/08/15.
 */
private[worker] class SigarSource(val worker: Worker) extends Source  {
  override def sourceName: String = "sigar"
  override val metricRegistry : MetricRegistry = new MetricRegistry()

  case class NetworkMetrics(bytesRx: Long, bytesTx: Long)
  case class DiskMetrics(bytesWritten: Long, bytesRead: Long)

  val sigar = new Sigar()

  var initialNetworkMetrics: NetworkMetrics = getNetworkMetrics()
  var initialDiskMetrics: DiskMetrics = getDiskMetrics()

  var previousBytesTxCalculation: Long = new Date().getTime
  var previousBytesTx : Long = initialNetworkMetrics.bytesTx
  var previousBytesTxMeasurement : Double = 0.0

  var previousBytesRxCalculation: Long = new Date().getTime
  var previousBytesRx : Long = initialNetworkMetrics.bytesRx
  var previousBytesRxMeasurement : Double = 0.0

  var previousBytesWrittenCalculation: Long = new Date().getTime
  var previousBytesWritten: Long = initialDiskMetrics.bytesWritten
  var previousBytesWrittenMeasurement: Double = 0.0

  var previousBytesReadCalculation: Long = new Date().getTime
  var previousBytesRead: Long = initialDiskMetrics.bytesRead
  var previousBytesReadMeasurement: Double = 0.0

  metricRegistry.register(MetricRegistry.name("kBytesTxPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics : NetworkMetrics = getNetworkMetrics()
      val currentDate : Long = new Date().getTime
      val currentBytesTx : Long = currentMetrics.bytesTx

      if(currentDate - previousBytesTxCalculation < 10000) previousBytesTxMeasurement
      else {
        val diffSeconds = (currentDate - previousBytesTxCalculation) / 1000.0
        val diffBytes = currentBytesTx - previousBytesTx

        previousBytesTxCalculation = currentDate
        previousBytesTx = currentBytesTx

        if (diffBytes == 0) previousBytesTxMeasurement = 0.0
        else previousBytesTxMeasurement = diffBytes / diffSeconds

        previousBytesTxMeasurement/1000.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("kBytesRxPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics : NetworkMetrics = getNetworkMetrics()
      val currentDate : Long = new Date().getTime
      val currentBytesRx : Long = currentMetrics.bytesRx

      if(currentDate - previousBytesRxCalculation < 10000) previousBytesRxMeasurement
      else {
        val diffSeconds = (currentDate-previousBytesRxCalculation)/1000.0;
        val diffBytes = currentBytesRx-previousBytesRx

        previousBytesRxCalculation = currentDate
        previousBytesRx = currentBytesRx

        if(diffBytes==0) previousBytesRxMeasurement = 0.0
        else previousBytesRxMeasurement = diffBytes/diffSeconds

        previousBytesRxMeasurement/1000.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("kBytesWrittenPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics : DiskMetrics = getDiskMetrics()
      val currentDate : Long = new Date().getTime
      val currentBytesWritten : Long = currentMetrics.bytesWritten

      if(currentDate - previousBytesWrittenCalculation < 10000) previousBytesWrittenMeasurement
      else {
        val diffSeconds = (currentDate-previousBytesWrittenCalculation)/1000.0;
        val diffBytes = currentBytesWritten-previousBytesWritten

        previousBytesWrittenCalculation = currentDate
        previousBytesWritten = currentBytesWritten

        if(diffBytes==0) previousBytesWrittenMeasurement = 0.0
        else previousBytesWrittenMeasurement = diffBytes/diffSeconds;

        previousBytesWrittenMeasurement/1000.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("kBytesReadPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics : DiskMetrics = getDiskMetrics()
      val currentDate : Long = new Date().getTime
      val currentBytesRead : Long = currentMetrics.bytesRead

      if(currentDate - previousBytesReadCalculation < 10000) previousBytesReadMeasurement
      else {
        val diffSeconds = (currentDate-previousBytesReadCalculation)/1000.0;
        val diffBytes = currentBytesRead-previousBytesRead

        previousBytesReadCalculation = currentDate
        previousBytesRead = currentBytesRead

        if(diffBytes==0) previousBytesReadMeasurement = 0.0
        else previousBytesReadMeasurement = diffBytes/diffSeconds;

        previousBytesReadMeasurement/1000.0
      }
    }
  })

  metricRegistry.register(MetricRegistry.name("appsRunning"), new Gauge[String] {
    override def getValue: String = {
      worker.executors.foreach{case(x,y)=>{

      }}
      val allApps = worker.appDirectories;
      val finishedApps = worker.finishedApps
      finishedApps.foreach(finishedApp => {
        allApps.remove(finishedApp)
      })
      "["+worker.appDirectories.keySet.mkString(",")+"]"
    }
  })

  def getNetworkMetrics(): NetworkMetrics = {
    var bytesReceived = 0L
    var bytesTransmitted = 0L

    sigar.getNetInterfaceList.foreach(interface => {
      val netInterfaceStat = sigar.getNetInterfaceStat(interface)
      bytesReceived += netInterfaceStat.getRxBytes
      bytesTransmitted += netInterfaceStat.getTxBytes
    })
    NetworkMetrics(bytesReceived, bytesTransmitted)
  }

  def getDiskMetrics(): DiskMetrics = {
    var bytesWritten = 0L
    var bytesRead = 0L
    val diskUsage = new DiskUsage
    bytesWritten += diskUsage.getWriteBytes
    bytesRead += diskUsage.getReadBytes

    DiskMetrics(bytesWritten, bytesRead)
  }

}
