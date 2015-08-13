package org.apache.spark.deploy.worker

import java.util.Date

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.metrics.source.Source
import org.hyperic.sigar.Sigar

/**
 * Created by johngouf on 06/08/15.
 */
private[worker] class SigarSource(val worker: Worker) extends Source  {
  override def sourceName: String = "sigar"
  override val metricRegistry : MetricRegistry = new MetricRegistry()

  case class NetworkMetrics(bytesRx: Long, bytesTx: Long)
  case class AverageNetworkMetrics(bytesRxPerSecond: Double, bytesTxPerSecond: Double)

  val sigar = new Sigar();

  var initialMetrics: NetworkMetrics = getNetworkMetrics();

  var previousBytesTxCalculation: Long = new Date().getTime
  var previousBytesTx : Long = initialMetrics.bytesTx

  var previousBytesRxCalculation: Long = new Date().getTime
  var previousBytesRx : Long = initialMetrics.bytesRx

  metricRegistry.register(MetricRegistry.name("bytesTxPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics : NetworkMetrics = getNetworkMetrics();
      val currentDate : Long = new Date().getTime;
      val currentBytesTx : Long = currentMetrics.bytesTx;

      val diffSeconds = (currentDate-previousBytesTxCalculation)/1000.0;
      val diffBytes = currentBytesTx-previousBytesTx

      previousBytesTxCalculation = currentDate
      previousBytesTx = currentBytesTx

      if(diffBytes==0) 0.0
      else diffBytes/diffSeconds;
    }
  })

  metricRegistry.register(MetricRegistry.name("bytesRxPerSecond"), new Gauge[Double] {
    override def getValue: Double = {
      val currentMetrics : NetworkMetrics = getNetworkMetrics();
      val currentDate : Long = new Date().getTime;
      val currentBytesRx : Long = currentMetrics.bytesRx;

      val diffSeconds = (currentDate-previousBytesRxCalculation)/1000.0;
      val diffBytes = currentBytesRx-previousBytesRx

      previousBytesRxCalculation = currentDate
      previousBytesRx = currentBytesRx

      if(diffBytes==0) 0.0
      else diffBytes/diffSeconds;
    }
  })

  metricRegistry.register(MetricRegistry.name("appsRunning"), new Gauge[String] {
    override def getValue: String = {
      val allApps = worker.appDirectories;
      val finishedApps = worker.finishedApps
      finishedApps.foreach(finishedApp => {
        allApps.remove(finishedApp)
      })
      "["+worker.appDirectories.keySet.mkString(",")+"]"
    }
  })

  def getNetworkMetrics(): NetworkMetrics = {
    var bytesReceived = 0L;
    var bytesTransmitted = 0L;

    sigar.getNetInterfaceList.foreach(interface => {
      val netInterfaceStat = sigar.getNetInterfaceStat(interface);
      bytesReceived += netInterfaceStat.getRxBytes;
      bytesTransmitted += netInterfaceStat.getTxBytes;
    })
    NetworkMetrics(bytesReceived, bytesTransmitted)
  }

}
