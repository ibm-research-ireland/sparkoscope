package org.apache.spark.metrics.sink

/**
 * Created by johngouf on 27/07/15.
 */
import java.io.{FileWriter, File}
import java.util.concurrent.{ThreadFactory, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, Locale, Properties}

import com.codahale.metrics.{CsvReporter, MetricRegistry}
import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.Utils
import org.hyperic.sigar.Sigar



private[spark] class SigarSink(val property: Properties, val registry: MetricRegistry,
                               securityMgr: SecurityManager) extends Sink {

  case class NetworkMetrics(bytesRx: Long, bytesTx: Long)
  case class AverageNetworkMetrics(bytesRxPerSecond: Double, bytesTxPerSecond: Double)

  val CSV_DEFAULT_DIR = "/tmp/";

  private val FACTORY_ID = new AtomicInteger(1)

  private val executor =  Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("sigar-sink" + '-' + FACTORY_ID.incrementAndGet()));

  val sigar = new Sigar();
  var initialMetrics: NetworkMetrics = getNetworkMetrics();

  val localhost = Utils.localHostName();

  val file = new File(CSV_DEFAULT_DIR+localhost+"-network.txt");
  if(file.exists()) file.delete();

  val fw = new FileWriter(file, true)

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

  def getAverageNetworkMetric(beforeNetworkMetrics: NetworkMetrics, afterNetworkMetrics: NetworkMetrics): AverageNetworkMetrics = {
    val bytesRxPerSecond = (afterNetworkMetrics.bytesRx-beforeNetworkMetrics.bytesRx)/10.0;
    val bytesTxPerSecond = (afterNetworkMetrics.bytesTx-beforeNetworkMetrics.bytesTx)/10.0;
    AverageNetworkMetrics(bytesRxPerSecond, bytesTxPerSecond);
  }

  override def start() {

    executor.scheduleAtFixedRate(new Runnable(){
      override def run(): Unit = {

        val date = new Date();

        val newMetrics: NetworkMetrics = getNetworkMetrics();

        val averageNetworkMetrics: AverageNetworkMetrics = getAverageNetworkMetric(initialMetrics,newMetrics);

        fw.write(date.getTime+","+averageNetworkMetrics.bytesRxPerSecond+","+averageNetworkMetrics.bytesTxPerSecond);
        fw.write("\n");
        fw.flush();

        initialMetrics = newMetrics;

      }
    },10,10,TimeUnit.SECONDS)

    executor.scheduleAtFixedRate(new Runnable(){
      override def run(): Unit = {
        System.out.println("Aggregate!!");
      }
    },0,50,TimeUnit.SECONDS)
  }

  override def stop() {
    fw.close();
    System.out.println("stoping");
  }

  override def report() {
    System.out.println("reporting");
  }

  class NamedThreadFactory (name: String) extends ThreadFactory {

    private val group = if ((s != null)) s.getThreadGroup else Thread.currentThread().getThreadGroup

    private val threadNumber = new AtomicInteger(1)

    private val namePrefix = "metrics-" + name + "-thread-"

    val s = System.getSecurityManager

    override def newThread(r: Runnable): Thread = {
      val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
      t.setDaemon(true)
      if (t.getPriority != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY)
      }
      t
    }
  }
}
