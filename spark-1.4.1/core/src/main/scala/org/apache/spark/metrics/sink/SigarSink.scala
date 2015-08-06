package org.apache.spark.metrics.sink

/**
 * Created by johngouf on 27/07/15.
 */
import java.io._
import java.net.URI
import java.util
import java.util.concurrent.{ThreadFactory, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, Locale, Properties}

import com.codahale.metrics.{Metric, CsvReporter, MetricRegistry}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataOutputStream, Path, FileSystem}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.{HistoryServer, FsHistoryProvider}
import org.apache.spark.{Logging, SparkConf, SecurityManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.Utils
import org.hyperic.sigar.Sigar
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


private[spark] class SigarSink(val property: Properties, val registry: MetricRegistry,
                               securityMgr: SecurityManager, config: SparkConf) extends Sink {

  case class NetworkMetrics(bytesRx: Long, bytesTx: Long)
  case class AverageNetworkMetrics(bytesRxPerSecond: Double, bytesTxPerSecond: Double)


  val CSV_DEFAULT_DIR = "/tmp/";

  private val FACTORY_ID = new AtomicInteger(1)

  private val executor =  Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("sigar-sink" + '-' + FACTORY_ID.incrementAndGet()));

  val sigar = new Sigar();
  var initialMetrics: NetworkMetrics = getNetworkMetrics();

  val localhost = Utils.localHostName();

  var entries = new ListBuffer[String]();

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

  val conf : Configuration = new Configuration()
  val outputBufferSize = conf.getInt("spark.eventLog.buffer.kb", 100) * 1024

  val path = new Path("hdfs://localhost:9000/custom-metrics/"+localhost+"-network")
  val fileSystem = Utils.getHadoopFileSystem(new URI("hdfs://localhost:9000/custom-metrics/"), conf)

  var hadoopDataStream: Option[FSDataOutputStream] = None

  val dstream = {
    hadoopDataStream = Some(fileSystem.create(path))
    hadoopDataStream.get
  }

  val bstream = new BufferedOutputStream(dstream, outputBufferSize)
  val workersForApplications : mutable.HashMap[String,PrintWriter] = new mutable.HashMap[String,PrintWriter]()

  var writer: Option[PrintWriter] = None
  writer = Some(new PrintWriter(bstream))

  System.out.println(System.getenv("SPARK_MASTER_WEBUI_PORT"));
  config.getAll.foreach(e => println(e._1+","+e._2))

  override def start() {

    executor.scheduleAtFixedRate(new Runnable(){
      override def run(): Unit = {

        val date = new Date();

        val newMetrics: NetworkMetrics = getNetworkMetrics();

        val averageNetworkMetrics: AverageNetworkMetrics = getAverageNetworkMetric(initialMetrics,newMetrics);

        entries += (date.getTime+","+averageNetworkMetrics.bytesRxPerSecond+","+averageNetworkMetrics.bytesTxPerSecond);

        initialMetrics = newMetrics;

      }
    },10,10,TimeUnit.SECONDS)

    executor.scheduleAtFixedRate(new Runnable(){
      override def run(): Unit = {
        System.out.println("Aggregate!!");
        writer.foreach(_.println(entries.mkString("\n")))
        writer.foreach(_.flush())
        hadoopDataStream.foreach(hadoopFlushMethod.invoke(_))
        entries.clear();
        //writer.foreach(_.close())
      }
    },15,55,TimeUnit.SECONDS)
  }

  override def stop() {
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

  private val hadoopFlushMethod = {
    val cls = classOf[FSDataOutputStream]
    scala.util.Try(cls.getMethod("hflush")).getOrElse(cls.getMethod("sync"))
  }
}
