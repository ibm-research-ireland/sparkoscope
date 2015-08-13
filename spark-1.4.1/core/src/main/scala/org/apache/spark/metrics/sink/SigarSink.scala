package org.apache.spark.metrics.sink

/**
 * Created by johngouf on 27/07/15.
 */

import java.io._
import java.net.URI
import java.util.concurrent.{TimeUnit, ThreadFactory, Executors}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, Properties}

import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerApplicationEnd}
import org.apache.spark.{SparkFirehoseListener, SecurityManager}

import org.apache.spark.util.Utils

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

private[spark] class SigarSink(val property: Properties, val registry: MetricRegistry,
                               securityMgr: SecurityManager) extends Sink {

  val SIGAR_KEY_PERIOD = "pollPeriod"
  val SIGAR_KEY_UNIT = "unit"
  val SIGAR_KEY_DIR = "dir"

  val SIGAR_DEFAULT_PERIOD = 10
  val SIGAR_DEFAULT_UNIT = "SECONDS"
  val SIGAR_DEFAULT_DIR = "hdfs://localhost:9000/custom-metrics/"

  val pollPeriod = Option(property.getProperty(SIGAR_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => SIGAR_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(SIGAR_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(SIGAR_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val hdfsPath = Option(property.getProperty(SIGAR_KEY_DIR)) match {
    case Some(s) => s
    case None => SIGAR_DEFAULT_DIR
  }

  private val FACTORY_ID = new AtomicInteger(1)

  private val executor =  Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("sigar-sink" + '-' + FACTORY_ID.incrementAndGet()));

  val localhost = Utils.localHostName();

  //val entriesPerApplication = new HashMap[String,ListBuffer[String]]
  val writersPerApplication  = new HashMap[String, (BufferedWriter,FSDataOutputStream)]


  var fileSystem : FileSystem = _
  var conf : Configuration = _

  setHadoopConf()

  val sparkTmpDir = sys.env.getOrElse("SPARK_PID_DIR",System.getProperty("java.io.tmpdir"));
  val fileLock = new File(sparkTmpDir + File.separator + "sigar.pid")
  var startThread = false;
  if(!fileLock.exists())
  {
    if(fileLock.createNewFile()) startThread = true
  }

  fileLock.deleteOnExit()

  override def start() {
    if(startThread)
    {
      //the purpose of this thread is to accumulate metrics, create writers if they dont exist, close if apps have finished
      executor.scheduleAtFixedRate(new Runnable(){
        override def run(): Unit = {
          val gauges = registry.getGauges
          val timestamp = new Date().getTime;
          val appsRunning = gauges.get("sigar.appsRunning").getValue.toString;
          var appsRunningList = appsRunning.replaceAll("\\[", "").replaceAll("\\]","").split(",")
          if(appsRunning.replaceAll("\\[", "").replaceAll("\\]","").length==0) appsRunningList = new Array[String](0)

          val entryList = new ListBuffer[String]()
          gauges.keySet().toArray().filter(!_.toString.equals("sigar.appsRunning")).foreach(metric => {
            val value = gauges.get(metric).getValue.toString;
            val entry = "\""+metric+"\" : "+value
            entryList += entry
          })
          entryList += ("\"host\" : "+"\""+localhost+"\"")
          entryList += ("\"timestamp\" : "+timestamp)
          val entryString = "{ "+entryList.mkString(",")+" }"
          appsRunningList.foreach(appId => {

            var writer = writersPerApplication.getOrElse(appId,createWriter(appId))
            writer._1.write(entryString)
            writer._1.newLine()
            writer._1.flush()
            writer._2.flush()
            writer._2.hsync()
            writersPerApplication.put(appId,writer)
          })

          writersPerApplication.keySet.foreach(previousAppId => {
            if(!appsRunningList.contains(previousAppId))
            {
              writersPerApplication.get(previousAppId).get._1.close()
              writersPerApplication.remove(previousAppId)
            }
          })
        }
      },0,pollPeriod,pollUnit)
    }
  }

  override def stop() {
    System.out.println("stoping");
  }

  override def report() {
    System.out.println("reporting");
  }

  def setHadoopConf()
  {
    conf = new Configuration()
    if(sys.env.contains("HADOOP_CONF_DIR"))
    {
      val confDir = sys.env.get("HADOOP_CONF_DIR").get
      val file = new File(confDir)
      if(file.exists&&file.isDirectory)
      {
        conf.addResource(confDir)
      }
    }

    fileSystem = Utils.getHadoopFileSystem(new URI(hdfsPath), conf)
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

  def createWriter(appId: String): (BufferedWriter,FSDataOutputStream) = {
    val appFolder = new Path(hdfsPath + File.separator + appId)
    if (!fileSystem.exists(appFolder)) {
      val folderCreated = fileSystem.mkdirs(appFolder)
      System.out.println(appFolder+" "+folderCreated)
    }

    val finalPath = new Path(hdfsPath + File.separator + appId + File.separator + localhost +".json")
    if (!fileSystem.exists(finalPath)) {
      val createdNewFile = fileSystem.createNewFile(finalPath)
      System.out.println(finalPath+" "+createdNewFile)
    }

    var hadoopDataStream = fileSystem.append(finalPath)
    var writer: BufferedWriter = new BufferedWriter(new OutputStreamWriter(hadoopDataStream))
    (writer, hadoopDataStream)
  }
}
