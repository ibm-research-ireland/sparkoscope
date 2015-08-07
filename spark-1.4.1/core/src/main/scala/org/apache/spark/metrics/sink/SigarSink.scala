package org.apache.spark.metrics.sink

/**
 * Created by johngouf on 27/07/15.
 */

import java.io.{File, BufferedOutputStream, PrintWriter}
import java.net.URI
import java.util.concurrent.{TimeUnit, ThreadFactory, Executors}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, Properties}

import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.{SparkConf, SecurityManager}

import org.apache.spark.util.Utils

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

private[spark] class SigarSink(val property: Properties, val registry: MetricRegistry,
                               securityMgr: SecurityManager) extends Sink {

  private val FACTORY_ID = new AtomicInteger(1)

  private val executor =  Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("sigar-sink" + '-' + FACTORY_ID.incrementAndGet()));

  val localhost = Utils.localHostName();

  val entriesPerApplication = new HashMap[String,ListBuffer[String]]
  val writersPerApplication = new HashMap[String,(PrintWriter,FSDataOutputStream)]

  val hdfsPath = "hdfs://localhost:9000/custom-metrics/"

  var fileSystem : FileSystem = _
  var sparkConf : SparkConf = _
  var conf : Configuration = _
  var outputBufferSize : Int = _

  override def start() {

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
          var existingEntries = entriesPerApplication.getOrElse(appId, new ListBuffer[String])
          existingEntries += entryString
          entriesPerApplication.put(appId,existingEntries)

          val writerAndStream = writersPerApplication.getOrElse(appId, createWriterAndStream(appId))
          writersPerApplication.put(appId,writerAndStream)

          if(existingEntries.length==5)
          {
            existingEntries.foreach(entry => {
              writerAndStream._1.println(entry);
            })
            writerAndStream._1.flush()
            hadoopFlushMethod.invoke(writerAndStream._2)
            entriesPerApplication.put(appId,new ListBuffer[String])
          }
        })

        writersPerApplication.keySet.foreach(previousAppId => {
          if(!appsRunningList.contains(previousAppId))
          {
            System.out.println(" "+previousAppId+" has finished");
            closeWriterAndStream(writersPerApplication.get(previousAppId).get)
            writersPerApplication.remove(previousAppId)
            entriesPerApplication.remove(previousAppId)
          }
        })
      }
    },0,10,TimeUnit.SECONDS)
  }

  override def stop() {
    System.out.println("stoping");
  }

  override def report() {
    System.out.println("reporting");
  }

  def setSparkConf(sparkConf: SparkConf)
  {
    this.sparkConf = sparkConf
    this.outputBufferSize = sparkConf.getInt("spark.eventLog.buffer.kb", 100) * 1024

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

  def createWriterAndStream(appId: String): (PrintWriter,FSDataOutputStream) = {
    val finalPath = new Path(hdfsPath+"/"+appId)

    var hadoopDataStream: FSDataOutputStream = fileSystem.create(finalPath)
    val bstream = new BufferedOutputStream(hadoopDataStream, outputBufferSize)
    var writer: PrintWriter = (new PrintWriter(bstream))
    (writer,hadoopDataStream)
  }

  def closeWriterAndStream(tuple: (PrintWriter, FSDataOutputStream)) = {
    val writer = tuple._1;
    val outputStream = tuple._2;

    writer.flush()
    hadoopFlushMethod.invoke(outputStream)
    writer.close()
  }

  private val hadoopFlushMethod = {
    val cls = classOf[FSDataOutputStream]
    scala.util.Try(cls.getMethod("hflush")).getOrElse(cls.getMethod("sync"))
  }
}
