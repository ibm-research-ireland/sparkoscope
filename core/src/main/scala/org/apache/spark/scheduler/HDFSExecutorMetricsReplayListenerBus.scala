package org.apache.spark.scheduler

import java.io.InputStream

import org.apache.spark.Logging
import scala.collection.{immutable, mutable}
import scala.util.parsing.json._

import scala.collection.mutable.ListBuffer
import scala.io.Source


/**
 * Created by Yiannis Gkoufas on 21/09/15.
 */
class HDFSExecutorMetricsReplayListenerBus extends SparkListenerBus with Logging {

  /**
   * Replay each event in the order maintained in the given stream. The stream is expected to
   * contain one JSON-encoded SparkListenerEvent per line.
   *
   * This method can be called multiple times, but the listener behavior is undefined after any
   * error is thrown by this method.
   *
   * @param logDataList Stream containing event log data.
   * @param sourceName Filename (or other source identifier) from whence @logData is being read
   * @param maybeTruncated Indicate whether log file might be truncated (some abnormal situations
   *        encountered, log file might not finished writing) or not
   */
  def replay(
              logDataList: ListBuffer[(InputStream,String)],
              sourceName: String,
              maybeTruncated: Boolean = false): Unit = {

    logDataList.foreach(logData => {
      try {
        for (line <- Source.fromInputStream(logData._1).getLines()) {
          val hashMapParsed = JSON.parseFull(line)
          val hashMap = {
            hashMapParsed match {
              case Some(m: Map[String, Any]) => m
              case _ => new immutable.HashMap[String,Any]
            }
          }
          val hdfsExecutorMetrics = new HDFSExecutorMetrics(
            hashMap("values").asInstanceOf[Map[String,Any]],
            hashMap("host").asInstanceOf[String],
            hashMap("timestamp").asInstanceOf[Double].toLong)
          postToAll(hdfsExecutorMetrics)
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace();
          logError(ex.toString)
          logWarning(s"Got JsonParseException from log file $logData")
        }
      }
    })
  }
}
