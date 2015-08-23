package org.apache.spark.scheduler

import java.io.{InputStreamReader, BufferedReader, InputStream, IOException}

import scala.collection.mutable.ListBuffer
import scala.io.Source

import com.fasterxml.jackson.core.JsonParseException
import org.json4s.jackson.JsonMethods._

import org.apache.spark.Logging
import org.apache.spark.util.JsonProtocol

/**
 * A SparkListenerBus that can be used to replay events from serialized event data.
 */
private[spark] class SigarReplayListenerBus extends SparkListenerBus with Logging {

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
              logDataList: ListBuffer[InputStream],
              sourceName: String,
              maybeTruncated: Boolean = false): Unit = {
    var currentLine: String = null
    logDataList.foreach(logData => {
      val lines = new BufferedReader(new InputStreamReader(logData))
      try{
        while ((currentLine = lines.readLine()) != null) {
          postToAll(JsonProtocol.sigarMetricsFromJson(parse(currentLine)))
        }
      }catch {
        case e: Exception =>
          logWarning(s"Got JsonParseException from log file $sourceName")
      }
      lines.close()
    })
  }
}
