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
package org.apache.spark.scheduler

import java.io.InputStream

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json._

import org.apache.spark.internal.Logging

private[spark] class HDFSExecutorMetricsReplayListenerBus extends SparkListenerBus with Logging {

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
              logDataList: ListBuffer[(InputStream, String)],
              sourceName: String,
              maybeTruncated: Boolean = false): Unit = {

    logDataList.foreach(logData => {
      try {
        for (line <- Source.fromInputStream(logData._1).getLines()) {
          val hashMapParsed = JSON.parseFull(line)
          val hashMap = {
            hashMapParsed match {
              case Some(m: Map[String, Any]) => m
              case _ => new immutable.HashMap[String, Any]
            }
          }
          val hdfsExecutorMetrics = new HDFSExecutorMetrics(
            hashMap("values").asInstanceOf[Map[String, Any]],
            hashMap("host").asInstanceOf[String],
            hashMap("timestamp").asInstanceOf[Double].toLong)
          postToAll(hdfsExecutorMetrics)
        }
      } catch {
        case ex: Exception =>
          ex.printStackTrace();
          logError(ex.toString)
          logWarning(s"Got JsonParseException from log file $logData")
      }
    })
  }
}
