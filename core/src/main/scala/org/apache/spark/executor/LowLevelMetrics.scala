/*
 * Added by Bogdan Nicolae. License pending, for IBM internal use only
 */

package org.apache.spark.executor

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi

/**
  * :: DeveloperApi ::
  * Low level networking shuffle metrics. They are captured from the netty block transfer service
  * and stored in SparkEnv, the only high level container available at netty level
  */
@DeveloperApi
class LowLevelMetrics extends Serializable {
  /**
    * Average number of blocks per request
    */
  private var _reqNo: Int = 0
  private var _blockNo: Int = 0

  def getAvgBlocksPerRequest() : Double = if (_reqNo > 0) _blockNo * 1.0 / _reqNo else 0.0
  def updateRequestMetrics(blockNo: Int) {
    _reqNo += 1
    _blockNo += blockNo
  }
  def reset() {
    _reqNo = 0
    _blockNo = 0
  }
}
