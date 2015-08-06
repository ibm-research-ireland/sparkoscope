package org.apache.spark.deploy.worker

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.metrics.source.Source

/**
 * Created by johngouf on 06/08/15.
 */
private[worker] class SigarSource(val worker: Worker) extends Source  {
  override def sourceName: String = "sigar"

  override def metricRegistry: MetricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("bytesTx"), new Gauge[Int] {
    override def getValue: Int = worker.executors.size
  })
}
