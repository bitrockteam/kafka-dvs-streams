package it.bitrock.dvs

import it.bitrock.dvs.streams.config.TopologyConfig
import org.apache.kafka.streams.kstream.TimeWindows

import scala.concurrent.duration.FiniteDuration

package object streams {
  def duration2JavaDuration(d: FiniteDuration): java.time.Duration =
    java.time.Duration.ofNanos(d.toNanos)

  def aggregationTimeWindows(topology: TopologyConfig): TimeWindows =
    TimeWindows
      .of(duration2JavaDuration(topology.aggregationTimeWindowSize))
      .grace(duration2JavaDuration(topology.aggregationTimeWindowGrace))

  def totalAggregationTimeWindows(topology: TopologyConfig): TimeWindows =
    TimeWindows
      .of(duration2JavaDuration(topology.aggregationTotalTimeWindowSize))
      .grace(duration2JavaDuration(topology.aggregationTimeWindowGrace))
}
