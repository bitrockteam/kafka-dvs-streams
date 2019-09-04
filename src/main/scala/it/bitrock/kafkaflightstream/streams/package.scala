package it.bitrock.kafkaflightstream

import scala.concurrent.duration.FiniteDuration

package object streams {
  def duration2JavaDuration(d: FiniteDuration): java.time.Duration =
    java.time.Duration.ofNanos(d.toNanos)
}
