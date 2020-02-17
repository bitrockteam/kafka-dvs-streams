package it.bitrock.dvs.streams.topologies

import org.apache.kafka.streams.scala.kstream.KStream

object StreamOps {

  implicit class KStreamOps[K, V](stream: KStream[K, V]) {

    def split(f1: (K, V) => Boolean, f2: (K, V) => Boolean): (KStream[K, V], KStream[K, V]) = {
      val streams = stream.branch(f1, f2)
      (streams(0), streams(1))
    }
  }
}
