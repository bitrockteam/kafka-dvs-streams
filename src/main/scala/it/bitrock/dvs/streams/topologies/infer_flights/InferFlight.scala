package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Duration

import it.bitrock.dvs.model.avro.FlightRaw
import it.bitrock.dvs.streams.topologies.infer_flights.lib.MoveFlight
import it.bitrock.dvs.streams.topologies.infer_flights.model.FlightRawTs
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, To}
import org.apache.kafka.streams.scala.kstream.KStream

object InferFlight extends ((KStream[String, FlightRaw], Duration, String) => KStream[String, FlightRawTs]) {
  override def apply(stream: KStream[String, FlightRaw], interval: Duration, storeName: String): KStream[String, FlightRawTs] = {
    val supplier = new TransformerSupplier[String, FlightRaw, KeyValue[String, FlightRawTs]] {
      override def get(): Transformer[String, FlightRaw, KeyValue[String, FlightRawTs]] = new InferTransformer(interval)
    }
    stream.transform(supplier, storeName)
  }

  private class InferTransformer(interval: Duration) extends Transformer[String, FlightRaw, KeyValue[String, FlightRawTs]] {
    var context: ProcessorContext = _

    override def init(context: ProcessorContext): Unit = this.context = context

    override def transform(key: String, value: FlightRaw): KeyValue[String, FlightRawTs] = {
      val currentEvTs = context.timestamp()
      // assuming eventTS >= flight.system.updated
      val timeDelta            = currentEvTs - value.system.updated.toEpochMilli + interval.toMillis
      val newFlight: FlightRaw = MoveFlight(value, timeDelta)
      val flightTs             = newFlight.system.updated.toEpochMilli
      val flightWithTs         = FlightRawTs(flightTs, newFlight)
      context.forward(key, flightWithTs, To.all.withTimestamp(flightTs))
      null // we emit using context.forward
    }

    override def close(): Unit = {}
  }
}
