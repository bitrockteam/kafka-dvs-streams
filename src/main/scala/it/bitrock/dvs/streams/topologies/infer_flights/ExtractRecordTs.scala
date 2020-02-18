package it.bitrock.dvs.streams.topologies.infer_flights

import it.bitrock.dvs.model.avro.FlightRaw
import it.bitrock.dvs.streams.topologies.infer_flights.model.FlightRawTs
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.kstream.KStream

object ExtractRecordTs {
  def apply(stream: KStream[String, FlightRaw], storeName: String): KStream[String, FlightRawTs] =
    stream.transform(ExtractorTransformerSupplier, storeName)

  private object ExtractorTransformerSupplier extends TransformerSupplier[String, FlightRaw, KeyValue[String, FlightRawTs]] {
    override def get(): Transformer[String, FlightRaw, KeyValue[String, FlightRawTs]] = new ExtractorTransformer()
  }

  private class ExtractorTransformer extends Transformer[String, FlightRaw, KeyValue[String, FlightRawTs]] {
    var context: ProcessorContext = _

    override def init(context: ProcessorContext): Unit =
      this.context = context

    override def close(): Unit = {}

    override def transform(key: String, value: FlightRaw): KeyValue[String, FlightRawTs] = {
      val recordTs = context.timestamp()
      KeyValue.pair(key, FlightRawTs(recordTs, value))
    }
  }
}
