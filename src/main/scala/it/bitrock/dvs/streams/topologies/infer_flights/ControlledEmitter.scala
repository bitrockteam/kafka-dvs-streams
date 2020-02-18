package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Duration

import it.bitrock.dvs.streams.topologies.infer_flights.model.FlightRawTs
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{Cancellable, ProcessorContext, PunctuationType, Punctuator, To}
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.KeyValueStore

import scala.collection.JavaConverters._

class ControlledEmitter(storeName: String, tickDuration: Duration) extends Transformer[String, FlightRawTs, KeyValue[String, FlightRawTs]] {
  var context: ProcessorContext = _
  var stateStore: KeyValueStore[String, FlightRawTs] = _
  var tickCancel: Cancellable = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    stateStore = context.getStateStore(storeName).asInstanceOf[KeyValueStore[String, FlightRawTs]]

    tickCancel = context.schedule(
      tickDuration,
      PunctuationType.WALL_CLOCK_TIME,
      new Tick(context, stateStore)
    )
  }

  override def close(): Unit = tickCancel.cancel()

  override def transform(key: String, value: FlightRawTs): KeyValue[String, FlightRawTs] = {
    if (value.timestamp < now) {
      // emit
      KeyValue.pair(key, value)
    } else {
      // store
      stateStore.put(key, value)
      null // do not emit
    }
  }

  private def now = System.currentTimeMillis()

  private class Tick(context: ProcessorContext, store: KeyValueStore[String, FlightRawTs]) extends Punctuator {
    override def punctuate(now: Long): Unit = {
      store
        .all().asScala
        .filter(_.value.timestamp <= now)
        .foreach(kv => {
          val key = kv.key
          val value = kv.value
          context.forward(
            key,
            value,
            To.all.withTimestamp(kv.value.timestamp)
          )
          store.delete(key)
        })
    }
  }
}

object ControlledEmitter {
  def transformStream(storeName: String, tickDuration: Duration, stream: KStream[String, FlightRawTs]): KStream[String, FlightRawTs] = {
    stream.transform(Supplier(storeName, tickDuration))
  }

  private case class Supplier(storeName: String, tickDuration: Duration) extends TransformerSupplier[String, FlightRawTs, KeyValue[String, FlightRawTs]] {
    override def get(): Transformer[String, FlightRawTs, KeyValue[String, FlightRawTs]] = new ControlledEmitter(storeName, tickDuration)
  }
}
