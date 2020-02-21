package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Duration

import it.bitrock.dvs.model.avro.FlightRaw
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.{TimestampedKeyValueStore, ValueAndTimestamp}

import scala.collection.JavaConverters._

class ControlledEmitter(storeName: String, tickDuration: Duration)
    extends Transformer[String, FlightRaw, KeyValue[String, FlightRaw]] {
  var context: ProcessorContext                               = _
  var stateStore: TimestampedKeyValueStore[String, FlightRaw] = _
  var tickCancel: Cancellable                                 = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    stateStore = context.getStateStore(storeName).asInstanceOf[TimestampedKeyValueStore[String, FlightRaw]]

    tickCancel = context.schedule(
      tickDuration,
      PunctuationType.WALL_CLOCK_TIME,
      new Tick(context, stateStore)
    )
  }

  override def close(): Unit = tickCancel.cancel()

  override def transform(key: String, value: FlightRaw): KeyValue[String, FlightRaw] = {
    val timestamp = context.timestamp()
    if (timestamp < now) {
      // emit
      KeyValue.pair(key, value)
    } else {
      // store
      stateStore.put(key, ValueAndTimestamp.make(value, timestamp))
      null // do not emit
    }
  }

  private def now: Long = System.currentTimeMillis()

  private class Tick(context: ProcessorContext, store: TimestampedKeyValueStore[String, FlightRaw]) extends Punctuator {
    override def punctuate(now: Long): Unit =
      store
        .all()
        .asScala
        .filter(_.value.timestamp <= now)
        .foreach { kv =>
          val key        = kv.key
          val valueAndTs = kv.value
          context.forward(
            key,
            valueAndTs.value(),
            To.all.withTimestamp(valueAndTs.timestamp())
          )
          store.delete(key)
        }
  }
}

object ControlledEmitter {
  def transformStream(
      storeName: String,
      tickDuration: Duration,
      stream: KStream[String, FlightRaw]
  ): KStream[String, FlightRaw] =
    stream.transform(Supplier(storeName, tickDuration))

  private case class Supplier(storeName: String, tickDuration: Duration)
      extends TransformerSupplier[String, FlightRaw, KeyValue[String, FlightRaw]] {
    override def get(): Transformer[String, FlightRaw, KeyValue[String, FlightRaw]] =
      new ControlledEmitter(storeName, tickDuration)
  }
}
