package it.bitrock.kafkaflightstream.streams

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.model.{System => _, _}
import it.bitrock.kafkaflightstream.streams.config.AppConfig
import it.bitrock.kafkageostream.kafkacommons.serialization.AvroSerdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes

import scala.concurrent.duration._

object Main extends App with LazyLogging {
  logger.info("Starting up")

  val config = AppConfig.load
  logger.debug(s"Loaded configuration: $config")

  val avroSerdes = new AvroSerdes(config.kafka.schemaRegistryUrl)

  val kafkaStreamsOptions = KafkaStreamsOptions(
    Serdes.String,
    avroSerdes.serdeFrom[FlightRaw],
    avroSerdes.serdeFrom[AirportRaw],
    avroSerdes.serdeFrom[AirlineRaw],
    avroSerdes.serdeFrom[CityRaw],
    avroSerdes.serdeFrom[AirplaneRaw],
    avroSerdes.serdeFrom[FlightWithDepartureAirportInfo],
    avroSerdes.serdeFrom[FlightWithAllAirportInfo],
    avroSerdes.serdeFrom[FlightWithAirline],
    avroSerdes.serdeFrom[FlightReceived],
    avroSerdes.serdeFrom[FlightReceivedList],
    Serdes.Long,
    avroSerdes.serdeFrom[TopArrivalAirportList],
    avroSerdes.serdeFrom[TopDepartureAirportList],
    avroSerdes.serdeFrom[Airport],
    avroSerdes.serdeFrom[TopSpeedList],
    avroSerdes.serdeFrom[SpeedFlight],
    avroSerdes.serdeFrom[TopAirlineList],
    avroSerdes.serdeFrom[Airline],
    avroSerdes.serdeFrom[CountFlight],
    avroSerdes.serdeFrom[CountAirline],
    avroSerdes.serdeFrom[CodeAirlineList]
  )

  val topologies = Streams.buildTopology(config, kafkaStreamsOptions)
  logger.debug(s"Built topology: ${topologies.head.describe}")
  logger.debug(s"Built topology: ${topologies(1).describe}")

  val streams = topologies.zipWithIndex.map {
    case (topology, index) =>
      val kafkaStreamsProperties = Streams.streamProperties(config.kafka)
      kafkaStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, s"${config.kafka.applicationId}-$index")
      logger.debug(s"Using streams properties: $kafkaStreamsProperties")
      new KafkaStreams(topology, kafkaStreamsProperties)
  }
  val latch = new CountDownLatch(streams.size)

  streams.foreach(stream => {
    sys.addShutdownHook {
      logger.info("Shutting down")

      if (stream.state.isRunning) {
        val shutdownTimeout = 1.second
        stream.close(duration2JavaDuration(shutdownTimeout))
      }

      latch.countDown()
    }

    stream.setUncaughtExceptionHandler((_: Thread, e: Throwable) => {
      logger.error("Uncaught exception while running streams", e)
      System.exit(0)
    })
  })

  try {
    logger.info("Starting streams")
    streams.foreach(stream => stream.start())
    latch.await()
  } catch {
    case e: Throwable =>
      logger.error("Exception starting streams", e)
      System.exit(1)
  }
}
