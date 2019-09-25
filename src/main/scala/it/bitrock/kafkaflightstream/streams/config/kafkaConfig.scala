package it.bitrock.kafkaflightstream.streams.config

import java.net.URI

import scala.concurrent.duration.FiniteDuration

final case class KafkaConfig(
    applicationId: String,
    bootstrapServers: String,
    schemaRegistryUrl: URI,
    topology: TopologyConfig
)

final case class TopologyConfig(
    flightRawTopic: String,
    airportRawTopic: String,
    airlineRawTopic: String,
    cityRawTopic: String,
    airplaneRawTopic: String,
    flightReceivedTopic: String,
    topArrivalAirportTopic: String,
    topDepartureAirportTopic: String,
    topSpeedTopic: String,
    topAirlineTopic: String,
    totalFlightTopic: String,
    totalAirlineTopic: String,
    aggregationTimeWindowSize: FiniteDuration,
    commitInterval: FiniteDuration,
    cacheMaxSizeBytes: Long,
    threadsAmount: Int
)
