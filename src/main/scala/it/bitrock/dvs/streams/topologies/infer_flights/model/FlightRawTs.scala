package it.bitrock.dvs.streams.topologies.infer_flights.model

import it.bitrock.dvs.model.avro.FlightRaw

case class FlightRawTs(timestamp: Long, flightRaw: FlightRaw)
