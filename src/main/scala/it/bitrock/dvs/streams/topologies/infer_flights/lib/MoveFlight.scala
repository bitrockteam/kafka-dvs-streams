package it.bitrock.dvs.streams.topologies.infer_flights.lib

import java.time.Instant

import it.bitrock.dvs.model.avro.FlightRaw


object MoveFlight extends ((FlightRaw, Long) => FlightRaw) {
  override def apply(flight: FlightRaw, intervalMs: Long): FlightRaw = {
    val intervalHours = intervalMs / 3600000.0
    val altitudeKm = flight.geography.altitude / 1000.0
    val delta = PositionDelta(flight.geography.direction, altitudeKm, flight.speed.horizontal, intervalHours)
    val newPosition = Vector(flight.geography.longitude, flight.geography.latitude)
      .zip(delta)
      .map(pair => pair._1 + pair._2)
    val newFlightUpdated = Instant.ofEpochMilli(flight.system.updated.toEpochMilli + intervalMs)
    flight.copy(
      // TODO add accuracy += 1 here
      geography = flight.geography.copy(longitude = newPosition(0), latitude = newPosition(1)),
      system = flight.system.copy(updated = newFlightUpdated)
    )
  }
}
