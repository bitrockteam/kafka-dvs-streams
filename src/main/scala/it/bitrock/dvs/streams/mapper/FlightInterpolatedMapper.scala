package it.bitrock.dvs.streams.mapper

import java.time.Instant

import it.bitrock.dvs.model.avro.{FlightInterpolated, FlightReceived, GeographyInfo}

object FlightInterpolatedMapper {

  def interpolatedWith(geography: GeographyInfo, time: Instant)(flightReceived: FlightReceived): FlightInterpolated =
    FlightInterpolated(
      iataNumber = flightReceived.iataNumber,
      icaoNumber = flightReceived.icaoNumber,
      geography = geography,
      speed = flightReceived.speed,
      departureAirport = flightReceived.departureAirport,
      arrivalAirport = flightReceived.arrivalAirport,
      airline = flightReceived.airline,
      airplane = flightReceived.airplane,
      status = flightReceived.status,
      updated = flightReceived.updated,
      interpolatedUntil = time
    )
}
