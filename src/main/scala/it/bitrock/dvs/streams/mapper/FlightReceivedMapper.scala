package it.bitrock.dvs.streams.mapper

import it.bitrock.dvs.model.avro.{
  AirlineInfo,
  AirlineRaw,
  AirplaneInfo,
  AirplaneRaw,
  AirportInfo,
  AirportRaw,
  FlightRaw,
  FlightReceived,
  FlightWithAirline,
  FlightWithAllAirportInfo,
  FlightWithDepartureAirportInfo,
  GeographyInfo
}

object FlightReceivedMapper {

  implicit class FlightRawDecorator(flightRaw: FlightRaw) {
    def toFlightWithDepartureAirportInfo(airportRaw: AirportRaw): FlightWithDepartureAirportInfo =
      FlightWithDepartureAirportInfo(
        flightRaw.flight.iataNumber,
        flightRaw.flight.icaoNumber,
        GeographyInfo(
          flightRaw.geography.latitude,
          flightRaw.geography.longitude,
          flightRaw.geography.altitude,
          flightRaw.geography.direction
        ),
        flightRaw.speed.horizontal,
        AirportInfo(
          airportRaw.codeIataAirport,
          airportRaw.nameAirport,
          airportRaw.nameCountry,
          airportRaw.codeIso2Country,
          airportRaw.timezone,
          airportRaw.gmt
        ),
        flightRaw.arrival.iataCode,
        flightRaw.airline.icaoCode,
        flightRaw.aircraft.regNumber,
        flightRaw.status,
        flightRaw.system.updated
      )
  }

  implicit class FlightWithDepartureAirportInfoDecorator(flightWithDepartureAirportInfo: FlightWithDepartureAirportInfo) {
    def toFlightWithAllAirportInfo(airportRaw: AirportRaw): FlightWithAllAirportInfo =
      FlightWithAllAirportInfo(
        flightWithDepartureAirportInfo.iataNumber,
        flightWithDepartureAirportInfo.icaoNumber,
        flightWithDepartureAirportInfo.geography,
        flightWithDepartureAirportInfo.speed,
        flightWithDepartureAirportInfo.airportDeparture,
        AirportInfo(
          airportRaw.codeIataAirport,
          airportRaw.nameAirport,
          airportRaw.nameCountry,
          airportRaw.codeIso2Country,
          airportRaw.timezone,
          airportRaw.gmt
        ),
        flightWithDepartureAirportInfo.airlineCode,
        flightWithDepartureAirportInfo.airplaneRegNumber,
        flightWithDepartureAirportInfo.status,
        flightWithDepartureAirportInfo.updated
      )
  }

  implicit class FlightWithAllAirportInfoDecorator(flightWithAllAirportInfo: FlightWithAllAirportInfo) {
    def toFlightWithAirline(airlineRaw: AirlineRaw): FlightWithAirline =
      FlightWithAirline(
        flightWithAllAirportInfo.iataNumber,
        flightWithAllAirportInfo.icaoNumber,
        flightWithAllAirportInfo.geography,
        flightWithAllAirportInfo.speed,
        flightWithAllAirportInfo.airportDeparture,
        flightWithAllAirportInfo.airportArrival,
        AirlineInfo(airlineRaw.codeIcaoAirline, airlineRaw.nameAirline, airlineRaw.sizeAirline),
        flightWithAllAirportInfo.airplaneRegNumber,
        flightWithAllAirportInfo.status,
        flightWithAllAirportInfo.updated
      )
  }

  implicit class FlightWithAirlineDecorator(flightWithAirline: FlightWithAirline) {
    def toFlightReceived(airplaneRaw: AirplaneRaw): FlightReceived =
      FlightReceived(
        flightWithAirline.iataNumber,
        flightWithAirline.icaoNumber,
        flightWithAirline.geography,
        flightWithAirline.speed,
        flightWithAirline.airportDeparture,
        flightWithAirline.airportArrival,
        flightWithAirline.airline,
        airplaneInfoOrDefault(airplaneRaw),
        flightWithAirline.status,
        flightWithAirline.updated
      )

    private def airplaneInfoOrDefault(airplaneRaw: AirplaneRaw): AirplaneInfo =
      Option(airplaneRaw)
        .map(airplane => AirplaneInfo(airplane.numberRegistration, airplane.productionLine, airplane.modelCode))
        .getOrElse(AirplaneInfo("N/A", "N/A", "N/A"))
  }

}
