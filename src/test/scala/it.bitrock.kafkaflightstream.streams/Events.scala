package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model._

trait Events {

  val EuropeanAirport1 = AirportRaw(
    "1",
    "Zurigo",
    "ZRH",
    "1234535",
    "13244345",
    "Swiss",
    "CH",
    "CH"
  )

  val EuropeanAirport2 = AirportRaw(
    "5231",
    "Milano Malpensa",
    "MXP",
    "45.627403",
    "8.71237",
    "Italy",
    "IT",
    "MIL"
  )

  val ForeignAirport1 = AirportRaw(
    "2",
    "Chicago O'hare International",
    "ORD",
    "41.976913",
    "-87.90488",
    "United States",
    "US",
    "CHI"
  )

  val AirlineEvent = AirlineRaw(
    "79",
    "SWISS",
    "LX",
    "SWR",
    "SWISS",
    "active",
    "67",
    "Switzerland",
    "CH"
  )

  val AirplaneEvent = AirplaneRaw(
    "HB-JHA",
    "Airbus A330/A340",
    "A330",
    "A330-343(E)",
    "4B187A",
    "A333",
    "",
    "",
    "JET",
    "8",
    "active"
  )

  val EuropeanFlightEvent: FlightRaw = FlightRaw(
    Geography(
      49.2655,
      -1.9623,
      9753.6,
      282.76
    ),
    Speed(
      805.14,
      0
    ),
    CommonCode(
      "ZRH",
      "LSZH"
    ),
    CommonCode(
      "MXP",
      "LIMC"
    ),
    Aircraft(
      "HBJHA",
      "A333",
      "",
      "A333"
    ),
    CommonCode(
      "LX",
      "SWR"
    ),
    Flight(
      "LX6U",
      "SWR6U",
      "6U"
    ),
    System(
      "1567415880",
      "3061"
    ),
    "en-route"
  )

  val ForeignFlightEvent: FlightRaw = FlightRaw(
    Geography(
      49.2655,
      -1.9623,
      9753.6,
      282.76
    ),
    Speed(
      805.14,
      0
    ),
    CommonCode(
      "ZRH",
      "LSZH"
    ),
    CommonCode(
      "ORD",
      "KORD"
    ),
    Aircraft(
      "HBJHA",
      "A333",
      "",
      "A333"
    ),
    CommonCode(
      "LX",
      "SWR"
    ),
    Flight(
      "LX6U",
      "SWR6U",
      "6U"
    ),
    System(
      "1567415880",
      "3061"
    ),
    "en-route"
  )

  val ExpectedEuropeanFlightEnrichedEvent: FlightEnrichedEvent = FlightEnrichedEvent(
    GeographyInfo(49.2655, -1.9623, 9753.6, 282.76),
    805.14,
    AirportInfo("ZRH", "Zurigo", "Swiss", "CH"),
    AirportInfo("MXP", "Milano Malpensa", "Italy", "IT"),
    AirlineInfo("SWISS", "67"),
    Some(AirplaneInfo("Airbus A330/A340", "4B187A")),
    "en-route"
  )

  val ExpectedFlightEnrichedEventWithoutAirplaneinfo: FlightEnrichedEvent = ExpectedEuropeanFlightEnrichedEvent.copy(airplane = None)

}
