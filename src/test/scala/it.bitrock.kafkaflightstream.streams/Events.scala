package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model._

trait Events {

  final val FlightIataCode = "EI35Y"
  final val FlightIcaoCode = "EIN35Y"
  final val Updated        = "12345"
  final val StatusEnRoute  = "en-route"

  final val ParamsEuropeanAirport1 = AirportParams("ZRH", "CH")
  final val ParamsEuropeanAirport2 = AirportParams("MXP", "IT")
  final val ParamsEuropeanAirport3 = AirportParams("MXR", "UA")
  final val ParamsEuropeanAirport4 = AirportParams("NAP", "IT")
  final val ParamsEuropeanAirport5 = AirportParams("NCE", "FR")
  final val ParamsEuropeanAirport6 = AirportParams("AEI", "ES")
  final val ParamsEuropeanAirport7 = AirportParams("BVG", "NO")
  final val ParamsForeignAirport   = AirportParams("ORD", "US")
  final val ParamsAirline1         = AirlineParams("SWR", "SWISS")
  final val ParamsAirline2         = AirlineParams("ESQ", "Europ Star Aircraft")
  final val ParamsAirline3         = AirlineParams("FFI", "Infinit Air")
  final val ParamsAirline4         = AirlineParams("GEC", "Lufthansa Cargo")
  final val ParamsAirline5         = AirlineParams("LLP", "Small Planet Airlines Polska")
  final val ParamsAirline6         = AirlineParams("DRU", "ALROSA Mirny Air Enterprise")
  final val ParamsAirline7         = AirlineParams("KAT", "Kato Airline")
  final val ParamsAirplane         = AirplaneParams("HBJHA")
  final val ParamsEuropeanFlight = FlightParams(
    ParamsEuropeanAirport1.iataCode,
    ParamsEuropeanAirport2.iataCode,
    ParamsAirline1.icaoCode,
    ParamsAirplane.numberRegistration
  )
  final val ParamsForeignFlight = FlightParams(
    ParamsEuropeanAirport1.iataCode,
    ParamsForeignAirport.iataCode,
    ParamsAirline1.icaoCode,
    ParamsAirplane.numberRegistration
  )

  final val SpeedArray = Array(123.45, 800, 958.37, 1216.67, 750, 987, 675.45, 900, 1000, 345.89)
  final val CodeAirlineArray = Array(
    ParamsAirline1.icaoCode,
    ParamsAirline2.icaoCode,
    ParamsAirline3.icaoCode,
    ParamsAirline4.icaoCode,
    ParamsAirline1.icaoCode,
    ParamsAirline3.icaoCode,
    ParamsAirline2.icaoCode,
    ParamsAirline5.icaoCode,
    ParamsAirline1.icaoCode,
    ParamsAirline1.icaoCode
  )

  final val EuropeanAirport1: AirportRaw   = buildAirportRaw(ParamsEuropeanAirport1)
  final val EuropeanAirport2: AirportRaw   = buildAirportRaw(ParamsEuropeanAirport2)
  final val EuropeanAirport3: AirportRaw   = buildAirportRaw(ParamsEuropeanAirport3)
  final val EuropeanAirport4: AirportRaw   = buildAirportRaw(ParamsEuropeanAirport4)
  final val EuropeanAirport5: AirportRaw   = buildAirportRaw(ParamsEuropeanAirport5)
  final val EuropeanAirport6: AirportRaw   = buildAirportRaw(ParamsEuropeanAirport6)
  final val EuropeanAirport7: AirportRaw   = buildAirportRaw(ParamsEuropeanAirport7)
  final val ForeignAirport: AirportRaw     = buildAirportRaw(ParamsForeignAirport)
  final val AirlineEvent1: AirlineRaw      = buildAirlineRaw(ParamsAirline1)
  final val AirlineEvent2: AirlineRaw      = buildAirlineRaw(ParamsAirline2)
  final val AirlineEvent3: AirlineRaw      = buildAirlineRaw(ParamsAirline3)
  final val AirlineEvent4: AirlineRaw      = buildAirlineRaw(ParamsAirline4)
  final val AirlineEvent5: AirlineRaw      = buildAirlineRaw(ParamsAirline5)
  final val AirlineEvent6: AirlineRaw      = buildAirlineRaw(ParamsAirline6)
  final val AirlineEvent7: AirlineRaw      = buildAirlineRaw(ParamsAirline7)
  final val AirplaneEvent: AirplaneRaw     = buildAirplaneRaw(ParamsAirplane)
  final val EuropeanFlightEvent: FlightRaw = buildFlightRaw(ParamsEuropeanFlight)
  final val ForeignFlightEvent: FlightRaw  = buildFlightRaw(ParamsForeignFlight)

  final val ExpectedEuropeanFlightEnrichedEvent = FlightEnrichedEvent(
    FlightIataCode,
    FlightIcaoCode,
    GeographyInfo(0, 0, 0, 0),
    0,
    AirportInfo(ParamsEuropeanAirport1.iataCode, "", "", ParamsEuropeanAirport1.codeCountry),
    AirportInfo(ParamsEuropeanAirport2.iataCode, "", "", ParamsEuropeanAirport2.codeCountry),
    AirlineInfo(ParamsAirline1.icaoCode, ParamsAirline1.nameAirline, ""),
    Some(AirplaneInfo(ParamsAirplane.numberRegistration, "", "")),
    StatusEnRoute,
    Updated
  )
  final val ExpectedFlightReceivedList = 0 to 9 map { key =>
    ExpectedEuropeanFlightEnrichedEvent.copy(iataNumber = key.toString, icaoNumber = key.toString)
  }
  final val ExpectedTopArrivalResult = TopArrivalAirportList(
    List(
      Airport(ParamsEuropeanAirport7.iataCode, 11),
      Airport(ParamsEuropeanAirport3.iataCode, 9),
      Airport(ParamsEuropeanAirport2.iataCode, 6),
      Airport(ParamsEuropeanAirport6.iataCode, 5),
      Airport(ParamsEuropeanAirport5.iataCode, 4)
    )
  )
  final val ExpectedTopDepartureResult = TopDepartureAirportList(
    List(
      Airport(ParamsEuropeanAirport7.iataCode, 11),
      Airport(ParamsEuropeanAirport3.iataCode, 9),
      Airport(ParamsEuropeanAirport2.iataCode, 6),
      Airport(ParamsEuropeanAirport6.iataCode, 5),
      Airport(ParamsEuropeanAirport5.iataCode, 4)
    )
  )
  final val ExpectedTopSpeedResult = TopSpeedList(
    Seq(
      SpeedFlight("3", SpeedArray(3)),
      SpeedFlight("8", SpeedArray(8)),
      SpeedFlight("5", SpeedArray(5)),
      SpeedFlight("2", SpeedArray(2)),
      SpeedFlight("7", SpeedArray(7))
    )
  )
  final val ExpectedTopAirlineResult = TopAirlineList(
    Seq(
      Airline(ParamsAirline7.nameAirline, 11),
      Airline(ParamsAirline3.nameAirline, 9),
      Airline(ParamsAirline2.nameAirline, 6),
      Airline(ParamsAirline6.nameAirline, 5),
      Airline(ParamsAirline5.nameAirline, 4)
    )
  )
  final val ExpectedTotalFlightResult  = CountFlightStatus(StatusEnRoute, 10)
  final val ExpectedTotalAirlineResult = CountAirline(5)

  case class AirportParams(iataCode: String, codeCountry: String)
  case class AirlineParams(icaoCode: String, nameAirline: String)
  case class AirplaneParams(numberRegistration: String)
  case class FlightParams(departureAirportCode: String, arrivalAirportCode: String, airlineCode: String, airplaneCode: String)

  private def buildAirportRaw(params: AirportParams)   = AirportRaw("", "", params.iataCode, "", "", "", params.codeCountry, "")
  private def buildAirlineRaw(params: AirlineParams)   = AirlineRaw("", params.nameAirline, "", params.icaoCode, "", "", "", "", "")
  private def buildAirplaneRaw(params: AirplaneParams) = AirplaneRaw(params.numberRegistration, "", "", "", "", "", "", "", "", "", "")
  private def buildFlightRaw(params: FlightParams) =
    FlightRaw(
      Geography(0, 0, 0, 0),
      Speed(0, 0),
      CommonCode(params.departureAirportCode, ""),
      CommonCode(params.arrivalAirportCode, ""),
      Aircraft(params.airplaneCode, "", "", ""),
      CommonCode("", params.airlineCode),
      Flight(FlightIataCode, FlightIcaoCode, ""),
      System(Updated, ""),
      StatusEnRoute
    )

}
