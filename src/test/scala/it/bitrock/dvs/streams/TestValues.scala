package it.bitrock.dvs.streams

import java.time.Instant

import it.bitrock.dvs.model.avro._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

trait TestValues {

  final val FlightIataCode = "EI35Y"
  final val FlightIcaoCode = "EIN35Y"
  final val Updated        = Instant.now
  final val StatusEnRoute  = "en-route"

  final val ParamsAirport1 = AirportParams("ZRH", "CH")
  final val ParamsAirport2 = AirportParams("MXP", "IT")
  final val ParamsAirport3 = AirportParams("MXR", "UA")
  final val ParamsAirport4 = AirportParams("NAP", "IT")
  final val ParamsAirport5 = AirportParams("NCE", "FR")
  final val ParamsAirport6 = AirportParams("AEI", "ES")
  final val ParamsAirport7 = AirportParams("BVG", "NO")
  final val ParamsAirline1 = AirlineParams("SWR", "SWISS")
  final val ParamsAirline2 = AirlineParams("ESQ", "Europ Star Aircraft")
  final val ParamsAirline3 = AirlineParams("FFI", "Infinit Air")
  final val ParamsAirline4 = AirlineParams("GEC", "Lufthansa Cargo")
  final val ParamsAirline5 = AirlineParams("LLP", "Small Planet Airlines Polska")
  final val ParamsAirline6 = AirlineParams("DRU", "ALROSA Mirny Air Enterprise")
  final val ParamsAirline7 = AirlineParams("KAT", "Kato Airline")
  final val ParamsAirplane = AirplaneParams("HBJHA")
  final val ParamsFlight = FlightParams(
    ParamsAirport1.iataCode,
    ParamsAirport2.iataCode,
    ParamsAirline1.icaoCode,
    ParamsAirplane.numberRegistration
  )

  final val SpeedArray = Array(123.45, 800, 958.37, 1216.67, 750, 987, 675.45, 900, 1000, 345.89)
  final val ProductionLineArray = Array(
    "Boeing 727",
    "McDonnell Douglas DC",
    "Airbus A318/A319/A32",
    "Boeing 737 NG",
    "",
    "Embraer EMB-190/EMB-",
    "ATR 42/72",
    "Saab 340",
    "Airbus A330/A340",
    "Boeing 737 Classic"
  )
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

  final val AirportEvent1: AirportRaw  = buildAirportRaw(ParamsAirport1)
  final val AirportEvent2: AirportRaw  = buildAirportRaw(ParamsAirport2)
  final val AirportEvent3: AirportRaw  = buildAirportRaw(ParamsAirport3)
  final val AirportEvent4: AirportRaw  = buildAirportRaw(ParamsAirport4)
  final val AirportEvent5: AirportRaw  = buildAirportRaw(ParamsAirport5)
  final val AirportEvent6: AirportRaw  = buildAirportRaw(ParamsAirport6)
  final val AirportEvent7: AirportRaw  = buildAirportRaw(ParamsAirport7)
  final val AirlineEvent1: AirlineRaw  = buildAirlineRaw(ParamsAirline1)
  final val AirlineEvent2: AirlineRaw  = buildAirlineRaw(ParamsAirline2)
  final val AirlineEvent3: AirlineRaw  = buildAirlineRaw(ParamsAirline3)
  final val AirlineEvent4: AirlineRaw  = buildAirlineRaw(ParamsAirline4)
  final val AirlineEvent5: AirlineRaw  = buildAirlineRaw(ParamsAirline5)
  final val AirlineEvent6: AirlineRaw  = buildAirlineRaw(ParamsAirline6)
  final val AirlineEvent7: AirlineRaw  = buildAirlineRaw(ParamsAirline7)
  final val AirplaneEvent: AirplaneRaw = buildAirplaneRaw(ParamsAirplane)
  final val FlightRawEvent: FlightRaw  = buildFlightRaw(ParamsFlight)

  final val FlightReceivedEvent = FlightReceived(
    FlightIataCode,
    FlightIcaoCode,
    GeographyInfo(0, 0, 0, 0),
    0,
    AirportInfo(ParamsAirport1.iataCode, "", "", ParamsAirport1.codeCountry, "", ""),
    AirportInfo(ParamsAirport2.iataCode, "", "", ParamsAirport2.codeCountry, "", ""),
    AirlineInfo(ParamsAirline1.icaoCode, ParamsAirline1.nameAirline, 0),
    AirplaneInfo(ParamsAirplane.numberRegistration, "", ""),
    StatusEnRoute,
    Updated
  )
  final val ExpectedFlightReceivedWithDefaultAirplane = FlightReceivedEvent.copy(airplane = AirplaneInfo("N/A", "N/A", "N/A"))
  final val ExpectedFlightReceivedList =
    (0 to 9).map(key => FlightReceivedEvent.copy(iataNumber = key.toString, icaoNumber = key.toString))
  final val ExpectedTopArrivalResult = TopArrivalAirportList(
    List(
      Airport(ParamsAirport7.iataCode, 11),
      Airport(ParamsAirport3.iataCode, 9),
      Airport(ParamsAirport2.iataCode, 6),
      Airport(ParamsAirport6.iataCode, 5),
      Airport(ParamsAirport5.iataCode, 4)
    )
  )
  final val ExpectedTopDepartureResult = TopDepartureAirportList(
    List(
      Airport(ParamsAirport7.iataCode, 11),
      Airport(ParamsAirport3.iataCode, 9),
      Airport(ParamsAirport2.iataCode, 6),
      Airport(ParamsAirport6.iataCode, 5),
      Airport(ParamsAirport5.iataCode, 4)
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
  final val ExpectedTotalFlightResult  = 10
  final val ExpectedTotalAirlineResult = 5

  case class AirportParams(iataCode: String, codeCountry: String)
  case class AirlineParams(icaoCode: String, nameAirline: String)
  case class AirplaneParams(numberRegistration: String)
  case class FlightParams(departureAirportCode: String, arrivalAirportCode: String, airlineCode: String, airplaneCode: String)

  private def buildAirportRaw(params: AirportParams) =
    AirportRaw(0, "", params.iataCode, 0, 0, "", params.codeCountry, "", "", "")
  private def buildAirlineRaw(params: AirlineParams) = AirlineRaw(0, params.nameAirline, "", params.icaoCode, "", "", 0, "", "")
  private def buildAirplaneRaw(params: AirplaneParams) =
    AirplaneRaw(params.numberRegistration, "", "", "", "", "", "", "", "", "", "")
  private def buildFlightRaw(params: FlightParams) =
    FlightRaw(
      Geography(0, 0, 0, 0),
      Speed(0, 0),
      CommonCode(params.departureAirportCode, ""),
      CommonCode(params.arrivalAirportCode, ""),
      Aircraft(params.airplaneCode, "", "", ""),
      CommonCode("", params.airlineCode),
      Flight(FlightIataCode, FlightIcaoCode, ""),
      System(Updated),
      StatusEnRoute
    )

  @SuppressWarnings(Array("DisableSyntax.null"))
  def dummyFlightReceivedForcingSuppression(topic: String) = new ProducerRecord(
    topic,
    null,
    java.lang.System.currentTimeMillis + 1.minute.toMillis,
    "",
    FlightReceivedEvent
  )

}
