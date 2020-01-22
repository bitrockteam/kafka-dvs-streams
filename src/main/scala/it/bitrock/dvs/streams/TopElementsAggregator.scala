package it.bitrock.dvs.streams

import it.bitrock.dvs.model.avro.{
  Airline,
  Airport,
  SpeedFlight,
  TopAirlineList,
  TopArrivalAirportList,
  TopDepartureAirportList,
  TopSpeedList
}

import scala.collection.SortedSet

trait TopElementsAggregator[A, E, K] {

  type ElementsOrderingTypes = (Double, String)

  implicit val ordering: Ordering[E] = Ordering[ElementsOrderingTypes].on[E](element2OrderingTypes)

  def element2OrderingTypes: E => ElementsOrderingTypes

  def topAmount: Int

  def initializer: A

  def removeElementFromTopList(agg: A, element: E): List[E]

  def updateTopList(agg: A, newTopList: List[E]): A

  val adder: (K, E, A) => A = (_, element, agg) => {
    // Remove old element from the top list, if present
    val updatedList       = removeElementFromTopList(agg, element) :+ element
    val sortedTopElements = SortedSet(updatedList: _*).take(topAmount)
    updateTopList(agg, sortedTopElements.toList)
  }

  val subtractor: (K, E, A) => A = (_, element, agg) => {
    val purgedList = removeElementFromTopList(agg, element)
    updateTopList(agg, purgedList)
  }

}

final class TopArrivalAirportAggregator(override val topAmount: Int)
    extends TopElementsAggregator[TopArrivalAirportList, Airport, String] {

  override lazy val element2OrderingTypes: Airport => (Double, String) =
    // Note: Long comparison is reversed!
    x => (-x.eventCount, x.airportCode)

  override def initializer: TopArrivalAirportList = TopArrivalAirportList()

  override def removeElementFromTopList(agg: TopArrivalAirportList, element: Airport): List[Airport] =
    agg.elements.filterNot(_.airportCode == element.airportCode).toList

  override def updateTopList(agg: TopArrivalAirportList, newTopList: List[Airport]): TopArrivalAirportList =
    agg.copy(elements = newTopList)

}

final class TopDepartureAirportAggregator(override val topAmount: Int)
    extends TopElementsAggregator[TopDepartureAirportList, Airport, String] {

  override lazy val element2OrderingTypes: Airport => (Double, String) =
    // Note: Long comparison is reversed!
    x => (-x.eventCount, x.airportCode)

  override def initializer: TopDepartureAirportList = TopDepartureAirportList()

  override def removeElementFromTopList(agg: TopDepartureAirportList, element: Airport): List[Airport] =
    agg.elements.filterNot(_.airportCode == element.airportCode).toList

  override def updateTopList(agg: TopDepartureAirportList, newTopList: List[Airport]): TopDepartureAirportList =
    agg.copy(elements = newTopList)

}

final class TopSpeedFlightAggregator(override val topAmount: Int)
    extends TopElementsAggregator[TopSpeedList, SpeedFlight, String] {

  override lazy val element2OrderingTypes: SpeedFlight => (Double, String) =
    // Note: Long comparison is reversed!
    x => (-x.speed, x.flightCode)

  override def initializer: TopSpeedList = TopSpeedList()

  override def removeElementFromTopList(agg: TopSpeedList, element: SpeedFlight): List[SpeedFlight] =
    agg.elements.filterNot(_.flightCode == element.flightCode).toList

  override def updateTopList(agg: TopSpeedList, newTopList: List[SpeedFlight]): TopSpeedList =
    agg.copy(elements = newTopList)

}

final class TopAirlineAggregator(override val topAmount: Int) extends TopElementsAggregator[TopAirlineList, Airline, String] {

  override lazy val element2OrderingTypes: Airline => (Double, String) =
    // Note: Long comparison is reversed!
    x => (-x.eventCount, x.airlineName)

  override def initializer: TopAirlineList = TopAirlineList()

  override def removeElementFromTopList(agg: TopAirlineList, element: Airline): List[Airline] =
    agg.elements.filterNot(_.airlineName == element.airlineName).toList

  override def updateTopList(agg: TopAirlineList, newTopList: List[Airline]): TopAirlineList =
    agg.copy(elements = newTopList)

}
