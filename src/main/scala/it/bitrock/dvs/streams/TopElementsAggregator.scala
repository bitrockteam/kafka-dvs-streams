package it.bitrock.dvs.streams

import it.bitrock.dvs.model.avro.{
  TopAirline,
  TopAirlineList,
  TopAirport,
  TopArrivalAirportList,
  TopDepartureAirportList,
  TopSpeed,
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
    extends TopElementsAggregator[TopArrivalAirportList, TopAirport, String] {

  override lazy val element2OrderingTypes: TopAirport => (Double, String) =
    // Note: Long comparison is reversed!
    x => (-x.eventCount, x.airportCode)

  override def initializer: TopArrivalAirportList = TopArrivalAirportList()

  override def removeElementFromTopList(agg: TopArrivalAirportList, element: TopAirport): List[TopAirport] =
    agg.elements.filterNot(_.airportCode == element.airportCode).toList

  override def updateTopList(agg: TopArrivalAirportList, newTopList: List[TopAirport]): TopArrivalAirportList =
    agg.copy(elements = newTopList)

}

final class TopDepartureAirportAggregator(override val topAmount: Int)
    extends TopElementsAggregator[TopDepartureAirportList, TopAirport, String] {

  override lazy val element2OrderingTypes: TopAirport => (Double, String) =
    // Note: Long comparison is reversed!
    x => (-x.eventCount, x.airportCode)

  override def initializer: TopDepartureAirportList = TopDepartureAirportList()

  override def removeElementFromTopList(agg: TopDepartureAirportList, element: TopAirport): List[TopAirport] =
    agg.elements.filterNot(_.airportCode == element.airportCode).toList

  override def updateTopList(agg: TopDepartureAirportList, newTopList: List[TopAirport]): TopDepartureAirportList =
    agg.copy(elements = newTopList)

}

final class TopSpeedFlightAggregator(override val topAmount: Int) extends TopElementsAggregator[TopSpeedList, TopSpeed, String] {

  override lazy val element2OrderingTypes: TopSpeed => (Double, String) =
    // Note: Long comparison is reversed!
    x => (-x.speed, x.flightCode)

  override def initializer: TopSpeedList = TopSpeedList()

  override def removeElementFromTopList(agg: TopSpeedList, element: TopSpeed): List[TopSpeed] =
    agg.elements.filterNot(_.flightCode == element.flightCode).toList

  override def updateTopList(agg: TopSpeedList, newTopList: List[TopSpeed]): TopSpeedList =
    agg.copy(elements = newTopList)

}

final class TopAirlineAggregator(override val topAmount: Int) extends TopElementsAggregator[TopAirlineList, TopAirline, String] {

  override lazy val element2OrderingTypes: TopAirline => (Double, String) =
    // Note: Long comparison is reversed!
    x => (-x.eventCount, x.airlineName)

  override def initializer: TopAirlineList = TopAirlineList()

  override def removeElementFromTopList(agg: TopAirlineList, element: TopAirline): List[TopAirline] =
    agg.elements.filterNot(_.airlineName == element.airlineName).toList

  override def updateTopList(agg: TopAirlineList, newTopList: List[TopAirline]): TopAirlineList =
    agg.copy(elements = newTopList)

}
