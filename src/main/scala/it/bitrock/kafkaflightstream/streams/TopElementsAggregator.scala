package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model.{Airport, TopAirportList}

import scala.collection.SortedSet

trait TopElementsAggregator[A, E, K] {

  type ElementsOrderingTypes = (Long, String)

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

final class TopAirportAggregator(override val topAmount: Int) extends TopElementsAggregator[TopAirportList, Airport, String] {

  override lazy val element2OrderingTypes: Airport => (Long, String) =
    // Note: Long comparison is reversed!
    x => (-x.eventCount, x.airportCode)

  override def initializer: TopAirportList = TopAirportList()

  override def removeElementFromTopList(agg: TopAirportList, element: Airport): List[Airport] =
    agg.elements.filterNot(_.airportCode == element.airportCode).toList

  override def updateTopList(agg: TopAirportList, newTopList: List[Airport]): TopAirportList =
    agg.copy(elements = newTopList)

}
