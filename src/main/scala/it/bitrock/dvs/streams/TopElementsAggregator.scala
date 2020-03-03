package it.bitrock.dvs.streams

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams.TopElementsAggregator.ElementsOrderingTypes

trait TopAggregatorDescriptor[A, E] {
  def sortBy: E => ElementsOrderingTypes
  def fromElements: List[E] => A
  def removeElement(e: E, a: A): List[E]
}

object TopAggregatorDescriptor {
  implicit val topArrivalAirportDescriptor: TopAggregatorDescriptor[TopArrivalAirportList, TopAirport] =
    new TopAggregatorDescriptor[TopArrivalAirportList, TopAirport] {
      override def sortBy: TopAirport => (Double, String)                  = x => (-x.eventCount, x.code)
      override def fromElements: List[TopAirport] => TopArrivalAirportList = TopArrivalAirportList.apply
      override def removeElement(e: TopAirport, a: TopArrivalAirportList): List[TopAirport] =
        a.elements.filterNot(_.code == e.code).toList
    }

  implicit val topDepartureAirportDescriptor: TopAggregatorDescriptor[TopDepartureAirportList, TopAirport] =
    new TopAggregatorDescriptor[TopDepartureAirportList, TopAirport] {
      override def sortBy: TopAirport => (Double, String)                    = x => (-x.eventCount, x.code)
      override def fromElements: List[TopAirport] => TopDepartureAirportList = TopDepartureAirportList.apply
      override def removeElement(e: TopAirport, a: TopDepartureAirportList): List[TopAirport] =
        a.elements.filterNot(_.code == e.code).toList
    }

  implicit val topSpeedDescriptor: TopAggregatorDescriptor[TopSpeedList, TopSpeed] =
    new TopAggregatorDescriptor[TopSpeedList, TopSpeed] {
      override def sortBy: TopSpeed => (Double, String)         = x => (-x.speed, x.flightCode)
      override def fromElements: List[TopSpeed] => TopSpeedList = TopSpeedList.apply
      override def removeElement(e: TopSpeed, a: TopSpeedList): List[TopSpeed] =
        a.elements.filterNot(_.flightCode == e.flightCode).toList
    }

  implicit val topAirlineDescriptor: TopAggregatorDescriptor[TopAirlineList, TopAirline] =
    new TopAggregatorDescriptor[TopAirlineList, TopAirline] {
      override def sortBy: TopAirline => (Double, String)           = x => (-x.eventCount, x.name)
      override def fromElements: List[TopAirline] => TopAirlineList = TopAirlineList.apply
      override def removeElement(e: TopAirline, a: TopAirlineList): List[TopAirline] =
        a.elements.filterNot(_.name == e.name).toList
    }
}

trait TopElementsAggregator[K, E, A] {
  def initializer: A
  def adder: (K, E, A) => A
  def subtractor: (K, E, A) => A
}

object TopElementsAggregator {
  type ElementsOrderingTypes = (Double, String)

  def apply[K, E, A](
      topAmount: Int
  )(implicit descriptor: TopAggregatorDescriptor[A, E]): TopElementsAggregator[K, E, A] =
    new TopElementsAggregator[K, E, A] {
      override def initializer: A = descriptor.fromElements(List.empty)

      override def adder: (K, E, A) => A =
        (_, element, agg) => {
          val updatedList = descriptor.removeElement(element, agg) :+ element
          descriptor.fromElements(updatedList.sortBy(descriptor.sortBy).take(topAmount))
        }

      override def subtractor: (K, E, A) => A =
        (_, element, agg) => descriptor.fromElements(descriptor.removeElement(element, agg))
    }
}
