package it.bitrock.dvs.streams.topologies.infer_flights.lib

object PositionDelta extends ((Double, Double, Double, Double) => Vector[Double]) {
  val avgEarthRadius = 6371.0088

  override def apply(direction: Double, altitude: Double, speed: Double, timeInterval: Double): Vector[Double] = {
    val multiplier = 360 * timeInterval * speed / (2 * Math.PI * (avgEarthRadius + altitude))
    Vector(Math.sin(direction), Math.cos(direction)).map(multiplier * _)
  }
}
