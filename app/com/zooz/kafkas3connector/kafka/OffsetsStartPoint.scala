package com.zooz.kafkas3connector.kafka

import enumeratum._

sealed trait OffsetsStartPoint extends EnumEntry

/** An enum representing a default starting offset for a kafka partition */
object OffsetsStartPoint extends Enum[OffsetsStartPoint] {
  val values = findValues
  
  case object earliest extends OffsetsStartPoint  // scalastyle:ignore
  case object latest extends OffsetsStartPoint    // scalastyle:ignore
}
