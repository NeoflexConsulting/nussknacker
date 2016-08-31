package pl.touk.esp.engine.api

sealed trait PartReference

sealed trait EndingReference extends PartReference {
  def nodeId: String
}

case class NextPartReference(id: String) extends PartReference
case class DeadEndReference(nodeId: String) extends EndingReference
case class EndReference(nodeId: String) extends EndingReference
