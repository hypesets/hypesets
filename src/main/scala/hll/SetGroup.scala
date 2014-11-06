package hll
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorLogging
import akka.actor.actorRef2Scala

object SetGroup {
  case class Add(setKey: String, element: String)
  case class EstimationState(estimated: Map[String, Double], toEstimate: Int, recipient: Option[ActorRef])
  case object Estimate
  case object AlreadyEstimating
  case class EstimationResult(result: Map[String, Double])
}

class SetGroup extends Actor with ActorLogging {
  var sets = Map[String, ActorRef]()
  var estimationState = SetGroup.EstimationState(Map(), 0, None)
  
  def estimate(sender: ActorRef) = estimationState.recipient match {
    case Some(_) => sender ! SetGroup.AlreadyEstimating
    case None if sets.size == 0 => sender ! SetGroup.EstimationResult(Map())
    case None => {
      estimationState = SetGroup.EstimationState(Map(), sets.size, Some(sender))
      
      for ((key, actor) <- sets) actor ! HLLSet.Estimate(key)
    }
  }
  
  def getSet(key: String): ActorRef = sets.get(key) match {
    case Some(actor) => actor
    case None => {
      val actor = context.actorOf(Props[HLLSet], s"set-$key")

      sets = sets + (key -> actor)
      actor
    }
  }
  
  def processEstimation(key: String, value: Double) = {
    val estimated = estimationState.estimated + (key -> value)

    estimationState = estimationState match {
      case SetGroup.EstimationState(_, 1, Some(recipient)) => {
        recipient ! SetGroup.EstimationResult(estimated)

        SetGroup.EstimationState(Map(), 0, None)
      }
      case SetGroup.EstimationState(_, toEstimate, recipient) =>
        SetGroup.EstimationState(estimated, toEstimate - 1, recipient)
    }
  }
  
  def receive = {
    case SetGroup.Add(setKey, element) => {
      val actor = getSet(setKey)
      
      actor ! HLLSet.Add(element)
    }
    case SetGroup.Estimate => estimate(sender)
    case HLLSet.EstimatedValue(value, key) => processEstimation(key, value)
  }
}
