package hll
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorLogging
import akka.actor.actorRef2Scala

object SetGroup {
  case class Add(setKey: String, element: String)
  case class EstimationState(estimated: Map[String, Double], toEstimate: Int, recipient: Option[ActorRef])
  case class Estimate(selector: String)
  case object AlreadyEstimating
  case class EstimationResult(result: Map[String, Double])
}

class SetGroup extends Actor with ActorLogging {
  
  case object Tick
  
  var sets = Map[String, SetReference]()
  var estimationState = SetGroup.EstimationState(Map(), 0, None)
  var ticks = 0L

  def estimate(sender: ActorRef, selector: String) = estimationState.recipient match {
    case Some(_) => sender ! SetGroup.AlreadyEstimating
    case None if sets.size == 0 => sender ! SetGroup.EstimationResult(Map())
    case None => {
      val pattern = selector r
      
      def keyFilter(key: String) = pattern.findFirstIn(key) match {
        case Some(_) => true
        case None => false
      }
      
      val pending = sets.filterKeys(keyFilter)
      val pendingSize = pending.size
      
      if ( pending.size == 0) {
        sender ! SetGroup.EstimationResult(Map())
      } else {
        estimationState = SetGroup.EstimationState(Map(), pending.size, Some(sender))

        for ((key, reference) <- sets.filterKeys(keyFilter)) {
          reference.actor ! HLLSet.Estimate(key)
        }
      }
    }
  }

  def getSet(key: String): ActorRef = sets.get(key) match {
    case Some(reference) => reference.actor
    case None => {
      val actor = context.actorOf(Props[HLLSet], s"set-$key")
      val reference = SetReference(ticks, actor)

      sets = sets + (key -> reference)
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
    case SetGroup.Estimate(selector) => estimate(sender, selector)
    case HLLSet.EstimatedValue(value, key) => processEstimation(key, value)
    case Tick => ticks += 1
  }
}
