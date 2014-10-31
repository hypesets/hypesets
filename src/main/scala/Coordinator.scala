import com.twitter.algebird.HLL
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorLogging

object Coordinator {
  case class Add(key: String, value: String)
  case class EstimationState(estimated: Map[String, Double], toEstimate: Int, recipient: Option[ActorRef])
  case object Estimate
  case object AlreadyEstimating
  case class EstimationResult(result: Map[String, Double])
}

class Coordinator extends Actor with ActorLogging {
  var sets = Map[String, ActorRef]()
  var estimationState = Coordinator.EstimationState(Map(), 0, None)
  
  def estimate(sender: ActorRef) = estimationState.recipient match {
    case Some(_) => sender ! Coordinator.AlreadyEstimating
    case None if sets.size == 0 => Coordinator.EstimationResult(Map())
    case None => {
      estimationState = Coordinator.EstimationState(Map(), sets.size, Some(sender))
      
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
      case Coordinator.EstimationState(_, 1, Some(recipient)) => {
        recipient ! Coordinator.EstimationResult(estimated)

        Coordinator.EstimationState(Map(), 0, None)
      }
      case Coordinator.EstimationState(_, toEstimate, recipient) =>
        Coordinator.EstimationState(estimated, toEstimate - 1, recipient)
    }
  }
  
  def receive = {
    case Coordinator.Add(key, value) => {
      val actor = getSet(key)
      
      actor ! HLLSet.Add(value)
    }
    case Coordinator.Estimate => estimate(sender)
    case HLLSet.EstimatedValue(value, key) => processEstimation(key, value)
  }
}
