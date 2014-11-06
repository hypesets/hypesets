package hll

import com.twitter.algebird.HyperLogLogMonoid
import akka.actor.Actor
import com.twitter.algebird.HLL
import akka.actor.ActorLogging

object HLLSet {
  case class Add(value: String)
  case class EstimatedValue(value: Double, id: String)
  case class Estimate(id: String)
}

class HLLSet extends Actor with ActorLogging {
  val hll = new HyperLogLogMonoid(12)
  var set =  hll.zero

  def addValue(value: String) = {
    val newSet = hll(value.getBytes())

    set = hll.plus(set, newSet)
  }

  def receive = {
    case HLLSet.Add(value) => addValue(value)
    case HLLSet.Estimate(id) => sender ! HLLSet.EstimatedValue(set.estimatedSize, id)
  }
}
