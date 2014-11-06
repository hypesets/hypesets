package hll

import akka.actor.Actor
import scala.concurrent.duration._
import akka.actor.Props

object TestActor {
  case object Stop
  case object Estimate
}

class TestActor extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  
  val testSet = context.actorOf(Props[SetGroup], "consumer")

  override def preStart(): Unit = {
    val values = List(("a", "a"), ("b", "a"), ("a", "a"), ("b", "b"))
    
    for ((key, value) <- values) {
      testSet ! SetGroup.Add(key, value)
      testSet ! SetGroup.Estimate
    }
    
    context.system.scheduler.scheduleOnce(1.second, self, TestActor.Estimate)
    context.system.scheduler.scheduleOnce(2.second, self, TestActor.Stop)
  }
  
  def receive = {
    case TestActor.Estimate => testSet ! SetGroup.Estimate
    case TestActor.Stop => context.stop(self)
    case SetGroup.EstimationResult(result) => println(result)
  }
}
