import akka.actor.Actor
import scala.concurrent.duration._

object TestActor {
  case object Stop
}

class TestActor extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(100.millis, self, TestActor.Stop)
  }
  
  def receive = {
    case TestActor.Stop => context.stop(self)
  }
}