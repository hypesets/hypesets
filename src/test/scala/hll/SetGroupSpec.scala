package hll

import org.scalatest._
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.testkit.ImplicitSender
import akka.actor.Props

class SetGroupSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def createSetGroup = system.actorOf(Props[SetGroup])

  "Estimation of empty empty set group" in {
    val setGroup = createSetGroup

    setGroup ! SetGroup.Estimate

    expectMsg(SetGroup.EstimationResult(Map()))
  }

  "Estimation of multiple set groups" in {
    val setGroup = createSetGroup

    setGroup ! SetGroup.Add("setA", "a")
    setGroup ! SetGroup.Add("setB", "a")
    setGroup ! SetGroup.Add("setA", "b")

    setGroup ! SetGroup.Estimate

    def expected(map: Map[String, Double]) =
      map.get("setA").get == 2.0 && map.get("setB").get == 1.0

    expectMsgPF() {
      case SetGroup.EstimationResult(result) if expected(result) => true
    }
  }

  "Return already estimating when estimation in progress (may fail randomly)" in {
    val setGroup = createSetGroup
    
    for (i <- 0 to 1000) setGroup ! SetGroup.Add(s"set$i", i.toString)
    
    setGroup ! SetGroup.Estimate
    setGroup ! SetGroup.Estimate
    
    expectMsg(SetGroup.AlreadyEstimating)
  }

}