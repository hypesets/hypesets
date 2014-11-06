package hll

import org.scalatest._
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.actor.actorRef2Scala
import hll.HLLSet

class HLLSetSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def createHLLSet = system.actorOf(Props[HLLSet])

  "Empty set estimation" in {
    val hllSet = createHLLSet

    hllSet ! HLLSet.Estimate("set key")

    expectMsg(HLLSet.EstimatedValue(0.0, "set key"))
  }

  "Set with 2 elements" in {
    val hllSet = createHLLSet

    for (element <- List("a", "b", "a")) hllSet ! HLLSet.Add(element)
    hllSet ! HLLSet.Estimate("set key")

    expectMsg(HLLSet.EstimatedValue(2.0, "set key"))
  }

  "Set with many elements" in {
    val hllSet = createHLLSet

    for (element <- 0 until 1000) hllSet ! HLLSet.Add(element.toString)
    hllSet ! HLLSet.Estimate("set key")

    expectMsgPF() {
      case HLLSet.EstimatedValue(value, "set key") if (value > 990 && value < 1010) => true 
    }
  }
}