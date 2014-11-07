package hll

import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike
import org.scalatest.Matchers
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.actor.Props
import akka.io.Tcp
import akka.util.ByteString
import akka.io.Tcp.Write
import akka.actor.ActorRef

class ConnectionSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
  
  def setGroup = system.actorOf(Props[SetGroup])
  def createConnection(setGroup: ActorRef) = system.actorOf(Props(classOf[Connection], self, setGroup))
  
  "adds new sets" in {
    val connection = createConnection(self)
    
    connection ! Tcp.Received(ByteString("ADD set_key element\n"))

    expectMsg(SetGroup.Add("set_key", "element"))
  }
  
  "returns failure on unknown command" in {
    val connection = createConnection(self)
    
    connection ! Tcp.Received(ByteString("UNKNOWN\n"))
    
    expectMsg(Write(ByteString("FAILURE: unknown command 'UNKNOWN'\n\n")))
  }
  
  "estimates all sets" in {
    val connection = createConnection(setGroup)
    
    connection ! Tcp.Received(ByteString("ADD my_set a\n"))
    connection ! Tcp.Received(ByteString("ADD my_set2 a\n"))
    
    connection ! Tcp.Received(ByteString("ESTIMATE\n"))
    
    expectMsgPF() {
      case Write(byteString, event) => {
        val output = byteString.utf8String.split("\n").sorted
        
        output == Array("", "my_set,1", "my_set2,1", "SUCCESS")
      }
    }
  }
  
  "handles multiple commands" in {
    val connection = createConnection(setGroup)
    
    connection ! Tcp.Received(ByteString("ADD my_set a\nADD my_set b\nESTIMATE\n"))
    
    expectMsg(Write(ByteString("SUCCESS\nmy_set,2\n\n")))
  }
  
  "handles partial commands" in {
    val connection = createConnection(setGroup)
    
    connection ! Tcp.Received(ByteString("ADD my"))
    connection ! Tcp.Received(ByteString("_set a\nESTIMATE\n"))
    
    expectMsg(Write(ByteString("SUCCESS\nmy_set,1\n\n")))
  }
}
