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
import akka.actor.ActorRefFactory
import com.typesafe.config.ConfigFactory

class ConnectionSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  
  def this() = this(ActorSystem("MySpec", ConfigFactory.load(Configuration.systemDefault)))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    databaseHelper.close
  }

  val databaseHelper = new DatabaseHelper("ConnectionSpec")

  def setGroup = system.actorOf(Props[SetGroup])
  def createConnection(setGroup: ActorRef) = system.actorOf(Props(classOf[Connection], self, setGroup, databaseHelper))

  "adds new sets" in {
    val connection = createConnection(self)
    val adder = system.actorOf(Props(classOf[Adder], databaseHelper.getDatabase, self))

    connection ! Tcp.Received(ByteString("ADD set_key element\n"))
    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
    connection ! Connection.Ack

    connection ! Tcp.Received(ByteString("ESTIMATE set_key set_key\n"))

    expectMsg(Write(ByteString("set_key,1\n"), Connection.Ack))
    connection ! Connection.Ack
    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
  }

  "returns failure on unknown command" in {
    val connection = createConnection(self)

    connection ! Tcp.Received(ByteString("UNKNOWN\n"))

    expectMsg(Write(ByteString("FAILURE: unknown command 'UNKNOWN'\nDONE\n")))
  }

  "estimates all sets" in {
    val connection = createConnection(setGroup)

    connection ! Tcp.Received(ByteString("ADD my_set a\n"))
    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
    connection ! Connection.Ack

    connection ! Tcp.Received(ByteString("ADD my_set2 a\n"))
    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
    connection ! Connection.Ack

    connection ! Tcp.Received(ByteString("ESTIMATE my_set my_setz\n"))

    expectMsg(Write(ByteString("my_set,1\n"), Connection.Ack))
    connection ! Connection.Ack
    // Possible race condition - following message might include DONE as well
    expectMsg(Write(ByteString("my_set2,1\n"), Connection.Ack))
    connection ! Connection.Ack
    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
  }

  "handles multiple commands" in {
    val connection = createConnection(setGroup)

    connection ! Tcp.Received(ByteString("ADD my_set a\nADD my_set b\n"))

    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
    connection ! Connection.Ack
    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
  }

  "handles partial commands" in {
    val connection = createConnection(setGroup)

    connection ! Tcp.Received(ByteString("ADD my"))
    connection ! Tcp.Received(ByteString("_set a\nADD my_set2 b\n"))

    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
    connection ! Connection.Ack
    expectMsg(Write(ByteString("DONE\n"), Connection.Ack))
  }
}
