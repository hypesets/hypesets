package hll

import akka.actor.Actor
import akka.io.Tcp
import akka.util.ByteString
import akka.actor.ActorRef
import akka.actor.Props
import akka.io.Tcp.Event

object Connection {
  case object Ack extends Event
}

class Connection(tcp: ActorRef, setGroup: ActorRef, database: DatabaseHelper) extends Actor {

  import Tcp._

  var partialCommand: String = ""
  val adder = context.actorOf(Props(classOf[Adder], database.getDatabase, self))
  var estimationBuffer = new StringBuilder
  var estimator: Option[ActorRef] = None
  var acknowledged = true

  def command(data: String) = {
    val line = data.trim
    val commandArray = line.split(" ")

    def estimate(start: String, stop: String) = {
      val actor = context.actorOf(Props(classOf[Estimator], database.getDatabase, self, start, stop))
      estimator = Some(actor)
      
      actor ! Estimator.Iterate
    }
    
    commandArray match {
      case Array("ADD", setKey, element) => {
        adder ! Adder.Add(setKey, element)
      }
      case Array("ESTIMATE", start, stop) if estimator == None => estimate(start, stop)
      case other => {
        tcp ! Write(ByteString(s"FAILURE: unknown command '$line'\n\n"))
      }
    }
  }

  def processInput(data: ByteString) = {
    val commands = (partialCommand + data.utf8String).split("\n")

    def processCommands(commands: Array[String]) = for (cmd <- commands) command(cmd)

    if (data.endsWith("\n")) {
      partialCommand = ""
      processCommands(commands)
    } else {
      partialCommand = commands.last
      processCommands(commands.dropRight(1))
    }
  }

  def sendEstimation = if (acknowledged) {
    val data = estimationBuffer.toString
    
    if (data.size > 0) {
      tcp ! Write(ByteString(estimationBuffer.toString), Connection.Ack)

      estimationBuffer = new StringBuilder
      acknowledged = false
    }
  }

  def listening: Actor.Receive = {
    case Received(data) => processInput(data)
    case Adder.Done => {
      estimationBuffer.append("DONE\n")
      
      sendEstimation
    }
    case Connection.Ack => {
      acknowledged = true
      
      sendEstimation
    }
    case Estimator.Estimation(key, value) => {
      estimationBuffer.append(key)
      estimationBuffer.append(',')
      estimationBuffer.append(value.toInt)
      estimationBuffer.append('\n')
      
      sendEstimation
    }
    case Estimator.Done => {
      estimationBuffer.append("DONE\n")
      
      sendEstimation
    }
    case PeerClosed => context stop self
  }

  def receive = listening
}
