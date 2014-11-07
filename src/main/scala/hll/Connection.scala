package hll

import akka.actor.Actor
import akka.io.Tcp
import akka.util.ByteString
import akka.actor.ActorRef

class Connection(tcp: ActorRef, setGroup: ActorRef) extends Actor {
  
  case class Command(cmd: String)
  
  import Tcp._
  
  var partialCommand: String = ""
  
  def command(data: String) = {
    val line = data.trim
    val commandArray = line.split(" ") 
    
    commandArray match {
      case Array("ADD", setKey, element) => setGroup ! SetGroup.Add(setKey, element)
      case Array("ESTIMATE") => {
        setGroup ! SetGroup.Estimate
        
        context.become(estimating)
      }
      case other => tcp ! Write(ByteString(s"FAILURE: unknown command '$line'\n\n"))
    }
  }
  
  def processInput(data: ByteString) = {  
    val commands = (partialCommand + data.utf8String).split("\n")
    
    def processCommands(commands: Array[String]) = for(cmd <- commands) command(cmd)
    
    if( data.endsWith("\n") ) {
      partialCommand = ""
      processCommands(commands)
    } else {
      partialCommand = commands.last
      processCommands(commands.dropRight(1))
    }
  }
  
  def estimating: Actor.Receive = {
    case SetGroup.EstimationResult(result) => {
      val response = new StringBuilder("SUCCESS\n")
      
      for ((setKey, count) <- result) {
        val convertedCount = count.toLong
        response.append(s"$setKey,$convertedCount\n")
      }
      response.append("\n")
      
      context.become(listening)
      tcp ! Write(ByteString(response.toString))
    }
    case PeerClosed => context stop self
  }
  
  def listening: Actor.Receive = {
    case Received(data) => processInput(data)
    case PeerClosed => context stop self
  }
  
  def receive = listening
}
