package hll

import akka.actor.{ Actor, ActorRef, Props }
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import java.net.InetSocketAddress

object Server {
  case object BindFailed
}

class Server extends Actor {

  import Tcp._
  import context.system
  
  val setGroup = context.actorOf(Props[SetGroup])
  
  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 30303))

  def receive = {
    case Bound(localAddress) => {}
    case CommandFailed(_: Bind) => context.parent ! Server.BindFailed
    case Connected(remote, local) => {
      val handler = context.actorOf(Props(classOf[Connection], sender, setGroup))
      
      sender ! Register(handler)
    }
  }
}