package hll

import akka.actor.ActorSystem
import akka.actor.Props

object Main extends App {
  val system = ActorSystem("akka-hll", Configuration.systemComplete)
  system.actorOf(Props[Server], "server")
}