package hll

import akka.actor.ActorRef

case class SetReference(lastAccess: Long, actor: ActorRef) {

}