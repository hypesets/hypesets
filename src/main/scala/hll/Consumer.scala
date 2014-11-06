package hll

import akka.actor.Actor
import akka.actor.ActorRef
import java.util.Properties
import kafka.consumer.Whitelist
import kafka.consumer.ConsumerConfig
import kafka.serializer.DefaultDecoder
import kafka.message.MessageAndMetadata
import org.msgpack.MessagePack
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import akka.actor.actorRef2Scala

object Consumer {
  case class Message(message: HashMap[String, Any])
}

class Consumer(properties: Properties, recipient: ActorRef) extends Actor {
  def receive = {
    case v => {}
  }

  val config = new ConsumerConfig(properties)
  val connector = kafka.consumer.Consumer.create(config)
  val filterSpec = new Whitelist(properties.getProperty("topic"))
  
  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).head
  
  def unpack(messageAndMetadata: MessageAndMetadata[Array[Byte], Array[Byte]]) = {
    val unpackedMessage: HashMap[String, Any] = new HashMap
    val unpacked = MessagePack.unpack(messageAndMetadata.message)
    for (entry <- unpacked.asMapValue.entrySet) {
      val value: Any = entry.getValue match {
        case v if v.isBooleanValue => v.asBooleanValue
        case v if v.isIntegerValue => v.asIntegerValue
        case v if v.isFloatValue => v.asFloatValue
        case v if v.isRawValue => v.asRawValue.getString
        case v => None
      }
      unpackedMessage.put(entry.getKey.asRawValue.getString, value)
    }
    unpackedMessage
  }
  
  for (messageAndMetadata <- stream) recipient ! Consumer.Message(unpack(messageAndMetadata)) 
}
