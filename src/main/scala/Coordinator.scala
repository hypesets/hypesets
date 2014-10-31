import akka.actor.Actor
import akka.actor.Props
import java.util.Properties
import java.util.Date
import java.text.SimpleDateFormat

class Coordinator(properties: Properties) extends Actor {
  
  val setGroup = context.actorOf(Props[SetGroup], "setGroup")
  val dateFormat = new SimpleDateFormat("YY/MM/dd")
  
  override def preStart = {
    context.actorOf(Props(classOf[Consumer], properties, self))
  }
  
  def receive = {
    case Consumer.Message(message) => {
      val appid = message.get("appid").get
      val countryCode = message.get("country_code").get
      
      val date = new Date(message.get("timestamp").get.asInstanceOf[Int] * 1000)
      val day = dateFormat.format(date)
      
      val key = s"$day-$appid-$countryCode"
      
      val deviceId = message.get("device_id").get.asInstanceOf[String]
      
      setGroup ! SetGroup.Add(key, deviceId)
    }
  }
}