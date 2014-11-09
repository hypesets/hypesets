package hll

import akka.actor.Actor
import akka.actor.ActorRef
import com.sleepycat.je.Database
import com.sleepycat.je.DatabaseEntry
import com.sleepycat.je.LockMode
import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.HLL
import com.sleepycat.je.OperationStatus
import com.twitter.algebird.HyperLogLog

object Adder {
  case class Add(key: String, value: String)
  case object Done
}

class Adder(database: Database, recipient: ActorRef) extends Actor {
  
  val hllm = new HyperLogLogMonoid(12)
  val dataEntry = new DatabaseEntry
  
  def add(key: String, value: String) = {
   
    def findSet(keyEntry: DatabaseEntry): HLL = {
      database.get(null, keyEntry, dataEntry, LockMode.DEFAULT) match {
        case OperationStatus.SUCCESS => HyperLogLog.fromBytes(dataEntry.getData)
        case _ => hllm.zero
      }
    }
    
    def storeSet(keyEntry: DatabaseEntry, set: HLL) = {
      database.put(null, keyEntry, new DatabaseEntry(HyperLogLog.toBytes(set)))
    }
    
    val keyEntry = new DatabaseEntry(key.getBytes("UTF-8"))
    
    val set = hllm.plus(findSet(keyEntry), hllm(value.getBytes("UTF-8")))
    storeSet(keyEntry, set)
    
    recipient ! Adder.Done
  }
  
  def receive = {
    case Adder.Add(key: String, value: String) => add(key, value)
  }
}
