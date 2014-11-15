package hll

import com.sleepycat.je.Database
import akka.actor.ActorRef
import com.sleepycat.je.DatabaseEntry
import com.sleepycat.je.LockMode
import com.sleepycat.je.OperationStatus
import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.HyperLogLog
import com.sleepycat.je.Cursor
import com.twitter.algebird.HLL
import akka.actor.Actor

object Estimator {
  trait Result
  
  case class Estimation(key: String, value: Double) extends Result
  case object Done extends Result
  case object Iterate
}

class Estimator(database: Database, recipient: ActorRef, start: String, stop: String) extends Actor {
  val cursor = database.openCursor(null, null)

  val startKey = new DatabaseEntry(start.getBytes("UTF-8"))

  val foundKey = new DatabaseEntry
  val foundData = new DatabaseEntry

  cursor.getSearchKeyRange(startKey, foundData, LockMode.DEFAULT)

  try {
    cursor.getCurrent(foundKey, foundData, LockMode.DEFAULT)
  } catch {
    case e: com.sleepycat.je.DatabaseException => finish
  }

  def finish {
    cursor.close

    recipient ! Estimator.Done
    
    context stop self
  }

  def iterate {
    val key = new String(foundKey.getData)

    if (key <= stop) {
      val set = HyperLogLog.fromBytes(foundData.getData)

      recipient ! Estimator.Estimation(key, set.estimatedSize)

      if (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
        self ! Estimator.Iterate
      } else finish
    } else finish
  }

  def receive = {
    case Estimator.Iterate if foundKey.getData != null => iterate
  }
}
