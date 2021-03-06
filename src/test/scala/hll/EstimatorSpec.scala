package hll

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.Environment
import com.sleepycat.je.DatabaseConfig
import java.io.File
import com.sleepycat.je.Database
import org.scalatest.BeforeAndAfterEach
import com.twitter.algebird.HyperLogLogMonoid
import com.sleepycat.je.DatabaseEntry
import com.twitter.algebird.HyperLogLog
import com.twitter.algebird.HLL
import akka.actor.Props
import akka.actor.ActorRef

class EstimatorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  def this() = this(ActorSystem("MySpec"))
  
  val databaseHelper = new DatabaseHelper("EstimatorSpec")
  
  override def afterAll {
    TestKit.shutdownActorSystem(system)
    databaseHelper.close
  }
  
  override def beforeEach {
    databaseHelper.truncate
  }
  
  def runEstimator(database: Database, recipient: ActorRef, start: String, stop: String) {
    val estimator = system.actorOf(Props(classOf[Estimator], database, self, start, stop))

    estimator ! Estimator.Iterate    
  }
  
  "estimates single set" in {
    val monoid = new HyperLogLogMonoid(12)
    val set = monoid.zero
    
    databaseHelper.saveSet("myKey", set)
    
    runEstimator(databaseHelper.getDatabase, self, "myKey", "myKey")
    
    expectMsg(Estimator.Estimation("myKey", 0.0))
    expectMsg(Estimator.Done)
  }
  
  "estimates range" in {
    val hll = new HyperLogLogMonoid(12)
    
    val set1 = hll.zero
    databaseHelper.saveSet("set1", set1)
    val set2 = hll.plus(set1, hll("a".getBytes))
    databaseHelper.saveSet("set12", set2)
    val set3 = hll.plus(set2, hll("b".getBytes))
    databaseHelper.saveSet("set12 1", set3)
    val set4 = hll.plus(set3, hll("c".getBytes))
    databaseHelper.saveSet("set13 1", set4)
    
    runEstimator(databaseHelper.getDatabase, self, "set12", "set12xxx")
    
    expectMsg(Estimator.Estimation("set12", 1.0))
    expectMsg(Estimator.Estimation("set12 1", 2.0))
    expectMsg(Estimator.Done)
  }
  
  "sends done when none keys found" in {
    runEstimator(databaseHelper.getDatabase, self, "someKey", "someKey")
    
    expectMsg(Estimator.Done)
  } 
}