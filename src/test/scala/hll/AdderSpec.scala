package hll

import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.actor.ActorSystem
import com.sleepycat.je.DatabaseEntry
import com.twitter.algebird.HyperLogLog
import com.twitter.algebird.HLL
import com.sleepycat.je.DatabaseConfig
import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.Database
import java.io.File
import akka.actor.Props
import com.sleepycat.je.LockMode
import com.twitter.algebird.HyperLogLogMonoid

class AdderSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  def this() = this(ActorSystem("MySpec"))
  
  val databaseHelper = new DatabaseHelper("AdderSpec")
  
  override def afterAll {
    TestKit.shutdownActorSystem(system)
    databaseHelper.close
  }
  
  override def beforeEach {
    databaseHelper.truncate
  }
  
  def runAdder(database: Database, key: String, element: String) = {
    val adder = system.actorOf(Props(classOf[Adder], database, self))

    adder ! Adder.Add(key, element) 
  }
  
  "Adds element to the new set" in {
    val hllm = new HyperLogLogMonoid(12)
    
    runAdder(databaseHelper.getDatabase, "set1", "abcd")
    
    expectMsg(Adder.Done)
    
    val savedSet = databaseHelper.loadSet("set1")
    val expected = hllm("abcd".getBytes("UTF-8"))
    
    assert(expected == savedSet)
  }
  
  "Adds element to the existing set" in {
    val hllm = new HyperLogLogMonoid(12)
    
    val initialSet = hllm("abcd".getBytes("UTF-8"))
    databaseHelper.saveSet("set1", initialSet)
    
    runAdder(databaseHelper.getDatabase, "set1", "abcde")
    
    expectMsg(Adder.Done)
    
    val savedSet = databaseHelper.loadSet("set1")
    
    val addedSet = hllm("abcde".getBytes("UTF-8"))
    val expected = hllm.plus(initialSet, addedSet)
    
    assert(expected == savedSet)
  }
}
