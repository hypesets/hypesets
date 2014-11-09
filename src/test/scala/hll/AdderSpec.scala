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
  
  override def beforeAll {
    database = Some(createDatabase)
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    database.map(_.close)
  }
  
  override def beforeEach {
    database.map(_.truncate(null, false))
  }
  
  var database: Option[Database] = None
  
  def createDatabase = {
    val envConfig = new EnvironmentConfig();
    envConfig.setAllowCreate(true);
    var dbEnvironment = new Environment(new File("db"), envConfig)
    
    // Open the database. Create it if it does not already exist.
    var dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbEnvironment.openDatabase(null, "sampleDatabase", dbConfig); 
  }
  
  def saveSet(name: String, set: HLL) = {
    val key = new DatabaseEntry(name.getBytes("UTF-8")) 
    val data = new DatabaseEntry(HyperLogLog.toBytes(set))
    
    database.map(_.put(null, key, data))
  }
  
  def runAdder(database: Database, key: String, element: String) = {
    val adder = system.actorOf(Props(classOf[Adder], database, self))

    adder ! Adder.Add(key, element) 
  }
  
  "Adds element to the new set" in {
    val hllm = new HyperLogLogMonoid(12)
    
    runAdder(database.get, "set1", "abcd")
    
    expectMsg(Adder.Done)
    
    val key = new DatabaseEntry("set1".getBytes("UTF-8"))
    val data = new DatabaseEntry
    
    database.get.get(null, key, data, LockMode.DEFAULT)
    
    val expected = HyperLogLog.toBytes(hllm("abcd".getBytes("UTF-8")))
    
    assert(data.getData.deep == expected.deep)
  }
  
  "Adds element to the existing set" in {
    val hllm = new HyperLogLogMonoid(12)
    
    val initialSet = hllm("abcd".getBytes("UTF-8"))
    saveSet("set1", initialSet)
    
    runAdder(database.get, "set1", "abcde")
    
    expectMsg(Adder.Done)
    
    val key = new DatabaseEntry("set1".getBytes("UTF-8"))
    val data = new DatabaseEntry
    
    database.get.get(null, key, data, LockMode.DEFAULT)
    
    val addedSet = hllm("abcde".getBytes("UTF-8"))
    val expected = HyperLogLog.toBytes(hllm.plus(initialSet, addedSet))
    
    assert(data.getData.deep == expected.deep)
  }
}