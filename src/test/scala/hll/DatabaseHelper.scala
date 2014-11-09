package hll

import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.Environment
import java.io.File
import com.sleepycat.je.DatabaseConfig
import com.sleepycat.je.DatabaseEntry
import com.twitter.algebird.HyperLogLog
import com.twitter.algebird.HLL
import com.sleepycat.je.LockMode

class DatabaseHelper(databaseName: String) {
  val envConfig = new EnvironmentConfig();
  envConfig.setAllowCreate(true);
  val dbEnvironment = new Environment(new File("db"), envConfig)

  // Open the database. Create it if it does not already exist.
  val dbConfig = new DatabaseConfig();
  dbConfig.setAllowCreate(true);
  val database = dbEnvironment.openDatabase(null, databaseName, dbConfig);

  def saveSet(name: String, set: HLL) = {
    val key = new DatabaseEntry(name.getBytes("UTF-8"))
    val data = new DatabaseEntry(HyperLogLog.toBytes(set))

    database.put(null, key, data)
  }
  
  def loadSet(name: String): HLL = {
    val key = new DatabaseEntry("set1".getBytes("UTF-8"))
    val data = new DatabaseEntry
    
    database.get(null, key, data, LockMode.DEFAULT)
    
    HyperLogLog.fromBytes(data.getData)
  }
  
  def close = { database.close }
  def truncate = { database.truncate(null, false) }
  def getDatabase = { database }
}