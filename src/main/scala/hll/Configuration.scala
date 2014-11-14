package hll

import com.typesafe.config.ConfigFactory

object Configuration {
  def systemDefault = ConfigFactory.parseString("cursor-traverse-dispatcher {\n" +
    "executor = \"thread-pool-executor\"\n" +
    "type = PinnedDispatcher\n" +
    "}")

  def systemComplete = {
    val regularConfig = ConfigFactory.load
    val combined = systemDefault.withFallback(regularConfig)

    ConfigFactory.load(combined)
  }
}