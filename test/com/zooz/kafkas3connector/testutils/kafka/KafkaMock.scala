package com.zooz.kafkas3connector.testutils.kafka

import java.util.Properties
import org.apache.curator.test.TestingServer
import java.io.File
import org.apache.commons.io.FileUtils
import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import play.api.Logger

class KafkaMock {
  val ZK_PORT = 50001
  val LOGS_DIR = "/tmp/tmp_kafka_dir"
  val ZK_DIR = "/tmp/zookeeper"
  val KAFKA_TOPIC_ENV = "SOURCE_KAFKA_TOPIC"
  val logger = Logger(this.getClass())
  lazy private val kafka = getKafkaServer()
  lazy private val zkTestServer = new TestingServer(ZK_PORT, new File(ZK_DIR))
  
  /**
   * Generating a Kafka Testing Server instance based on mock configuration
   */
  private def getKafkaServer() = {
    val kafkaServerParts = KafkaMock.KAFKA_BOOTSTRAP.split(":")
    
    for (dir <- Seq(ZK_DIR, LOGS_DIR)) {
      val dirAsFile = new File(dir)
      if (dirAsFile.exists()) {
        FileUtils.deleteDirectory(dirAsFile);
      }
      dirAsFile.mkdirs()
    }
    
    
    val props = new Properties()
    props.put("broker.id", "0");
    props.put("host.name", kafkaServerParts(0));
    props.put("port", kafkaServerParts(1));
    props.put("log.dir", LOGS_DIR);
    props.put("zookeeper.connect", zkTestServer.getConnectString());
    props.put("replica.socket.timeout.ms", "1500");
    props.put("offsets.topic.replication.factor", "1");
    val config = new KafkaConfig(props);
    new KafkaServerStartable(config);
  }
  
  def start() = {
    logger.info("Starting Kafka Mock service")
    kafka.startup()
  }
  
  def stop() = {
    logger.info("Stopping Kafka Mock service")
    try {
      kafka.shutdown()
      kafka.awaitShutdown()
      
      zkTestServer.close()
    } catch {
      case e: Exception => logger.warn("Caught exception while shutting down Kafka", e)
    }
  }
}

object KafkaMock {
  val KAFKA_BOOTSTRAP = "localhost:9092"
}
