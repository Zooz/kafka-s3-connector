package com.zooz.kafkas3connector.kafka

import java.util.Properties
import org.apache.kafka.clients.admin.{ AdminClient, ListTopicsOptions, NewTopic }
import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.clients.admin.Config
import java.util.Collections
import play.api.Logger

/** A helper class used to manage Kafka topics (can be used for testing) */
class KafkaAdmin(bootstrapServers: String) {
  val TIMEOUT = 60 * 1000
  protected val logger = Logger(this.getClass())

  /** Modifies a topic retention */
  private def modifyRetention(adminClient: AdminClient, topic: String, retentionMs: Long) = {
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      val retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, retentionMs.toString())
      val updateConfig = new java.util.HashMap[ConfigResource, Config]();
      updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));
      val alterConfigsResult = adminClient.alterConfigs(updateConfig);
      alterConfigsResult.all()
    } catch {
      case e: Exception => 
        logger.error(s"Caugt an exception while trying to modify retention for topic $topic", e)
        throw(e)
    }
  }
  
  /** Creates a Kafka topic, if it doesn't exist already */
  def createTopicIfNotExists(
      topic: String, 
      replicationFactor: Short = 1,
      partitions: Int = 1,
      retentionMs: Option[Long] = None) = {
    val adminClient = createAdminClient
    try {
      if (!isTopicExist(adminClient, topic)) {
        logger.info(s"Topic $topic does not exists - creating it")
        val newTopic = new NewTopic(topic, partitions, replicationFactor)
        val result = adminClient.createTopics(List(newTopic).asJavaCollection)
        result.all().get
        logger.info(s"Topic $topic successfully created")
        if (retentionMs.isDefined) {
          modifyRetention(adminClient, topic, retentionMs.get)
        }
      }
    } catch {
      case e: Exception => {
        logger.error(s"Failed creating topic $topic", e)
        throw (e)
      }
    } finally {
      adminClient.close()
    }
  }

  /** Lists all topics and checks if the given topic is one of them */
  private def isTopicExist(adminClient: AdminClient, topic: String): Boolean = {
    logger.info(s"Checking if topic $topic exists")
    val futureGetTopics = adminClient.listTopics(new ListTopicsOptions().timeoutMs(TIMEOUT))
    val topics = futureGetTopics.names.get
    !topics.isEmpty() && topics.contains(topic)
  }

  /** Creates a handle to the Kafka cluster */
  private def createAdminClient: AdminClient = {
    val config = new Properties
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    AdminClient.create(config)
  }
}
