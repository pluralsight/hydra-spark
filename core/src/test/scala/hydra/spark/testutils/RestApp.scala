package hydra.spark.testutils

/**
  * Created by alexsilva on 4/12/17.
  */

import java.util.Properties

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry
import io.confluent.kafka.schemaregistry.zookeeper.SchemaRegistryIdentity
import org.eclipse.jetty.server.Server

class RestApp(val port: Int, val zkConnect: String, val kafkaTopic: String,
              val compatibilityType: String, val masterEligibility: Boolean) {

  val prop = new Properties()

  var app: SchemaRegistryRestApplication = _
  var server: Server = _
  // \prop.setProperty(SchemaRegistryConfig.P, port.asInstanceOf[Integer].toString)
  prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect)
  prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic)
  prop.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, compatibilityType)
  prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility:java.lang.Boolean)
  val restConnect = s"http://localhost:$port"


  @throws[Exception]
  def start(): Unit = {
    app = new SchemaRegistryRestApplication(prop)
    server = app.createServer
    server.start()
  }

  @throws[Exception]
  def stop(): Unit = {
    if (server != null) {
      server.stop
      server.join
    }
  }

  def isMaster: Boolean = app.schemaRegistry.isMaster

  @throws[SchemaRegistryException]
  def setMaster(schemaRegistryIdentity: SchemaRegistryIdentity): Unit = {
    app.schemaRegistry.setMaster(schemaRegistryIdentity)
  }

  def myIdentity: SchemaRegistryIdentity = app.schemaRegistry.myIdentity

  def masterIdentity: SchemaRegistryIdentity = app.schemaRegistry.masterIdentity

  def schemaRegistry: SchemaRegistry = app.schemaRegistry
}