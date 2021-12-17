package ru.neoflex.nussknacker.flink.jdbc

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.flink.connector.jdbc.JdbcExecutionOptions
import pl.touk.nussknacker.engine.api.component.{ ComponentDefinition, ComponentProvider, NussknackerVersion }
import pl.touk.nussknacker.engine.api.process.ProcessObjectDependencies

import java.sql.{ Connection, DriverManager }

class JdbcSinkComponentProvider extends ComponentProvider with LazyLogging {

  override val providerName: String = "flinkJdbcSink"

  override def resolveConfigForExecution(config: Config): Config = config

  override def create(config: Config, dependencies: ProcessObjectDependencies): List[ComponentDefinition] = {
    logger.debug(s"Trying to load $providerName component provider")
    readConfig(config.getConfig("config"), "queryJdbcSink").map(createJdbcSinkFactory).toList
  }

  private def createJdbcSinkFactory(jdbcSinkConfig: JdbcSinkConfig) = {
    val sinkFactory = new JdbcSinkFactory(
      jdbcSinkConfig.db,
      new ExtJdbcMetaDataProvider(getConnection(jdbcSinkConfig.db))
    )
    logger.debug(s"Created $sinkFactory for ${jdbcSinkConfig.name}")
    ComponentDefinition(jdbcSinkConfig.name, sinkFactory)
  }

  private def readConfig(config: Config, path: String): Option[JdbcSinkConfig] = {
    if (config.hasPath(path)) {
      Some(config.as[JdbcSinkConfig](path))
    } else {
      None
    }
  }

  override def isCompatible(version: NussknackerVersion): Boolean = true

  private def getConnection(connectionConfig: ConnectionConfig): () => Connection = () =>
    DriverManager.getConnection(connectionConfig.url, connectionConfig.username, connectionConfig.password)

}

final case class JdbcSinkConfig(name: String, db: ConnectionConfig)
final case class ConnectionConfig(
  driverClassName: String,
  url: String,
  username: String,
  password: String,
  batchSize: Int = JdbcExecutionOptions.DEFAULT_SIZE,
  maxRetries: Int = JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES,
  batchIntervalMs: Long = 0)
