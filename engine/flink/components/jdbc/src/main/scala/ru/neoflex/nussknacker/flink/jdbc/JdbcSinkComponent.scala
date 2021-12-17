package ru.neoflex.nussknacker.flink.jdbc

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.connector.jdbc.{ JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder }
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import pl.touk.nussknacker.engine.api.{ Context, LazyParameter, ValueWithContext }
import pl.touk.nussknacker.engine.flink.api.process.{ BasicFlinkSink, FlinkLazyParameterFunctionHelper }

import java.sql.PreparedStatement

final class JdbcSinkComponent(
  query: String,
  connectionConfig: ConnectionConfig,
  parameters: LazyParameter[List[AnyRef]])
    extends BasicFlinkSink {

  override type Value = List[AnyRef]

  override def valueFunction(
    helper: FlinkLazyParameterFunctionHelper
  ): FlatMapFunction[Context, ValueWithContext[Value]] = helper.lazyMapFunction(parameters)

  override def toFlinkFunction: SinkFunction[Value] = {
    val statementBuilder: JdbcStatementBuilder[Value] = new JdbcStatementBuilder[Value]() {
      override def accept(statement: PreparedStatement, args: List[AnyRef]): Unit = setQueryArguments(statement, args)
    }
    JdbcSink.sink(
      query,
      statementBuilder,
      executionOptions,
      connectionOptions
    )
  }

  private def connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    .withUrl(connectionConfig.url)
    .withUsername(connectionConfig.username)
    .withPassword(connectionConfig.password)
    .withDriverName(connectionConfig.driverClassName)
    .build()

  private def executionOptions = JdbcExecutionOptions
    .builder()
    .withBatchSize(connectionConfig.batchSize)
    .withMaxRetries(connectionConfig.maxRetries)
    .withBatchIntervalMs(connectionConfig.batchIntervalMs)
    .build()

  private def setQueryArguments(statement: PreparedStatement, queryArguments: Value): Unit =
    queryArguments.zipWithIndex.foreach { case (value, index) =>
      statement.setObject(index + 1, value)
    }
}
