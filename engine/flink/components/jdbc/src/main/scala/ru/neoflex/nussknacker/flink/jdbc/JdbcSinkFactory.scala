package ru.neoflex.nussknacker.flink.jdbc

import com.typesafe.scalalogging.LazyLogging
import pl.touk.nussknacker.engine.api.LazyParameter
import pl.touk.nussknacker.engine.api.context.ProcessCompilationError.{CustomNodeError, NodeId}
import pl.touk.nussknacker.engine.api.context.ValidationContext
import pl.touk.nussknacker.engine.api.context.transformation.{DefinedEagerParameter, NodeDependencyValue, SingleInputGenericNodeTransformation}
import pl.touk.nussknacker.engine.api.definition.{NodeDependency, Parameter, SqlParameterEditor}
import pl.touk.nussknacker.engine.api.process.SinkFactory
import pl.touk.nussknacker.engine.api.typed.typing
import pl.touk.nussknacker.engine.flink.api.process.FlinkSink
import pl.touk.nussknacker.sql.db.schema.DbParameterMetaData
import ru.neoflex.nussknacker.flink.jdbc.JdbcSinkFactory._

import java.sql.SQLSyntaxErrorException
import scala.util.{Failure, Success, Try}

object JdbcSinkFactory {

  val ArgsPrefix         = "Arg"
  val QueryParameterName = "Query"

  def argName(num: Int): String = s"$ArgsPrefix$num"

  final case class ParametersState(query: String, argsCount: Int)
}

class JdbcSinkFactory(connectionConfig: ConnectionConfig, extJdbcMetaDataProvider: ExtJdbcMetaDataProvider)
    extends SingleInputGenericNodeTransformation[FlinkSink]
    with SinkFactory
    with LazyLogging {

  override type State = ParametersState

  private val queryParameter = Parameter[String](QueryParameterName).copy(editor = Some(SqlParameterEditor))

  override def nodeDependencies: List[NodeDependency] = Nil

  override def contextTransformation(
    context: ValidationContext,
    dependencies: List[NodeDependencyValue]
  )(implicit nodeId: NodeId
  ): NodeTransformationDefinition =
    initialStep orElse
      queryParameterStep(context) orElse
      finalStep(context)

  private def initialStep: NodeTransformationDefinition = { case TransformationStep(Seq(), None) =>
    NextParameters(queryParameter :: Nil)
  }

  private def queryParameterStep(context: ValidationContext)(implicit nodeId: NodeId): NodeTransformationDefinition = {
    case TransformationStep((queryParameterName, DefinedEagerParameter(query: String, _)) :: Nil, None) =>
      if (query.isEmpty) {
        FinalResults(
          context,
          errors = CustomNodeError("Query is missing", Some(queryParameterName)) :: Nil,
          state = None
        )
      } else {
        parseQuery(query, context)
      }
  }

  private def finalStep(context: ValidationContext)(implicit nodeId: NodeId): NodeTransformationDefinition = {
    case TransformationStep(_, Some(state)) => FinalResults(context, Nil, Some(state))
  }

  private def parseQuery(
    query: String,
    context: ValidationContext
  )(implicit nodeId: NodeId
  ): TransformationStepResult = {
    def handleException(e: Throwable) = e match {
      case sqlException: SQLSyntaxErrorException => sqlException.getMessage
      case _                                     =>
        logger.info(s"Failed to execute query: $query", e)
        s"Failed to execute query: ${e.getClass.getSimpleName}"
    }

    Try(extJdbcMetaDataProvider.getQueryParameterMetaData(query)) match {
      case Success(parameterMetaData) =>
        NextParameters(
          toParameters(parameterMetaData),
          state = Some(ParametersState(query, parameterMetaData.parameterCount))
        )
      case Failure(exception)         =>
        val errorMessage = handleException(exception)
        FinalResults(context, CustomNodeError(errorMessage, Some(QueryParameterName)) :: Nil)
    }
  }

  private def toParameters(dbParameterMetaData: DbParameterMetaData): List[Parameter] = {
    withParameters(dbParameterMetaData.parameterCount) { paramNum =>
      Parameter(argName(paramNum), typing.Unknown).copy(isLazyParameter = true)
    }
  }

  private def withParameters[T](parametersNum: Int)(f: Int => T): List[T] = (1 to parametersNum).map(f).toList

  override def implementation(
    params: Map[String, Any],
    dependencies: List[NodeDependencyValue],
    finalState: Option[State]
  ): JdbcSinkComponent = {
    val state          = finalState.get
    val queryArguments = withParameters(state.argsCount) { argNum =>
      val name = argName(argNum)
      params(name).asInstanceOf[LazyParameter[AnyRef]]
    }
    val sink           = new JdbcSinkComponent(
      state.query,
      connectionConfig,
      LazyParameter.sequence(queryArguments, (x: List[AnyRef]) => x, _ => typing.Unknown)
    )
    logger.debug(s"Created $sink component. Query: ${state.query}, params: $params, deps: $dependencies")
    sink
  }
}
