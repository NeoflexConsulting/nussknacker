package ru.neoflex.nussknacker.flink.jdbc

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.scala._
import org.scalatest.{ FlatSpec, Inside, Matchers, OptionValues }
import pl.touk.nussknacker.engine.api.component.SingleComponentConfig
import pl.touk.nussknacker.engine.api.context.ProcessCompilationError.{ CustomNodeError, MissingParameters, NodeId }
import pl.touk.nussknacker.engine.api.context.{ ProcessCompilationError, ValidationContext }
import pl.touk.nussknacker.engine.api.{ MetaData, StreamMetaData }
import pl.touk.nussknacker.engine.compile.ExpressionCompiler
import pl.touk.nussknacker.engine.compile.nodecompilation.GenericNodeTransformationValidator
import pl.touk.nussknacker.engine.graph.evaluatedparam
import pl.touk.nussknacker.engine.{ spel, ModelData }
import pl.touk.nussknacker.engine.testing.LocalModelData

class JdbcComponentSpec extends FlatSpec with Matchers with OptionValues with Inside {
  import spel.Implicits._

  val db = new HsqlDB(
    List(
      "CREATE TABLE posts(id bigint primary key, text varchar(128) not null, user_id bigint not null)"
    )
  )

  implicit val meta: MetaData = MetaData("processId", StreamMetaData())
  implicit val nodeId: NodeId = NodeId("id")

  val modelData: ModelData = LocalModelData(
    ConfigFactory.empty(),
    new TestConfigCreator[String](List.empty, db.connectionConfig, db.getConnection)
  )

  val validator = new GenericNodeTransformationValidator(
    ExpressionCompiler.withoutOptimization(modelData),
    modelData.processWithObjectsDefinition.expressionConfig
  )

  val sinkFactory = new JdbcSinkFactory(db.connectionConfig, new ExtJdbcMetaDataProvider(() => db.getConnection()))
  val insertQuery = "INSERT INTO posts(id, text, user_id) values (?, ?, ?)"

  "factory" should "parse query and ask next arguments" in {
    val parameters = evaluatedparam.Parameter(JdbcSinkFactory.QueryParameterName, s"'$insertQuery'") :: Nil

    val validationResult =
      validator.validateNode(sinkFactory, parameters, Nil, None, SingleComponentConfig.zero)(ValidationContext())

    validationResult.isValid should be(true)
    val result = validationResult.toOption.value

    result.errors should have size 3

    result.errors.toSet.flatMap[String, Set[_]] { error: ProcessCompilationError =>
      error match {
        case MissingParameters(params, _) => params
        case _                            => Set.empty[String]
      }
    } should contain theSameElementsAs Seq(
      JdbcSinkFactory.argName(1),
      JdbcSinkFactory.argName(2),
      JdbcSinkFactory.argName(3)
    )

    result.parameters.map(_.name) should contain theSameElementsAs Seq(
      JdbcSinkFactory.QueryParameterName,
      JdbcSinkFactory.argName(1),
      JdbcSinkFactory.argName(2),
      JdbcSinkFactory.argName(3)
    )

    result.finalState.value should be(JdbcSinkFactory.ParametersState(insertQuery, 3))
  }

  "factory" should "parse query and arguments" in {
    val parameters = evaluatedparam.Parameter(JdbcSinkFactory.QueryParameterName, s"'$insertQuery'") ::
      evaluatedparam.Parameter(JdbcSinkFactory.argName(1), "1") ::
      evaluatedparam.Parameter(JdbcSinkFactory.argName(2), "'asd'") ::
      evaluatedparam.Parameter(JdbcSinkFactory.argName(3), "1") ::
      Nil

    val validationResult =
      validator.validateNode(sinkFactory, parameters, Nil, None, SingleComponentConfig.zero)(ValidationContext())

    validationResult.isValid should be(true)
    val result = validationResult.toOption.value

    result.errors should be(empty)

    result.parameters.map(_.name) should contain theSameElementsAs Seq(
      JdbcSinkFactory.QueryParameterName,
      JdbcSinkFactory.argName(1),
      JdbcSinkFactory.argName(2),
      JdbcSinkFactory.argName(3)
    )

    result.finalState.value should be(JdbcSinkFactory.ParametersState(insertQuery, 3))
  }

  "factory" should "return error on bad query" in {
    val parameters =
      evaluatedparam.Parameter(JdbcSinkFactory.QueryParameterName, s"'INSERT INTO bad_table(id) values(?)'") :: Nil

    val validationResult =
      validator.validateNode(sinkFactory, parameters, Nil, None, SingleComponentConfig.zero)(ValidationContext())

    validationResult.isValid should be(true)
    val result = validationResult.toOption.value

    result.errors should have size 1
    inside(result.errors) { case CustomNodeError(_, _, Some(paramName)) :: Nil =>
      paramName should be(JdbcSinkFactory.QueryParameterName)
    }

    result.finalState should be(None)
  }
}
