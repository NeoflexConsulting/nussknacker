package ru.neoflex.nussknacker.flink.jdbc

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{ Assertion, BeforeAndAfterEach, FunSuite, Matchers }
import pl.touk.nussknacker.engine.api.ProcessVersion
import pl.touk.nussknacker.engine.api.deployment.DeploymentData
import pl.touk.nussknacker.engine.build.EspProcessBuilder
import pl.touk.nussknacker.engine.flink.test.FlinkSpec
import pl.touk.nussknacker.engine.graph.{ expression, EspProcess }
import pl.touk.nussknacker.engine.modelconfig.DefaultModelConfigLoader
import pl.touk.nussknacker.engine.process.ExecutionConfigPreparer
import pl.touk.nussknacker.engine.process.compiler.FlinkProcessCompiler
import pl.touk.nussknacker.engine.process.registrar.FlinkProcessRegistrar
import pl.touk.nussknacker.engine.spel
import pl.touk.nussknacker.engine.testing.LocalModelData

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class FlinkJdbcSinkSpec extends FunSuite with FlinkSpec with Matchers with BeforeAndAfterEach {

  import spel.Implicits._

  override protected def afterEach(): Unit = {
    super.afterEach()
    DB.HsqlDB.conn.prepareStatement("DELETE FROM posts").execute()
  }

  private def buildProcess(parameters: (String, expression.Expression)*): EspProcess = {
    EspProcessBuilder
      .id("jdbc-sink-test")
      .source("dummy-source", "source")
      .emptySink(
        "jdbc-sink",
        "flinkJdbcSink",
        parameters: _*
      )
  }

  test("sink should persist message to database") {
    val process = buildProcess(
      JdbcSinkFactory.QueryParameterName -> "'INSERT INTO posts(id, text, user_id) values (?, ?, ?)'",
      JdbcSinkFactory.argName(1)         -> "1",
      JdbcSinkFactory.argName(2)         -> "#input.message",
      JdbcSinkFactory.argName(3)         -> "777"
    )

    val data = List(Post("some message"))

    executeTest(process, data) { case (id, text, userId) =>
      id should be(1)
      text should be("some message")
      userId should be(777)
    }
  }

  test("sink should update existing message in database") {
    DB.HsqlDB.conn.prepareStatement("INSERT INTO posts(id, text, user_id) values(1, 'test message', 22)").execute()

    val process = buildProcess(
      JdbcSinkFactory.QueryParameterName -> "'UPDATE posts SET text = ? WHERE id = ?'",
      JdbcSinkFactory.argName(1)         -> "#input.message",
      JdbcSinkFactory.argName(2)         -> "1"
    )

    val data = List(Post("updated message"))

    executeTest(process, data) { case (id, text, userId) =>
      id should be(1)
      text should be("updated message")
      userId should be(22)
    }
  }

  test("sink should delete one message from database") {
    DB.HsqlDB.conn.prepareStatement("INSERT INTO posts(id, text, user_id) values(1, 'test message', 22)").execute()
    DB.HsqlDB.conn.prepareStatement("INSERT INTO posts(id, text, user_id) values(2, 'test message', 22)").execute()

    val process = buildProcess(
      JdbcSinkFactory.QueryParameterName -> "'DELETE FROM posts WHERE id = ?'",
      JdbcSinkFactory.argName(1)         -> "1"
    )

    val data = List(Post("delete marker"))

    executeTest(process, data) { case (id, _, _) =>
      id should be(2)
    }
  }

  test("sink should insert all input messages to database") {

    val process = buildProcess(
      JdbcSinkFactory.QueryParameterName -> "'INSERT INTO posts(id, text, user_id) values (?, ?, ?)'",
      JdbcSinkFactory.argName(1)         -> "#input.id",
      JdbcSinkFactory.argName(2)         -> "#input.message",
      JdbcSinkFactory.argName(3)         -> "777"
    )

    val data = List(
      Post("first", Some(1)),
      Post("second", Some(2)),
      Post("third", Some(3))
    )

    run(process, data)

    val st = DB.HsqlDB.conn.prepareStatement("SELECT * FROM posts")
    val rs = st.executeQuery()

    val totalRecords = Iterator.continually(rs).takeWhile(_.next()).map(_ => 1).sum

    totalRecords should be(data.size)

    st.close()
  }

  private def executeTest(
    process: EspProcess,
    data: List[Post]
  )(
    assertion: (Long, String, Long) => Assertion
  ): Assertion = {
    run(process, data)

    val st = DB.HsqlDB.conn.prepareStatement("SELECT * FROM posts")
    val rs = st.executeQuery()
    rs.next()

    val moreThanOneRow = !rs.isLast
    val id             = rs.getLong("id")
    val text           = rs.getString("text")
    val userId         = rs.getLong("user_id")
    rs.close()

    moreThanOneRow should be(false)
    assertion(id, text, userId)
  }

  def run[T: ClassTag: TypeTag: TypeInformation](process: EspProcess, data: List[T]): Unit = {
    val env            = flinkMiniCluster.createExecutionEnvironment()
    val finalConfig    = ConfigFactory.load()
    val resolvedConfig =
      new DefaultModelConfigLoader().resolveInputConfigDuringExecution(finalConfig, getClass.getClassLoader).config
    val modelData      =
      LocalModelData(resolvedConfig, new TestConfigCreator(data, DB.HsqlDB.connectionConfig, DB.HsqlDB.getConnection))
    val registrar      =
      FlinkProcessRegistrar(new FlinkProcessCompiler(modelData), ExecutionConfigPreparer.unOptimizedChain(modelData))
    registrar.register(new StreamExecutionEnvironment(env), process, ProcessVersion.empty, DeploymentData.empty)
    env.executeAndWaitForFinished(process.id)()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    DB.HsqlDB.close()
  }
}

object DB {
  val HsqlDB = new HsqlDB(
    List(
      "CREATE TABLE posts(id bigint primary key, text varchar(128) not null, user_id bigint not null)"
    )
  )
}

final case class Post(message: String, id: Option[Long] = None)
