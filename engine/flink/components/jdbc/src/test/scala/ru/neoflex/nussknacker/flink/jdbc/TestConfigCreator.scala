package ru.neoflex.nussknacker.flink.jdbc

import org.apache.flink.api.common.typeinfo.TypeInformation
import pl.touk.nussknacker.engine.api.process.{ ProcessObjectDependencies, SinkFactory, WithCategories }
import pl.touk.nussknacker.engine.process.helpers.BaseSampleConfigCreator

import java.sql.Connection
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class TestConfigCreator[T: ClassTag: TypeTag: TypeInformation](
  data: List[T],
  connectionConfig: ConnectionConfig,
  getConnection: () => Connection)
    extends BaseSampleConfigCreator(data) {
  override def sinkFactories(
    processObjectDependencies: ProcessObjectDependencies
  ): Map[String, WithCategories[SinkFactory]] =
    super.sinkFactories(processObjectDependencies) + ("flinkJdbcSink" -> WithCategories(
      new JdbcSinkFactory(connectionConfig, new ExtJdbcMetaDataProvider(getConnection))
    ))
}
