package ru.neoflex.nussknacker.flink.jdbc

import pl.touk.nussknacker.sql.db.schema.DbParameterMetaData

import java.sql.Connection
import scala.util.Using

final class ExtJdbcMetaDataProvider(getConnection: () => Connection) {
  def this(connection: Connection) = this(() => connection)

  def getQueryParameterMetaData(query: String): DbParameterMetaData = {
    Using.resource(getConnection()) { connection =>
      Using.resource(connection.prepareStatement(query)) { statement =>
        DbParameterMetaData(statement.getParameterMetaData.getParameterCount)
      }
    }
  }
}
