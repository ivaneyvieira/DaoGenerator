package br.com.astrosoft.dbmetadados

import java.lang.Thread.yield
import java.sql.Connection
import kotlin.coroutines.experimental.buildSequence

class Database(val connection: Connection, val databaseName: String) {
  private val metadata = connection.metaData
  val tables: List<Table> = tableList()

  private fun tableList(): List<Table> {
    var rs = metadata.getTables(databaseName, databaseName, "%", arrayOf("TABLE"))
    val seq = buildSequence {
      while (rs.next())
        yield(Table(
                name = rs.getString("TABLE_NAME"),
                type = rs.getString("TABLE_TYPE"),
                remarks = rs.getString("REMARKS")
                   ))
    }
    return seq.toList()
  }

  inner class Table(val name: String, val type: String, val remarks: String) {
    val columns = columnsList()
    val primaryKeys = primaryKey()

    private fun primaryKey(): Index {
      val rs = metadata.getPrimaryKeys(databaseName, databaseName, name)
      val seq = buildSequence {
        while (rs.next()) {
          val first = rs.getString("PK_NAME")
          val second = columns.firstOrNull { it.name == rs.getString("COLUMN_NAME") }
          yield(Pair(first, second))
        }
      }.toList()
      return Index(seq.first().first, seq.mapNotNull { it.second })
    }

    private fun columnsList(): List<Column> {
      val rs = metadata.getColumns(databaseName, databaseName, name, "%")
      return buildSequence {
        while (rs.next())
          yield(Column(
                  name = rs.getString("COLUMN_NAME"),
                  type = rs.getString("DATA_TYPE"),
                  size = rs.getInt("COLUMN_SIZE"),
                  decimal_digits = rs.getInt("COLUMN_SIZE"),
                  nullable = rs.getString("IS_NULLABLE"),
                  remarks = rs.getString("REMARKS")
                      ))
      }.toList()
    }
  }

  inner class Column(val name: String, val type: String, val size: Int,
                     val decimal_digits: Int, val nullable: String, val remarks: String)

  inner class Index(val name: String, val column: List<Column>)
}


