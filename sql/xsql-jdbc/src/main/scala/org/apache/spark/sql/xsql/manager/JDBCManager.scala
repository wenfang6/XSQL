/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.xsql.manager

import java.sql.{Connection, ResultSet, SQLException}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.language.implicitConversions

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.xsql.{CatalogDataSource, DataSourceManager}
import org.apache.spark.sql.xsql.types.{JDBC_COLUMN_AUTOINC, JDBC_COLUMN_DEFAULT, PRIMARY_KEY}


trait JDBCManager extends DataSourceManager with Logging{

  import JDBCManager._
  import DataSourceManager._

  protected var connOpt: Option[Connection] = None

  /**
    * the conf of partitons info present in the currently specified file.
    */
  protected val partitionsMap = new HashMap[String, HashMap[String, HashMap[String, String]]]

  @throws[SQLException]
  implicit protected def typeConvertor(rs: ResultSet) = {
    val list = new ArrayBuffer[HashMap[String, Object]]()
    val md = rs.getMetaData
    val columnCount = md.getColumnCount
    while (rs.next) {
      val rowData = new HashMap[String, Object]()
      for (i <- 1 to columnCount) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      list.append(rowData)
    }
    list
  }

  protected def getConnect(): Connection
  protected def setJdbcOptions(dbName: String, tbName: String): JDBCOptions
  protected def isSelectedDatabase(dsName: String, dbMap: HashMap[String, Object]): Boolean


  /**
    * Author: weiwenda Date: 2018-07-13 20:06
    * Description: similiar to JdbcUtils createTable
    */
  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val conn = getConnect()
    val dbName = tableDefinition.database
    // Must select the database here, as we reuse only one connection
    selectDatabase(conn, dbName)
    val exists = if (ignoreIfExists) "IF NOT EXISTS" else ""
    val dialect = JdbcDialects.get(cachedProperties(URL))
    val strSchema = schemaString(tableDefinition.schema, dialect)
    val table = dialect.quoteIdentifier(tableDefinition.identifier.table)
    val createTableOptions = tableDefinition.properties.map(a => a._1 + "=" + a._2).mkString(" ")
    val sql = s"CREATE TABLE ${exists} $table ($strSchema) $createTableOptions"

    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
      val jdbcOptions = setJdbcOptions(dbName, tableDefinition.identifier.table)
      val schema = resolveTableConnnectOnce(conn, jdbcOptions)
      tableDefinition.copy(
        tableType = CatalogTableType.JDBC,
        storage =
          CatalogStorageFormat.empty.copy(properties = jdbcOptions.asProperties.asScala.toMap),
        schema = schema,
        provider = Some(FULL_PROVIDER))
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error when execute ${sql}, details:\n${e.getMessage}")
    } finally {
      statement.close()
    }
  }

  override def scanXSQLTables(
      dataSourcesByName: mutable.HashMap[String, CatalogDataSource],
      tableDefinition: CatalogTable,
      dataCols: Seq[AttributeReference],
      limit: Boolean,
      sql: String): Seq[Seq[Any]] = {
    val conn = getConnect()
    // Must select the database here, as we reuse only one connection
    selectDatabase(conn, tableDefinition.identifier.database.get)
    val statement = conn.createStatement
    var newSQL = sql
    try {
      if (!limit) {
        newSQL = sql + s" limit ${DEFAULT_lIMIT}"
      }
      val resultSet = statement.executeQuery(newSQL)
      val schema = dataCols.map(dc => StructField(dc.name, dc.dataType))
      val result = JdbcUtils.resultSetToRows(resultSet, StructType(schema)).toSeq.map(_.toSeq)
      result
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error when execute ${sql}, details:\n${e.getMessage}")
    }
  }

  protected def dropTableSQLText(
     dbName: String,
     table: String,
     ignoreIfNotExists: Boolean,
     purge: Boolean): String = {
    throw new UnsupportedOperationException(s"Drop ${shortName()} table not supported!")
  }

  /**
    * Drop 'table' in some data source.
    */
  override def dropTable(
      dbName: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    val conn = getConnect()
    // Must select the database here, as we reuse only one connection
    selectDatabase(conn, dbName)
    val statement = conn.createStatement
    val sql = dropTableSQLText(dbName, table, ignoreIfNotExists, purge)
    try {
      statement.executeUpdate(sql)
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error when execute ${sql}, details:\n${e.getMessage}")
    } finally {
      statement.close()
    }
  }

  /**
    * Alter schema of 'table' in some data source.
    */
  override def alterTableDataSchema(dbName: String, queryContent: String): Unit = {
    val conn = getConnect()
    // Must select the database here, as we reuse only one connection
    selectDatabase(conn, dbName)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(queryContent)
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error when execute ${queryContent}, details:\n${e.getMessage}")
    } finally {
      statement.close()
    }
  }


  override def doGetRawTable(
      dbName: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    val conn = getConnect()
    selectDatabase(conn, dbName)
    val jdbcOptions = setJdbcOptions(dbName, table)
    val schema = resolveTableConnnectOnce(conn, jdbcOptions)
    cacheSpecialProperties(dsName, dbName, table)
    Option(
      CatalogTable(
        identifier = TableIdentifier(table, Option(dbName), Option(dsName)),
        tableType = CatalogTableType.JDBC,
        storage = CatalogStorageFormat.empty.copy(
          properties = jdbcOptions.asProperties.asScala.toMap ++
            specialProperties
              .getOrElse(s"${dsName}.${dbName}.${table}", Map.empty[String, String])),
        schema = schema,
        provider = Some(FULL_PROVIDER)))
  }

  protected def selectDatabase(conn: Connection, dbName: String): Unit = {
    throw new UnsupportedOperationException(s"select ${shortName()} database not supported!")
  }

  override def renameTable(dbName: String, oldName: String, newName: String): Unit = {
    val sql = s"ALTER TABLE ${quoteIdentifier(oldName)} RENAME TO ${quoteIdentifier(newName)}"
    val conn = getConnect()
    // Must select the database here, as we reuse only one connection
    selectDatabase(conn, dbName)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error when execute ${sql}, details:\n${e.getMessage}")
    } finally {
      statement.close()
    }
  }

  override def truncateTable(
      table: CatalogTable,
      partitionSpec: Option[TablePartitionSpec]): Unit = {
    val dbName = table.database
    val conn = getConnect()
    selectDatabase(conn, dbName)
    val tableName = table.identifier.table
    val quoteTbName = quoteIdentifier(tableName)
    val sql = s"TRUNCATE TABLE ${quoteTbName}"
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error when execute ${sql}, details:\n${e.getMessage}")
    } finally {
      statement.close()
    }
  }

  override def getDefaultOptions(table: CatalogTable): Map[String, String] = {
    val jdbcOptions = setJdbcOptions(table.database, table.identifier.table)
    jdbcOptions.asProperties.asScala.toMap ++
      Map(DataSourceManager.TABLETYPE -> CatalogTableType.JDBC.name)
  }

  override def stop(): Unit = {
    connOpt.foreach(_.close())
  }

  protected def quoteIdentifier(tbName: String): String = {
    s"`$tbName`"
  }


  private def cacheSpecialProperties(
       dsName: String,
       dbName: String,
       tbName: String): Unit = {
    val tablePartitionsMap = partitionsMap.get(dbName)
    var partitionsParameters = new HashMap[String, String]
    if (tablePartitionsMap != None) {
      if (tablePartitionsMap.get.get(tbName) != None) {
        partitionsParameters = tablePartitionsMap.get.get(tbName).get
      }
    }
    if (partitionsParameters.nonEmpty) {
      specialProperties += ((s"${dsName}.${dbName}.${tbName}", partitionsParameters))
    }
  }

  /**
    * Author: weiwenda Date: 2018-07-13 20:04
    * Description: similar to JDBCUtils schemaString
    */
  private def schemaString(schema: StructType, dialect: JdbcDialect): String = {
    val sb = new StringBuilder()
    val pkColNames = ArrayBuffer[String]()
    schema.fields.foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ = dialect
        .getJDBCType(field.dataType)
        .orElse(getCommonJDBCType(field.dataType))
        .get
        .databaseTypeDefinition
      val nullable = if (typ.equalsIgnoreCase("TIMESTAMP")) {
        "NULL"
      } else {
        if (field.nullable) {
          ""
        } else {
          "NOT NULL"
        }
      }
      sb.append(s", $name $typ $nullable ")
      if (field.metadata.contains(JDBC_COLUMN_DEFAULT)) {
        sb.append(s"${DEFAULT} ${field.metadata.getString(JDBC_COLUMN_DEFAULT)} ")
      }
      if (field.metadata.contains(JDBC_COLUMN_AUTOINC)) {
        sb.append(s"${AUTO_INCREMENT} ")
      }
      if (field.metadata.contains(COMMENT.toLowerCase)) {
        sb.append(s"${COMMENT} '${field.metadata.getString(COMMENT.toLowerCase)}'")
      }
      if (field.metadata.contains(PRIMARY_KEY)) {
        pkColNames.append(name)
      }
    }
    if (pkColNames.size > 0) {
      sb.append(s", ${COLUMN_PRIMARY_KEY} (${pkColNames.mkString(",")})")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

}

object JDBCManager{

  val DRIVER = "driver"
  val PARTITION_CONF = "partitionConf"
  val AUTO_INCREMENT = "AUTO_INCREMENT"
  val COMMENT = "COMMENT"
  val COLUMN_PRIMARY_KEY = "PRIMARY KEY"
  val FULL_PROVIDER = "jdbc"
  val DEFAULT = "default"

  /**
    * Author: weiwenda Date: 2018-07-12 14:14
    * Description: similar to JDBCRelation'schema method
    * note: database must be selected before call the method
    */
  def resolveTableConnnectOnce(conn: Connection, options: JDBCOptions): StructType = {
    val url = options.url
    val table = options.tableOrQuery
    val dialect = JdbcDialects.get(url)
    val statement = conn.prepareStatement(dialect.getSchemaQuery(table))
    try {
      val rs = statement.executeQuery()
      try {
        JdbcUtils.getSchema(rs, dialect, alwaysNullable = true)
      } finally {
        rs.close()
      }
    } finally {
      statement.close()
    }
  }

}
