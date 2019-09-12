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

import java.sql.{Connection, DriverManager}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.xsql.{CatalogDataSource, DataSourceManager, OracleDataSource}
import org.apache.spark.sql.xsql.DataSourceType.ORACLE
import org.apache.spark.sql.xsql.internal.config.{XSQL_DEFAULT_DATABASE, XSQL_DEFAULT_DATASOURCE}
import org.apache.spark.sql.xsql.util.Utils
import org.apache.spark.util.{Utils => SparkUtils}

class OracleManager(conf: SparkConf) extends JDBCManager{

  def this() = {
    this(null)
  }

  import JDBCManager._
  import DataSourceManager._

  override def shortName(): String = ORACLE.toString

  override def getConnect(): Connection = {
    if (!connOpt.isDefined || !connOpt.get.isValid(0)) {
      SparkUtils.classForName(cachedProperties(DRIVER))
      connOpt = Some(
        DriverManager.getConnection(
          cachedProperties(URL),
          cachedProperties(USER),
          cachedProperties(PASSWORD)))
    }
    connOpt.get
  }

  override def setJdbcOptions(dbName: String, tbName: String): JDBCOptions = {
    val tableName = quoteIdentifier(tbName)
    val jdbcOptions = new JDBCOptions(
      cachedProperties
        .updated(JDBCOptions.JDBC_URL, cachedProperties(URL))
        .updated(JDBCOptions.JDBC_TABLE_NAME, s"$dbName.$tableName")
        .updated(JDBCOptions.JDBC_DRIVER_CLASS, cachedProperties(DRIVER))
        .toMap)
    jdbcOptions
  }

  override def isSelectedDatabase(dsName: String, dbMap: HashMap[String, Object]): Boolean = {
    val dbName = dbMap.get("TABLE_SCHEM").map(_.toString.toLowerCase).getOrElse("")
    val defaultSource = conf.get(XSQL_DEFAULT_DATASOURCE)
    val isDefault = dsName.equalsIgnoreCase(defaultSource)
    isSelectedDatabase(isDefault, dbName, conf.get(XSQL_DEFAULT_DATABASE))
  }

  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val url = cachedProperties.get(URL)
    if (url.isEmpty) {
      throw new SparkException("Data source is oracle must have uri!")
    }
    val partitionFile = cachedProperties.get(PARTITION_CONF)
    if (partitionFile != None) {
      val partitionFilePath = Utils.getPropertiesFile(file = partitionFile.get)
      Utils.getSettingsFromFile(partitionFilePath, partitionsMap, Utils.getPartitonsFromStr)
    }

    val ds = new OracleDataSource(
      dataSourceName,
      ORACLE,
      this,
      url.get,
      cachedProperties(USER),
      cachedProperties(PASSWORD),
      cachedProperties(VERSION))
    dataSourcesByName(ds.getName) = ds
    // Get jdbc connection, get schemas(databases in XSQL)
    val conn = getConnect()
    val dbMetaData = conn.getMetaData()
    val databases = dbMetaData.getSchemas()
    val xdatabases =
      dataSourceToCatalogDatabase.getOrElseUpdate(
        ds.getName,
        new HashMap[String, CatalogDatabase])
    // Get each database's info, update dataSourceToCatalogDatabase and dbToCatalogTable
    databases.filter { isSelectedDatabase(dataSourceName, _) }.foreach { dbMap =>
      val dbName = dbMap.get("TABLE_SCHEM").map(_.toString.toLowerCase).getOrElse("")
      logDebug(s"Parse oracle schema: $dbName")
      val db = CatalogDatabase(
        id = newDatabaseId,
        dataSourceName = dataSourceName,
        name = dbName,
        description = null,
        locationUri = null,
        properties = Map.empty)
      xdatabases += ((db.name, db))
    }
  }

  /**
    * Cache table.
    */
  override protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, mutable.HashMap[String, CatalogDatabase]],
      dbToCatalogTable: mutable.HashMap[Int, mutable.HashMap[String, CatalogTable]]): Unit = {
    val conn = getConnect()
    val dbMetaData = conn.getMetaData()
    val xdatabases = dataSourceToCatalogDatabase(dataSourceName)
    xdatabases.foreach {
      case (dbName, db) =>
        val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
        val tablePartitionsMap = partitionsMap.get(dbName)
        val tables = if (dbMetaData.storesUpperCaseIdentifiers) {
          dbMetaData.getTables(null, dbName.toUpperCase, "%", Array("TABLE"))
        } else {
          dbMetaData.getTables(null, dbName, "%", Array("TABLE"))
        }
        val (whiteTables, blackTables) = getWhiteAndBlackTables(dbName)
        tables
          .filter { tbMap =>
            isSelectedTable(
              whiteTables,
              blackTables,
              tbMap.get("TABLE_NAME").map(_.toString).getOrElse(""))
          }
          .foreach { tbMap =>
            val tbName = tbMap.get("TABLE_NAME").map(_.toString).getOrElse("")
            val jdbcOptions = setJdbcOptions(dbName, tbName)
            val schema = resolveTableConnnectOnce(conn, jdbcOptions)
            var partitionsParameters = new HashMap[String, String]
            if (tablePartitionsMap != None) {
              if (tablePartitionsMap.get.get(tbName) != None) {
                partitionsParameters = tablePartitionsMap.get.get(tbName).get
              }
            }
            if (partitionsParameters.nonEmpty) {
              specialProperties +=
                ((s"${dataSourceName}.${dbName}.${tbName}", partitionsParameters))
            }
            // The storage contains the following info
            // jdbcOptions:
            // 1. url
            // 2. user
            // 3. password
            // 4. type
            // 5. version
            // 6. table_name
            // 7. driver
            // 8. database
            // ...and other infos are configured in the configuration's file
            // specialProperties:
            // 1. partitons'info of this table
            val tb = CatalogTable(
              identifier = TableIdentifier(tbName, Option(dbName), Option(dataSourceName)),
              tableType = CatalogTableType.JDBC,
              storage = CatalogStorageFormat.empty.copy(
                properties = jdbcOptions.asProperties.asScala.toMap ++
                  specialProperties.
                    getOrElse(s"${dataSourceName}.${dbName}.${tbName}", Map.empty[String, String])),
              schema = schema,
              provider = Some(FULL_PROVIDER))
            xtables += ((tbName, tb))
          }
    }
  }

  override def listTables(dbName: String): Seq[String] = {
    val conn = getConnect()
    val (whiteTables, blackTables) = getWhiteAndBlackTables(dbName)
    val dbMetaData = conn.getMetaData()
    val tables = if (dbMetaData.storesUpperCaseIdentifiers) {
      dbMetaData.getTables(null, dbName.toUpperCase, "%", Array("TABLE"))
    } else {
      dbMetaData.getTables(null, dbName, "%", Array("TABLE"))
    }
    tables
      .map { tbMap =>
        tbMap.get("TABLE_NAME").map(_.toString).getOrElse("")
      }
      .filter(isSelectedTable(whiteTables, blackTables, _))
  }

  override def listDatabases(): Seq[String] = {
    val conn = getConnect()
    val dbMetaData = conn.getMetaData()
    dbMetaData
      .getSchemas
      .filter { isSelectedDatabase(dsName, _) }
      .map { dbMap =>
        dbMap.get("TABLE_SCHEM").map(_.toString.toLowerCase).getOrElse("")
      }
  }

  /**
    * Check table exists or not.
    */
  override def tableExists(dbName: String, table: String): Boolean = {
    val conn = getConnect()
    val dbMetaData = conn.getMetaData()
    if (dbMetaData.storesUpperCaseIdentifiers) {
      dbMetaData.getTables(null, dbName.toUpperCase, table, Array("TABLE")).next()
    } else {
      dbMetaData.getTables(null, dbName, table, Array("TABLE")).next()
    }
  }

  override def dropTableSQLText(
    dbName: String,
    table: String,
    ignoreIfNotExists: Boolean,
    purge: Boolean): String = {
    s"DROP TABLE ${quoteIdentifier(table)} ${if (purge) "purge" else ""}"
  }

  override def selectDatabase(conn: Connection, dbName: String): Unit = {
    val statement = conn.createStatement
    val sql = s"ALTER SESSION SET CURRENT_SCHEMA=$dbName"
    statement.executeUpdate(sql)
  }

  override def quoteIdentifier(tbName: String): String = {
    s""""$tbName""""
  }
}
object OracleManager{
  val PROVIDER = "ORACLE"
}
