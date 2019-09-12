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
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.xsql.{CatalogDataSource, DataSourceManager, MysqlDataSource}
import org.apache.spark.sql.xsql.DataSourceType.MYSQL
import org.apache.spark.sql.xsql.XSQLExternalCatalog.SPARK_XSQL_DATASOURCE_PREFIX
import org.apache.spark.sql.xsql.internal.config.{XSQL_DEFAULT_DATABASE, XSQL_DEFAULT_DATASOURCE}
import org.apache.spark.sql.xsql.util.Utils
import org.apache.spark.util.{Utils => SparkUtils}


class MysqlManager(conf: SparkConf) extends JDBCManager {

  def this() = {
    this(null)
  }

  import MysqlManager._
  import JDBCManager._
  import DataSourceManager._

  override def shortName(): String = MYSQL.toString

  override def getConnect(): Connection = {
    if (!connOpt.isDefined || !connOpt.get.isValid(0)) {
      SparkUtils.classForName(cachedProperties(DRIVER))
      val useSSL = cachedProperties.getOrElse(USE_SSL, "true").toBoolean
      logDebug(s"useSSL:${useSSL}")
      connOpt = Some(
        DriverManager.getConnection(
          s"${cachedProperties(URL)}?useSSL=${useSSL}",
          cachedProperties(USER),
          cachedProperties(PASSWORD)))
    }
    connOpt.get
  }

  override def setJdbcOptions(dbName: String, tbName: String): JDBCOptions = {
    val useSSL = cachedProperties.getOrElse(USE_SSL, "true").toBoolean
    logDebug(s"useSSL:${useSSL}")
    val jdbcOptions = new JDBCOptions(
      cachedProperties
        .updated(JDBCOptions.JDBC_URL, s"${cachedProperties(URL)}/${dbName}?useSSL=${useSSL}")
        .updated(JDBCOptions.JDBC_TABLE_NAME, tbName)
        .updated(JDBCOptions.JDBC_DRIVER_CLASS, cachedProperties(DRIVER))
        .updated(DATABASE, dbName)
        .toMap)
    jdbcOptions
  }

  override def isSelectedDatabase(dsName: String, dbMap: HashMap[String, Object]): Boolean = {
    val dbName = dbMap.get("TABLE_CAT").map(_.toString).getOrElse("")
    if (dbName.equals("information_schema")) {
      cachedProperties.getOrElse(IS_SHOW_SCHEMA_DATABASE, "false").toBoolean
    } else {
      val defaultSource = conf.get(XSQL_DEFAULT_DATASOURCE)
      val isDefault = dsName.equalsIgnoreCase(defaultSource)
      isSelectedDatabase(isDefault, dbName, conf.get(XSQL_DEFAULT_DATABASE))
    }
  }


  /**
    * Author: weiwenda Date: 2018-07-10 16:21
    * Description: load metaData while initialize SparkSession
    */
  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val url = cachedProperties.get(URL)
    if (url.isEmpty) {
      throw new SparkException("Data source is Mysql must have uri!")
    }
    val partitionFile = cachedProperties.get(PARTITION_CONF)
    if (partitionFile != None) {
      val partitionFilePath = Utils.getPropertiesFile(file = partitionFile.get)
      Utils.getSettingsFromFile(partitionFilePath, partitionsMap, Utils.getPartitonsFromStr)
    }

    val ds = new MysqlDataSource(
      dataSourceName,
      MYSQL,
      this,
      url.get,
      cachedProperties(USER),
      cachedProperties(PASSWORD),
      cachedProperties(VERSION))
    dataSourcesByName(ds.getName) = ds
    // Get jdbc connection, get databases
    val conn = getConnect()
    val dbMetaData = conn.getMetaData()
    val databases = dbMetaData.getCatalogs()
    val xdatabases =
      dataSourceToCatalogDatabase.getOrElseUpdate(
        ds.getName,
        new HashMap[String, CatalogDatabase])
    // Get each database's info, update dataSourceToCatalogDatabase and dbToCatalogTable
    databases.filter { isSelectedDatabase(dataSourceName, _) }.foreach { dbMap =>
      val dbName = dbMap.get("TABLE_CAT").map(_.toString).getOrElse("")
      logDebug(s"Parse mysql database: $dbName")
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
        conn.setCatalog(dbName)
        val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
        val tablePartitionsMap = partitionsMap.get(dbName)
        val tables = dbMetaData.getTables(dbName, null, "%", Array("TABLE"))
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

  /**
    * Check table exists or not.
    */
  override def tableExists(dbName: String, table: String): Boolean = {
    val conn = getConnect()
    val dbMetaData = conn.getMetaData()
    dbMetaData.getTables(dbName, null, table, Array("TABLE")).next()
  }

  override def listTables(dbName: String): Seq[String] = {
    val conn = getConnect()
    val (whiteTables, blackTables) = getWhiteAndBlackTables(dbName)
    val dbMetaData = conn.getMetaData()
    dbMetaData
      .getTables(dbName, null, "%", Array("TABLE"))
      .map { tbMap =>
        tbMap.get("TABLE_NAME").map(_.toString.toLowerCase).getOrElse("")
      }
      .filter(isSelectedTable(whiteTables, blackTables, _))
  }

  override def listDatabases(): Seq[String] = {
    val conn = getConnect()
    val dbMetaData = conn.getMetaData()
    dbMetaData
      .getCatalogs()
      .filter { isSelectedDatabase(dsName, _) }
      .map { dbMap =>
        dbMap.get("TABLE_CAT").map(_.toString).getOrElse("")
      }
  }

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val exists = if (ignoreIfExists) "IF NOT EXISTS" else ""
    val schema = dbDefinition.properties.map(e => e._1 + " " + e._2).mkString(" ", " ", " ")
    val sql = s"CREATE DATABASE ${exists} ${dbDefinition.name}${schema}"
    val statement = getConnect().createStatement
    try {
      statement.executeUpdate(sql)
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error when execute ${sql}, details:\n${e.getMessage}")
    } finally {
      statement.close()
    }
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    val sql = s"DROP DATABASE ${if (ignoreIfNotExists) "IF EXISTS" else ""} ${db}"
    val statement = getConnect().createStatement
    try {
      statement.executeUpdate(sql)
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error when execute ${sql}, details:\n${e.getMessage}")
    } finally {
      statement.close()
    }
  }

  override def dropTableSQLText(
      dbName: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): String = {
    s"DROP TABLE ${if (ignoreIfNotExists) "IF EXISTS" else ""} $table"
  }

  override def selectDatabase(conn: Connection, dbName: String): Unit = {
    conn.setCatalog(dbName)
  }

  override def getXSQLTableRowsAndIndex(identifier: TableIdentifier): (Int, Seq[String]) = {
    val dsName = identifier.dataSource.get
    val dbName = identifier.database.get
    val tbName = identifier.table
    val conn = getConnect()
    // Must select the database here, as we reuse only one connection
    selectDatabase(conn, dbName)
    (getTableRows(conn, dbName, tbName), getTableIndex(conn, dbName, tbName))
  }

  private def getTableRows(conn: Connection, dbName: String, tbName: String): Int = {
    val statement = conn.createStatement()
    val sql =
      s"""
         |select table_rows from information_schema.tables
         |where table_schema = '$dbName' and table_name = '$tbName'
       """.stripMargin
    val rs = statement.executeQuery(sql)
    if (rs.first()) {
      rs.getInt(1)
    } else {
      -1
    }
  }

  private def getTableIndex(conn: Connection, dbName: String, tbName: String): Seq[String] = {
    val index = ArrayBuffer[String]()
    val statement = conn.createStatement()
    val sql =
      s"""
         |show index from ${dbName}.${tbName}
      """.stripMargin
    val rs = statement.executeQuery(sql)
    while (rs.next()) {
      if (rs.getInt(SEQ_IN_INDEX) == 1) {
        index += rs.getString(COLUMN_NAME)
      }
    }
    index
  }

  override def isPushDown(
      runtimeConf: SQLConf,
      dataSourceName: String,
      plan: LogicalPlan): Boolean = {
    val push = runtimeConf
      .getConfString(s"${SPARK_XSQL_DATASOURCE_PREFIX}${dataSourceName}.${PUSHDOWN}", TRUE)
      .toBoolean
    val considerTableRows = runtimeConf
      .getConfString(
        s"${SPARK_XSQL_DATASOURCE_PREFIX}${dataSourceName}." +
          s"${PUSHDOWN}.${CONSIDER_TABLE_ROWS_TO_PUSHDOWN}",
        TRUE)
      .toBoolean
    val considerTableIndex = runtimeConf
      .getConfString(
        s"${SPARK_XSQL_DATASOURCE_PREFIX}${dataSourceName}." +
          s"${PUSHDOWN}.${CONSIDER_TABLE_INDEX_TO_PUSHDOWN}",
        TRUE)
      .toBoolean
    if (push && (considerTableRows || considerTableIndex)) {
      val tableIdentifierToInfo = new HashMap[String, HashMap[String, Any]]
      val tableIdentifierToJoinColumn = new HashMap[String, Seq[String]]
      val leafs = plan.collectLeaves
      // Get table's info and put in a map
      leafs.foreach { leaf =>
        leaf match {
          case UnresolvedRelation(identifier) =>
            val ds = identifier.dataSource.get
            val db = identifier.database.get
            val tbName = identifier.table
            val (tableRows, tableIndex) = getXSQLTableRowsAndIndex(identifier)
            val tableInfo = new HashMap[String, Any]
            tableInfo.put(TABLEROWS, tableRows)
            tableInfo.put(TABLEINDEX, tableIndex)
            tableIdentifierToInfo += s"${ds}_${db}_${tbName}" -> tableInfo
          case _ =>
        }
      }
      // Get tableIdentifier and join field
      plan foreach {
        case Join(left, right, _, condition) =>
          if (considerTableIndex && condition.nonEmpty) {
            val aliasToTableIdentifier = new HashMap[String, String]
            val aliasToColumnName = new HashMap[String, Seq[String]]
            @inline def getAliasToIdentifierOfJoinChild(plan: LogicalPlan): Unit = {
              plan match {
                case SubqueryAlias(alias, child) =>
                  child.collectLeaves().head match {
                    case UnresolvedRelation(identifier) =>
                      val ds = identifier.dataSource.get
                      val db = identifier.database.get
                      val tbName = identifier.table
                      aliasToTableIdentifier += alias.identifier -> s"${ds}_${db}_${tbName}"
                    case _ =>
                  }
                case UnresolvedRelation(identifier) =>
                  val ds = identifier.dataSource.get
                  val db = identifier.database.get
                  val tbName = identifier.table
                  aliasToTableIdentifier += tbName -> s"${ds}_${db}_${tbName}"
                case _ =>
              }
            }
            // Save alias and tableIdentifier for the join child
            getAliasToIdentifierOfJoinChild(left)
            getAliasToIdentifierOfJoinChild(right)
            // Collect alias and column_name on the conditon of join
            condition.get foreachUp {
              case attr: UnresolvedAttribute =>
                val oldColumnSeq = aliasToColumnName.getOrElse(attr.nameParts.head, Seq.empty)
                aliasToColumnName +=
                  attr.nameParts.head -> (oldColumnSeq ++ Seq(attr.nameParts.last))
              case _ =>
            }
            aliasToColumnName.keySet.foreach { alias =>
              tableIdentifierToJoinColumn +=
                aliasToTableIdentifier(alias) -> aliasToColumnName(alias)
            }
          }
        case _ =>
      }
      // Determine the pushdown based on the number of Rows and the index for the table
      if (leafs.size == 1) {
        // There is only one table
        val tableInfo = tableIdentifierToInfo.head._2
        considerTableRows match {
          case true =>
            // Whether tableRows of the table is less than 10000000, if satisfied then pushdown
            tableInfo(TABLEROWS).asInstanceOf[Int] < DEFAULT_PUSHDOWN_SINGLE_TABLE_ROWS
          case false =>
            // Don't consider tableRows, just pushdown
            true
        }
      } else {
        // There are more than one table
        (considerTableIndex, considerTableRows) match {
          case (true, a) =>
            // Determine if the join condition contains an index field
            val containIndex = tableIdentifierToInfo.keySet.exists { table =>
              val tableIndex = tableIdentifierToInfo(table).get(TABLEINDEX) match {
                case Some(_) =>
                  tableIdentifierToInfo(table).get(TABLEINDEX).get.asInstanceOf[Seq[String]]
                case None => Seq.empty[String]
              }
              val joinColumn = tableIdentifierToJoinColumn.get(table) match {
                case Some(_) => tableIdentifierToJoinColumn(table)
                case None => Seq.empty[String]
              }
              tableIndex.intersect(joinColumn).nonEmpty
            }
            (containIndex, a) match {
              case (true, _) => true
              case (false, true) =>
                tableIdentifierToInfo.keySet.exists { table =>
                  tableIdentifierToInfo(table).get(TABLEROWS).get.asInstanceOf[Int] <
                    DEFAULT_PUSHDOWN_MULTI_TABLE_ROWS
                }
              case (false, false) => true
            }
          case (false, true) =>
            tableIdentifierToInfo.keySet.exists { table =>
              tableIdentifierToInfo(table).get(TABLEROWS).get.asInstanceOf[Int] <
                DEFAULT_PUSHDOWN_MULTI_TABLE_ROWS
            }
          case (false, false) => true
        }
      }
    } else {
      push
    }
  }
}

object MysqlManager {
  val USE_SSL = "useSSL"
  val DATABASE = "DATABASE"
  val IS_SHOW_SCHEMA_DATABASE = "showSchemaDatabase"
  val PROVIDER = "MYSQL"
  val COLUMN_NAME = "Column_name"
  val SEQ_IN_INDEX = "Seq_in_index"
}

