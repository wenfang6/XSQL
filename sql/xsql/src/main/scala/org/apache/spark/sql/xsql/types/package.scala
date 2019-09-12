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

package org.apache.spark.sql.xsql

/**
 * Contains a type system for attributes produced by relations, including complex types like
 * structs, arrays and maps.
 */
package object types {

  /**
   * Metadata key used to store the raw Elastic Search type string in the metadata of StructField.
   *  This is relevant for datatypes that do not have a direct Spark SQL counterpart, such as TEXT
   *  and KEYWORD. We need to preserve the original type in order to invoke the correct object
   * inspector in Elastic Search.
   */
  val ELASTIC_SEARCH_TYPE_STRING = "ELASTIC_SEARCH_TYPE_STRING"
  val DRUID_TYPE_STRING = "DRUID_TYPE_STRING"

  /**
   * Metadata key used to store the raw MongoDB type string in the metadata of StructField.
   *  This is relevant for datatypes that do not have a direct Spark SQL counterpart, such as TEXT
   *  and KEYWORD. We need to preserve the original type in order to invoke the correct object
   * inspector in MongoDB.
   */
  val MONGO_TYPE_STRING = "MONGO_TYPE_STRING"

  val JDBC_COLUMN_DEFAULT = "JDBC_COLUMN_DEFAULT"

  val JDBC_COLUMN_AUTOINC = "JDBC_COLUMN_AUTO_INCREMENT"

  val PRIMARY_KEY = "PRIMARY_KEY"
}
