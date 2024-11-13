// Modifications Copyright 2021 StarRocks Limited.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.read

import com.google.common.base.Objects
import com.starrocks.connector.spark.cfg.ConfigurationOptions._
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.read.StarRocksScan.filterClauseForTest
import com.starrocks.connector.spark.rest.RestService
import com.starrocks.connector.spark.sql.DialectUtils
import com.starrocks.connector.spark.sql.schema.{StarRocksSchema, TableIdentifier}
import com.starrocks.connector.spark.util.{ConfigUtils, FieldUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class StarRocksScanBuilder(identifier: TableIdentifier,
                           schema: StructType,
                           starRocksSchema: StarRocksSchema,
                           config: Settings) extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with SupportsPushDownFilters
  /*with SupportsPushDownV2Filters*/ {

  private val log = LoggerFactory.getLogger(classOf[StarRocksScanBuilder])

  private var outputSchema: StructType = schema

  private lazy val dialect = JdbcDialects.get("")
  private lazy val sqlBuilder: V2ExpressionSQLBuilder = new V2ExpressionSQLBuilder

  private lazy val inValueLengthLimit = math.min(
    config.getIntegerProperty(STARROCKS_FILTER_QUERY_IN_MAX_COUNT, DEFAULT_FILTER_QUERY_IN_MAX_COUNT),
    STARROCKS_FILTER_QUERY_IN_VALUE_UPPER_LIMIT
  )

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    this.outputSchema = if (requiredCols.isEmpty) {
      StructType(Array(outputSchema.fields(0)))
    } else {
      StructType(outputSchema.filter(field => requiredCols.contains(field.name)))
    }

    // pass read column to BE
    // selectClause = outputSchema.fieldNames.mkString(",")
    // config.setProperty(STARROCKS_READ_FIELD, selectClause)
  }

  private var supportedFilters = Array.empty[Filter]

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition(
      DialectUtils.compileFilter(_, dialect, inValueLengthLimit).isDefined
    )

    supportedFilters = supported
    unsupported
  }

  override def pushedFilters(): Array[Filter] = supportedFilters

  override def build(): Scan = new StarRocksScan(
    identifier, outputSchema, pushedFilters(), starRocksSchema, config)

}

case class StarRocksScan(identifier: TableIdentifier,
                         var outputSchema: StructType,
                         pushedFilters: Array[Filter],
                         starRocksSchema: StarRocksSchema,
                         config: Settings)
  extends Scan
    with Batch
    with Logging
    with SupportsReportPartitioning
    with PartitionReaderFactory {

  private lazy val dialect = JdbcDialects.get("")

  private lazy val inValueLengthLimit = math.min(
    config.getIntegerProperty(STARROCKS_FILTER_QUERY_IN_MAX_COUNT, DEFAULT_FILTER_QUERY_IN_MAX_COUNT),
    STARROCKS_FILTER_QUERY_IN_VALUE_UPPER_LIMIT
  )

  private lazy val filterClause: String = {
    var clause = pushedFilters
      .flatMap(DialectUtils.compileFilter(_, dialect, inValueLengthLimit))
      .map(filter => s"($filter)")
      .mkString(" and ")

    val customFilters = config.getProperty(STARROCKS_FILTER_QUERY)
    if (StringUtils.isNotBlank(customFilters)) {
      if (ConfigUtils.isBypassRead(config)) {
        throw new IllegalStateException(s"$STARROCKS_FILTER_QUERY config isn't allowed for bypass read.")
      } else {
        clause += (if (StringUtils.isBlank(clause)) customFilters else s" and ($customFilters)")
      }
    }

    if (ConfigUtils.isVerbose(config)) {
      // only for test
      filterClauseForTest = clause
    }

    clause
  }

  private var selectClause = outputSchema.fieldNames
    .map(fn => FieldUtils.quoteIfNecessary(fn)).mkString(",")

  private var inputPartitions: Array[InputPartition] = buildPartitions

  private def buildPartitions: Array[InputPartition] = {
    if (ConfigUtils.isBypassRead(config)) {
      val partitions = RestService.splitTasks(
          identifier, selectClause, filterClause, starRocksSchema, config, log)
        .asScala.toArray
      val requiredFieldNames: Set[String] =
        outputSchema.map(_.name).toSet ++ pushedFilters.flatMap(_.references).toSet
      partitions.foreach(p => p.setRequiredFieldNames(requiredFieldNames.asJava))
      partitions.toArray
    } else {
      RestService.findPartitions(
        identifier, selectClause, filterClause, config, log).asScala.toArray
    }
  }

  override def readSchema(): StructType = outputSchema

  def setOutputSchema(outputSchema: StructType): Unit = {
    this.outputSchema = outputSchema
    this.selectClause = outputSchema.fieldNames
      .map(fn => FieldUtils.quoteIfNecessary(fn)).mkString(",")
    this.inputPartitions = buildPartitions
  }

  override def toBatch: Batch = new StarRocksScan(identifier, outputSchema, pushedFilters, starRocksSchema, config)

  override def description(): String = {
    super.description() +
      s", Table[${identifier.toFullName}]" +
      s", PushedFilters[$filterClause]" +
      s", ReadSchema[$selectClause]"
  }

  override def outputPartitioning(): Partitioning = new UnknownPartitioning(inputPartitions.length)

  override def planInputPartitions(): Array[InputPartition] = inputPartitions

  override def createReaderFactory(): PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val reader = if (ConfigUtils.isBypassRead(config)) {
      new BypassLoadPartitionReader(partition, config, starRocksSchema)
    } else {
      new StarRocksCatalogPartitionReader(partition, config, starRocksSchema)
    }
    reader.asInstanceOf[PartitionReader[InternalRow]]
  }

  override def equals(other: Any): Boolean = other match {
    case other: StarRocksScan =>
      this.identifier != null &&
        this.identifier.equals(other.identifier) &&
        this.selectClause.equals(other.selectClause) &&
        this.filterClause.equals(other.filterClause) &&
        this.starRocksSchema.equals(starRocksSchema)
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(identifier, selectClause, filterClause, starRocksSchema)
}

object StarRocksScan {

  var filterClauseForTest: String = ""

  def resetFilterClauseForTest(): Unit = {
    filterClauseForTest = ""
  }

}
