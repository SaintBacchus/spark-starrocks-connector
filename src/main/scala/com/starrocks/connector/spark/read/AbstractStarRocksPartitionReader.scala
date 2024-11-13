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

import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.rdd.{BaseValueReader, RpcValueReader}
import com.starrocks.connector.spark.rest.{BypassPartition, RpcPartition}
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import com.starrocks.connector.spark.util.{ConfigUtils, FieldUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}

import scala.collection.JavaConverters.asScalaBufferConverter

abstract class AbstractStarRocksPartitionReader[Record <: StarRocksGenericRow](
                                                                                partition: InputPartition,
                                                                                settings: Settings,
                                                                                schema: StarRocksSchema
                                                                              )
  extends PartitionReader[Record] with Logging {

  // the reader obtain data from StarRocks BE
  lazy val reader: BaseValueReader = {
    val valueReader = new RpcValueReader(partition.asInstanceOf[RpcPartition], settings)
    valueReader.init()
    valueReader
  }

  private val rowOrder: Seq[String] = if (ConfigUtils.isBypassRead(settings)) {
    partition.asInstanceOf[BypassPartition]
      .getSelectClause.split(""",\s*""").map(fn => FieldUtils.eraseQuote(fn))
  } else {
    partition.asInstanceOf[RpcPartition]
      .getSelectClause.split(""",\s*""").map(fn => FieldUtils.eraseQuote(fn))
  }

  var currentRow: Array[Any] = Array.fill(rowOrder.size)(null)

  def getNextRecord: AnyRef = {
    if (!hasNext) {
      return null
    }
    reader.rowBatch.next.asScala.zipWithIndex.foreach {
      case (s, index) if index < currentRow.length => currentRow.update(index, s)
      case _ => // nothing
    }
    currentRow
  }

  def hasNext: Boolean = {
    reader.hasNext
  }

  def next(): Boolean = {
    val hasNext = reader.rowBatchHasNext || reader.rowHasNext
    if (hasNext) {
      getNextRecord
    }
    reader.rowBatchHasNext || reader.rowHasNext
  }

  def get: Record = decode(currentRow)

  def decode(values: Array[Any]): Record

  override def close(): Unit = {
    reader.close()
  }
}