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

package org.apache.spark.sql.execution.datasources.v2

import com.google.common.base.Objects
import com.starrocks.connector.spark.read.StarRocksScan
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._

class StarRocksBatchScanExec(override val output: Seq[AttributeReference],
                             @transient override val scan: Scan,
                             override val runtimeFilters: Seq[Expression],
                             override val ordering: Option[Seq[SortOrder]] = None,
                             @transient override val table: Table,
                             override val spjParams: StoragePartitionJoinParams = StoragePartitionJoinParams())
  extends BatchScanExec(output, scan, runtimeFilters, ordering, table, spjParams) {

  override def doCanonicalize(): BatchScanExec = {
    val batchExec = this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output))
    batchExec
  }

  override def nodeName: String = {
    s"StarRocksBatchScan ${table.name()}".trim
  }

  // batch has output(selectClause) equal
  override def equals(other: Any): Boolean = other match {
    case other: BatchScanExec if batch.isInstanceOf[StarRocksScan] =>
      this.batch.equals(other.batch) &&
        this.runtimeFilters.equals(other.runtimeFilters) &&
        this.spjParams.equals(other.spjParams)
    case _ =>
      false
  }

  // batch has output(selectClause) hashcode
  override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters, spjParams, ordering)

}