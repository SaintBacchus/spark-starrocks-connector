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

import com.starrocks.connector.spark.cfg.SparkSettings.addPrefix
import com.starrocks.connector.spark.read.StarRocksScan
import com.starrocks.connector.spark.sql.StarRocksTable
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig.{KEY_WRITER_MODE, WriteMode}
import com.starrocks.connector.spark.sql.write.StarRocksWriteBuilder.StarRocksWriteImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSet, DynamicPruning, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.execution._

case class StarRocksCustomStrategy(spark: SparkSession) extends SparkStrategy with Logging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    spark.sparkContext.getConf.getOption(addPrefix(KEY_WRITER_MODE)) match {
      case Some(mode) => if (WriteMode.BYPASS.not(mode)) Nil else plan match {
        case AppendData(r@DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), query, _, _, Some(write), _)
          if v1.isInstanceOf[StarRocksTable] && write.isInstanceOf[StarRocksWriteImpl] =>
          StarRocksWriteExec(write.toBatch, planLater(query), Nil) :: Nil

        case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation) =>
          // projection and filters were already pushed down in the optimizer.
          // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
          // not support columnar, a projection is added to convert the rows to UnsafeRow.
          val (runtimeFilters, postScanFilters) = filters.partition {
            case _: DynamicPruning => true
            case _ => false
          }

          withProjectAndFilter(project, postScanFilters, runtimeFilters, relation) :: Nil

        case WriteToDataSourceV2(_, write, query, customMetrics) =>
          StarRocksWriteExec(write, planLater(query), customMetrics) :: Nil

        case _ => Nil
      }

      case None => Nil
    }

  private def attributeMap(output: Seq[AttributeReference]): AttributeMap[AttributeReference] = {
    AttributeMap(output.map(o => (o, o)))
  }

  private def withProjectAndFilter(projects: Seq[NamedExpression],
                                   filters: Seq[Expression],
                                   runtimeFilters: Seq[Expression],
                                   relation: DataSourceV2ScanRelation): SparkPlan = {
    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filters.flatMap(_.references))
    val filterCondition = filters.reduceLeftOption(And)

    val scan = if (projects.nonEmpty &&
      projects.map(_.toAttribute) == projects &&
      projectSet.size == projects.size &&
      filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns = projects
        // Safe due to if above.
        .asInstanceOf[Seq[Attribute]]
        // Match original case of attributes.
        .map(attributeMap(relation.output))

      val oldScan = relation.scan.asInstanceOf[StarRocksScan]
      oldScan.setOutputSchema(requestedColumns.toStructType)
      new StarRocksBatchScanExec(
        requestedColumns,
        oldScan,
        runtimeFilters,
        relation.ordering,
        relation.relation.table,
        StoragePartitionJoinParams(relation.keyGroupedPartitioning)
      )
    } else {
      new StarRocksBatchScanExec(
        relation.output,
        relation.scan,
        runtimeFilters,
        relation.ordering,
        relation.relation.table,
        StoragePartitionJoinParams(relation.keyGroupedPartitioning)
      )
    }
    filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

    val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    if (withFilter.output != projects || !scan.supportsColumnar) {
      ProjectExec(projects, withFilter)
    } else {
      withFilter
    }
  }

}