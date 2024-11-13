// Copyright 2021-present StarRocks, Inc. All rights reserved.
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

package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.sql.schema.TableIdentifier;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public class StarRocksWriteBuilder implements WriteBuilder {

    private final TableIdentifier identifier;
    private final LogicalWriteInfo logicalInfo;
    private final WriteStarRocksConfig config;
    private final StarRocksSchema starRocksSchema;

    public StarRocksWriteBuilder(TableIdentifier identifier,
                                 LogicalWriteInfo logicalInfo,
                                 WriteStarRocksConfig config,
                                 StarRocksSchema starRocksSchema) {
        this.identifier = identifier;
        this.logicalInfo = logicalInfo;
        this.config = config;
        this.starRocksSchema = starRocksSchema;
    }

    @Override
    public Write build() {
        return new StarRocksWriteImpl(identifier, logicalInfo, config, starRocksSchema);
    }

    public static class StarRocksWriteImpl implements Write, RequiresDistributionAndOrdering {

        private final TableIdentifier identifier;
        private final LogicalWriteInfo logicalInfo;
        private final WriteStarRocksConfig config;
        private final StarRocksSchema schema;

        public StarRocksWriteImpl(TableIdentifier identifier,
                                  LogicalWriteInfo logicalInfo,
                                  WriteStarRocksConfig config,
                                  StarRocksSchema schema) {
            this.identifier = identifier;
            this.logicalInfo = logicalInfo;
            this.config = config;
            this.schema = schema;
        }

        @Override
        public String description() {
            return String.format("StarRocksWrite[%s]", identifier.toFullName());
        }

        @Override
        public BatchWrite toBatch() {
            return new StarRocksWrite(identifier, logicalInfo, config, schema);
        }

        @Override
        public StreamingWrite toStreaming() {
            return new StarRocksWrite(identifier, logicalInfo, config, schema);
        }

        @Override
        public int requiredNumPartitions() {
            return config.getNumPartitions();
        }

        @Override
        public Distribution requiredDistribution() {
            if (config.getNumPartitions() <= 0) {
                return Distributions.unspecified();
            }

            // TODO is it possible to implement a distribution without shuffle like DataSet#coalesce
            String[] partitionColumns = config.getPartitionColumns();
            if (partitionColumns == null) {
                partitionColumns = logicalInfo.schema().names();
            }

            Expression[] expressions = new Expression[partitionColumns.length];
            for (int i = 0; i < partitionColumns.length; i++) {
                expressions[i] = Expressions.column(partitionColumns[i]);
            }

            return Distributions.clustered(expressions);
        }

        @Override
        public SortOrder[] requiredOrdering() {
            return new SortOrder[0];
        }
    }
}
