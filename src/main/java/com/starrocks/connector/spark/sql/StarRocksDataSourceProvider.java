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

package com.starrocks.connector.spark.sql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.connector.spark.rest.RestClientFactory;
import com.starrocks.connector.spark.rest.TableSchemaConverter;
import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.schema.InferSchema;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.sql.schema.TableIdentifier;
import com.starrocks.format.rest.ResponseContent;
import com.starrocks.format.rest.RestClient;
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TableSchema;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.makeWriteCompatibleWithRead;

public class StarRocksDataSourceProvider implements RelationProvider,
        TableProvider,
        DataSourceRegister {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDataSourceProvider.class);

    private static final TableSchemaConverter SCHEMA_CONVERTER = new TableSchemaConverter();

    private static final ObjectMapper JSON_PARSER = new ObjectMapper();

    @Override
    public BaseRelation createRelation(SQLContext sqlContext,
                                       scala.collection.immutable.Map<String, String> parameters) {
        Map<String, String> mutableParams = new HashMap<>();
        scala.collection.immutable.Map<String, String> transParameters = DialectUtils.params(parameters, LOG);
        transParameters.toStream().foreach(key -> mutableParams.put(key._1, key._2));
        return new StarRocksRelation(sqlContext, transParameters);
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(options));
        return InferSchema.inferSchema(getStarRocksSchema(config), config);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(properties));
        return new StarRocksTable(schema, getStarRocksSchema(config), config);
    }

    @Override
    public String shortName() {
        return "starrocks";
    }

    public static StarRocksSchema getStarRocksSchema(StarRocksConfig config) {
        return getStarRocksSchema(config, new TableIdentifier(config.getDatabase(), config.getTable()));
    }

    public static StarRocksSchema getStarRocksSchema(StarRocksConfig config, TableIdentifier identifier) {
        if (config.isGetTableSchemaByJsonConfig()) {
            String schemaPath = config.getTableSchemaPath();
            String partitionsPath = config.getTablePartitionsPath();
            try {
                TableSchema tableSchema = JSON_PARSER.readValue(new File(schemaPath),
                        new TypeReference<ResponseContent<TableSchema>>() {}).getResult();
                List<TablePartition> tablePartitions = JSON_PARSER.readValue(new File(partitionsPath),
                        new TypeReference<ResponseContent<ResponseContent.PagedResult<TablePartition>>>() {})
                        .getResult().getItems();

                return SCHEMA_CONVERTER.apply(tableSchema, tablePartitions);
            } catch (IOException e) {
                throw new IllegalStateException(
                        "get table schema from json for " + identifier.toFullName() + " error, " + e.getMessage(), e);
            }
        }

        try (RestClient restClient = RestClientFactory.create(config)) {
            TableSchema tableSchema = restClient.getTableSchema(
                    identifier.getCatalog(), identifier.getDatabase(), identifier.getTable());
            List<TablePartition> tablePartitions = restClient.listTablePartitions(
                    identifier.getCatalog(), identifier.getDatabase(), identifier.getTable(), false);
            return SCHEMA_CONVERTER.apply(tableSchema, tablePartitions);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "get table schema for " + identifier.toFullName() + " error, " + e.getMessage(), e);
        }
    }

}
