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

package com.starrocks.connector.spark.sql.preprocessor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.connector.spark.sql.schema.TableModel;
import com.starrocks.proto.TabletSchema.ColumnPB;
import com.starrocks.proto.TabletSchema.KeysType;
import com.starrocks.proto.TabletSchema.TabletSchemaPB;
import com.starrocks.proto.Types;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class EtlJobConfig implements Serializable {
    // global dict
    public static final String GLOBAL_DICT_TABLE_NAME = "starrocks_global_dict_table_%d";
    // Compatible with old global dict table name in previous version
    public static final String DORIS_GLOBAL_DICT_TABLE_NAME = "doris_global_dict_table_%d";
    public static final String DISTINCT_KEY_TABLE_NAME = "starrocks_distinct_key_table_%d_%s";
    public static final String STARROCKS_INTERMEDIATE_HIVE_TABLE_NAME = "starrocks_intermediate_hive_table_%d_%s";

    // hdfsEtlPath/jobs/dbId/loadLabel/PendingTaskSignature
    private static final String ETL_OUTPUT_PATH_FORMAT = "%s/jobs/%d/%s/%d";
    private static final String ETL_OUTPUT_FILE_NAME_DESC_V1 =
            "version.label.tableId.partitionId.indexId.bucket.schemaHash.parquet";
    // tableId.partitionId.indexId.bucket.schemaHash
    public static final String TABLET_META_FORMAT = "%d.%d.%d.%d.%d";
    public static final String ETL_OUTPUT_FILE_FORMAT = "parquet";

    // dpp result
    public static final String DPP_RESULT_NAME = "dpp_result.json";

    @SerializedName(value = "tables")
    public Map<Long, EtlTable> tables;
    @SerializedName(value = "outputPath")
    public String outputPath;
    @SerializedName(value = "outputFilePattern")
    public String outputFilePattern;
    @SerializedName(value = "label")
    public String label;
    @SerializedName(value = "properties")
    public EtlJobProperty properties;
    @SerializedName(value = "configVersion")
    public ConfigVersion configVersion;

    public EtlJobConfig(Map<Long, EtlTable> tables, String outputFilePattern, String label, EtlJobProperty properties) {
        this.tables = tables;
        // set outputPath when submit etl job
        this.outputPath = null;
        this.outputFilePattern = outputFilePattern;
        this.label = label;
        this.properties = properties;
        this.configVersion = ConfigVersion.V1;
    }

    @Override
    public String toString() {
        return "EtlJobConfig{" +
                "tables=" + tables +
                ", outputPath='" + outputPath + '\'' +
                ", outputFilePattern='" + outputFilePattern + '\'' +
                ", label='" + label + '\'' +
                ", properties=" + properties +
                ", version=" + configVersion +
                '}';
    }

    public String getOutputPath() {
        return outputPath;
    }

    public static String getOutputPath(String hdfsEtlPath, long dbId, String loadLabel, long taskSignature) {
        return String.format(ETL_OUTPUT_PATH_FORMAT, hdfsEtlPath, dbId, loadLabel, taskSignature);
    }

    public static String getOutputFilePattern(String loadLabel, FilePatternVersion filePatternVersion) {
        return String.format("%s.%s.%s.%s", filePatternVersion.name(), loadLabel, TABLET_META_FORMAT,
                ETL_OUTPUT_FILE_FORMAT);
    }

    public static String getDppResultFilePath(String outputPath) {
        return outputPath + "/" + DPP_RESULT_NAME;
    }

    public static String getTabletMetaStr(String filePath) throws Exception {
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String[] fileNameArr = fileName.split("\\.");
        // check file version
        switch (FilePatternVersion.valueOf(fileNameArr[0])) {
            case V1:
                // version.label.tableId.partitionId.indexId.bucket.schemaHash.parquet
                if (fileNameArr.length != ETL_OUTPUT_FILE_NAME_DESC_V1.split("\\.").length) {
                    throw new Exception("etl output file name error, format: " + ETL_OUTPUT_FILE_NAME_DESC_V1
                            + ", name: " + fileName);
                }
                long tableId = Long.parseLong(fileNameArr[2]);
                long partitionId = Long.parseLong(fileNameArr[3]);
                long indexId = Long.parseLong(fileNameArr[4]);
                int bucket = Integer.parseInt(fileNameArr[5]);
                int schemaHash = Integer.parseInt(fileNameArr[6]);
                // tableId.partitionId.indexId.bucket.schemaHash
                return String.format(TABLET_META_FORMAT, tableId, partitionId, indexId, bucket, schemaHash);
            default:
                throw new Exception("etl output file version error. version: " + fileNameArr[0]);
        }
    }

    public String configToJson() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.addDeserializationExclusionStrategy(new HiddenAnnotationExclusionStrategy());
        Gson gson = gsonBuilder.create();
        return gson.toJson(this);
    }

    public static EtlJobConfig configFromJson(String jsonConfig) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();
        return gson.fromJson(jsonConfig, EtlJobConfig.class);
    }

    public static class EtlJobProperty implements Serializable {
        @SerializedName(value = "strictMode")
        public boolean strictMode;
        @SerializedName(value = "timezone")
        public String timezone;

        @Override
        public String toString() {
            return "EtlJobProperty{" +
                    "strictMode=" + strictMode +
                    ", timezone='" + timezone + '\'' +
                    '}';
        }
    }

    public enum ConfigVersion {
        V1
    }

    public enum FilePatternVersion {
        V1
    }

    public enum SourceType {
        FILE,
        HIVE
    }

    @Data
    public static class EtlTable implements Serializable {
        @SerializedName(value = "indexes")
        public List<EtlIndex> indexes;
        @SerializedName(value = "partitionInfo")
        public EtlPartitionInfo partitionInfo;
        @SerializedName(value = "fileGroups")
        public List<EtlFileGroup> fileGroups;
        @SerializedName("bfFpp")
        private double bfFpp;
        @SerializedName("compressionType")
        private String compressionType;

        private Map<Long, Long> tabletPartitionIndex;

        public EtlTable() {
        }

        public EtlTable(List<EtlIndex> etlIndexes, EtlPartitionInfo etlPartitionInfo, String compressionType, double bfFpp) {
            this.indexes = etlIndexes;
            this.partitionInfo = etlPartitionInfo;
            this.fileGroups = Lists.newArrayList();
            this.tabletPartitionIndex = new HashMap<>();
            for (EtlPartition p : partitionInfo.getPartitions())  {
                long partitionId = p.getPartitionId();
                for (Long tabletId : p.getTabletIds()) {
                    tabletPartitionIndex.put(tabletId, partitionId);
                }
            }
            this.bfFpp = bfFpp;
            this.compressionType = compressionType;
        }

        public void addFileGroup(EtlFileGroup etlFileGroup) {
            fileGroups.add(etlFileGroup);
        }

        public TabletSchemaPB toPbTabletSchema() {
            return toPbTabletSchema(new ArrayList<>(0));
        }

        public TabletSchemaPB toPbTabletSchema(List<String> columnNames) {
            EtlIndex etlIndex = indexes.get(0);

            TabletSchemaPB.Builder builder = TabletSchemaPB.newBuilder()
                    .setId(etlIndex.schemaId)
                    .setKeysType(toPbTabletSchema(etlIndex.indexType))
                    .setCompressionType(toPbCompressionType(compressionType))
                    .setBfFpp(bfFpp)
                    .setNumShortKeyColumns(etlIndex.getShortKeyColumnCount())
                    .setNumRowsPerRowBlock(1024)
                    .addAllSortKeyIdxes(etlIndex.getSortKeyIdxes())
                    .addAllSortKeyUniqueIds(etlIndex.getSortKeyUniqueIds());

            List<EtlColumn> columns = etlIndex.getColumns();
            if (CollectionUtils.isEmpty(columnNames)) {
                builder.addAllColumn(columns.stream().map(EtlColumn::toPbColumn).collect(Collectors.toList()));
            } else {
                int maxColUniqueId = 0;

                Map<String, EtlColumn> nameToColumns =
                        columns.stream().collect(Collectors.toMap(e -> e.columnName, e -> e));
                for (String colName : columnNames) {
                    EtlColumn etlColumn = nameToColumns.get(colName);
                    if (null == etlColumn) {
                        throw new IllegalStateException("column not found: " + colName);
                    }
                    maxColUniqueId = Math.max(maxColUniqueId, etlColumn.uniqueId);
                    builder.addColumn(etlColumn.toPbColumn());
                }
                builder.setNextColumnUniqueId(maxColUniqueId + 1);
            }

            return builder.build();
        }

        KeysType toPbTabletSchema(String indexType) {
            return TableModel.of(indexType)
                    .map(TableModel::toKeysType)
                    .orElseThrow(() -> new IllegalStateException("unknown index type: " + indexType));
        }

        Types.CompressionTypePB toPbCompressionType(String type) {
            return Types.CompressionTypePB.valueOf(type != null ? type : "LZ4_FRAME");
        }

        public long getPartitionId(long tabletId) {
            return tabletPartitionIndex.getOrDefault(tabletId, -1L);
        }

        @Override
        public String toString() {
            return "EtlTable{" +
                    "indexes=" + indexes +
                    ", partitionInfo=" + partitionInfo +
                    ", fileGroups=" + fileGroups +
                    '}';
        }
    }

    public static class EtlColumn implements Serializable {
        static int index = 0;
        @SerializedName(value = "columnName")
        public String columnName;
        @SerializedName(value = "columnType")
        public String columnType;
        @SerializedName(value = "isAllowNull")
        public Boolean isAllowNull;
        @SerializedName(value = "isKey")
        public Boolean isKey;
        @SerializedName(value = "aggregationType")
        public String aggregationType;
        @SerializedName(value = "defaultValue")
        public String defaultValue;
        @SerializedName(value = "stringLength")
        public Integer stringLength;
        @SerializedName(value = "precision")
        public Integer precision;
        @SerializedName(value = "scale")
        public Integer scale;
        @SerializedName(value = "defineExpr")
        public String defineExpr;
        @SerializedName("uniqueId")
        public int uniqueId;

        // for unit test
        public EtlColumn() {
        }

        public EtlColumn(String columnName,
                         String columnType,
                         boolean isAllowNull,
                         boolean isKey,
                         String aggregationType,
                         String defaultValue,
                         int stringLength,
                         int precision,
                         int scale,
                         int uniqueId) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.isAllowNull = isAllowNull;
            this.isKey = isKey;
            this.aggregationType = aggregationType;
            this.defaultValue = defaultValue;
            this.stringLength = stringLength;
            this.precision = precision;
            this.scale = scale;
            this.defineExpr = null;
            this.uniqueId = uniqueId;
        }

        ColumnPB toPbColumn() {
            return ColumnPB.newBuilder()
                    .setName(columnName)
                    .setUniqueId(uniqueId)
                    .setIsKey(isKey)
                    .setIsNullable(isAllowNull)
                    .setType(columnType)
                    .setLength(stringLength)
                    .setIndexLength(stringLength)
                    .setPrecision(precision)
                    .setFrac(scale)
                    .setAggregation(null == aggregationType ? "NONE" : aggregationType)
                    .build();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("columnName", columnName)
                    .append("columnType", columnType)
                    .append("isAllowNull", isAllowNull)
                    .append("isKey", isKey)
                    .append("aggregationType", aggregationType)
                    .append("defaultValue", defaultValue)
                    .append("stringLength", stringLength)
                    .append("precision", precision)
                    .append("scale", scale)
                    .append("defineExpr", defineExpr)
                    .append("uniqueId", uniqueId)
                    .toString();
        }
    }

    public static class EtlIndexComparator implements Comparator<EtlIndex> {
        @Override
        public int compare(EtlIndex a, EtlIndex b) {
            int diff = a.columns.size() - b.columns.size();
            if (diff == 0) {
                return 0;
            } else if (diff > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    @Data
    public static class EtlIndex implements Serializable {
        @SerializedName(value = "indexId")
        public long indexId;
        @SerializedName(value = "columns")
        public List<EtlColumn> columns;
        // @SerializedName(value = "schemaHash")
        // public int schemaHash;
        @SerializedName(value = "indexType")
        public String indexType;
        @SerializedName(value = "isBaseIndex")
        public boolean isBaseIndex;

        @SerializedName(value = "schemaId")
        private long schemaId;
        @SerializedName(value = "storageType")
        private String storageType;
        @SerializedName(value = "sortKeyIdxes")
        public List<Integer> sortKeyIdxes;
        @SerializedName(value = "sortKeyUniqueIds")
        public List<Integer> sortKeyUniqueIds;
        @SerializedName(value = "schemaVersion")
        private int schemaVersion = 0;
        @SerializedName(value = "shortKeyColumnCount")
        private short shortKeyColumnCount;
        public EtlIndex() {
        }

        public EtlIndex(long indexId,
                        List<EtlColumn> etlColumns,
                        // int schemaHash,
                        String indexType,
                        boolean isBaseIndex) {
            this.indexId = indexId;
            this.columns = etlColumns;
            // this.schemaHash = schemaHash;
            this.indexType = indexType;
            this.isBaseIndex = isBaseIndex;
        }

        public EtlColumn getColumn(String name) {
            for (EtlColumn column : columns) {
                if (column.columnName.equals(name)) {
                    return column;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "EtlIndex{" +
                    "indexId=" + indexId +
                    ", columns=" + columns +
                    // ", schemaHash=" + schemaHash +
                    ", indexType='" + indexType + '\'' +
                    ", isBaseIndex=" + isBaseIndex +
                    '}';
        }
    }

    @Data
    public static class EtlPartitionInfo implements Serializable {
        @SerializedName(value = "partitionType")
        public String partitionType;
        @SerializedName(value = "partitionColumnRefs")
        public List<String> partitionColumnRefs;
        @SerializedName(value = "distributionColumnRefs")
        public List<String> distributionColumnRefs;
        @SerializedName(value = "partitions")
        public List<EtlPartition> partitions;

        public EtlPartitionInfo() {
        }

        public EtlPartitionInfo(String partitionType,
                                List<String> partitionColumnRefs,
                                List<String> distributionColumnRefs,
                                List<EtlPartition> etlPartitions) {
            this.partitionType = partitionType;
            this.partitionColumnRefs = partitionColumnRefs;
            this.distributionColumnRefs = distributionColumnRefs;
            this.partitions = etlPartitions;
        }

        @Override
        public String toString() {
            return "EtlPartitionInfo{" +
                    "partitionType='" + partitionType + '\'' +
                    ", partitionColumnRefs=" + partitionColumnRefs +
                    ", distributionColumnRefs=" + distributionColumnRefs +
                    ", partitions=" + partitions +
                    '}';
        }
    }

    @Data
    public static class EtlPartition implements Serializable {
        @SerializedName(value = "partitionId")
        public Long partitionId;
        @SerializedName(value = "startKeys")
        public List<Object> startKeys;
        @SerializedName(value = "endKeys")
        public List<Object> endKeys;
        @SerializedName(value = "isMinPartition")
        public Boolean isMinPartition;
        @SerializedName(value = "isMaxPartition")
        public Boolean isMaxPartition;
        @SerializedName(value = "bucketNum")
        public Integer bucketNum;
        @SerializedName(value = "storagePath")
        public String storagePath;
        @SerializedName(value = "tabletIds")
        public List<Long> tabletIds;
        @SerializedName(value = "backendIds")
        public List<Long> backendIds;

        public EtlPartition() {
        }

        public EtlPartition(Long partitionId,
                            List<Object> startKeys,
                            List<Object> endKeys,
                            Boolean isMinPartition,
                            Boolean isMaxPartition,
                            Integer bucketNum,
                            String storagePath,
                            List<Long> tabletIds,
                            List<Long> backendIds) {
            this.partitionId = partitionId;
            this.startKeys = startKeys;
            this.endKeys = endKeys;
            this.isMinPartition = isMinPartition;
            this.isMaxPartition = isMaxPartition;
            this.bucketNum = bucketNum;
            this.storagePath = storagePath;
            this.tabletIds = tabletIds;
            this.backendIds = backendIds;
        }

        @Override
        public String toString() {
            return "EtlPartition{" +
                    "partitionId=" + partitionId +
                    ", startKeys=" + startKeys +
                    ", endKeys=" + endKeys +
                    ", isMinPartition=" + isMinPartition +
                    ", isMaxPartition=" + isMaxPartition +
                    ", bucketNum=" + bucketNum +
                    ", storagePath=" + storagePath +
                    '}';
        }
    }

    @Data
    public static class EtlFileGroup implements Serializable {
        @SerializedName(value = "sourceType")
        public SourceType sourceType = SourceType.FILE;
        @SerializedName(value = "filePaths")
        public List<String> filePaths;
        @SerializedName(value = "fileFieldNames")
        public List<String> fileFieldNames;
        @SerializedName(value = "columnsFromPath")
        public List<String> columnsFromPath;
        @SerializedName(value = "columnSeparator")
        public String columnSeparator;
        @SerializedName(value = "lineDelimiter")
        public String lineDelimiter;
        @SerializedName(value = "isNegative")
        public boolean isNegative;
        @SerializedName(value = "fileFormat")
        public String fileFormat;
        @SerializedName(value = "columnMappings")
        public Map<String, EtlColumnMapping> columnMappings;
        @SerializedName(value = "where")
        public String where;
        @SerializedName(value = "partitions")
        public List<Long> partitions;
        @SerializedName(value = "hiveDbTableName")
        public String hiveDbTableName;
        @SerializedName(value = "hiveTableProperties")
        public Map<String, String> hiveTableProperties;

        // hive db table used in dpp, not serialized
        // set with hiveDbTableName (no bitmap column) or IntermediateHiveTable (created by global dict builder) in spark etl job
        public String dppHiveDbTableName;

        // for data infile path
        public EtlFileGroup(SourceType sourceType, List<String> filePaths, List<String> fileFieldNames,
                            List<String> columnsFromPath, String columnSeparator, String lineDelimiter,
                            boolean isNegative, String fileFormat, Map<String, EtlColumnMapping> columnMappings,
                            String where, List<Long> partitions) {
            this.sourceType = sourceType;
            this.filePaths = filePaths;
            this.fileFieldNames = fileFieldNames;
            this.columnsFromPath = columnsFromPath;
            this.columnSeparator = Strings.isNullOrEmpty(columnSeparator) ? "\t" : columnSeparator;
            this.lineDelimiter = lineDelimiter;
            this.isNegative = isNegative;
            this.fileFormat = fileFormat;
            this.columnMappings = columnMappings;
            this.where = where;
            this.partitions = partitions;
        }

        // for data from table
        public EtlFileGroup(SourceType sourceType, List<String> fileFieldNames, String hiveDbTableName,
                            Map<String, String> hiveTableProperties, boolean isNegative,
                            Map<String, EtlColumnMapping> columnMappings, String where, List<Long> partitions) {
            this.sourceType = sourceType;
            this.fileFieldNames = fileFieldNames;
            this.hiveDbTableName = hiveDbTableName;
            this.hiveTableProperties = hiveTableProperties;
            this.isNegative = isNegative;
            this.columnMappings = columnMappings;
            this.where = where;
            this.partitions = partitions;
        }

        @Override
        public String toString() {
            return "EtlFileGroup{" +
                    "sourceType=" + sourceType +
                    ", filePaths=" + filePaths +
                    ", fileFieldNames=" + fileFieldNames +
                    ", columnsFromPath=" + columnsFromPath +
                    ", columnSeparator='" + columnSeparator + '\'' +
                    ", lineDelimiter='" + lineDelimiter + '\'' +
                    ", isNegative=" + isNegative +
                    ", fileFormat='" + fileFormat + '\'' +
                    ", columnMappings=" + columnMappings +
                    ", where='" + where + '\'' +
                    ", partitions=" + partitions +
                    ", hiveDbTableName='" + hiveDbTableName + '\'' +
                    ", hiveTableProperties=" + hiveTableProperties +
                    '}';
        }
    }

    /**
     * FunctionCallExpr = functionName(args)
     * For compatibility with old designed functions used in Hadoop MapReduce etl
     * <p>
     * expr is more general, like k1 + 1, not just FunctionCall
     */
    public static class EtlColumnMapping implements Serializable {
        @SerializedName(value = "functionName")
        public String functionName;
        @SerializedName(value = "args")
        public List<String> args;
        @SerializedName(value = "expr")
        public String expr;

        private static Map<String, String> functionMap = new ImmutableMap.Builder<String, String>()
                .put("md5sum", "md5").build();

        public EtlColumnMapping(String functionName, List<String> args) {
            this.functionName = functionName;
            this.args = args;
        }

        public EtlColumnMapping(String expr) {
            this.expr = expr;
        }

        public String toDescription() {
            StringBuilder sb = new StringBuilder();
            if (functionName == null) {
                sb.append(expr);
            } else {
                if (functionMap.containsKey(functionName)) {
                    sb.append(functionMap.get(functionName));
                } else {
                    sb.append(functionName);
                }
                sb.append("(");
                if (args != null) {
                    for (String arg : args) {
                        sb.append(arg);
                        sb.append(",");
                    }
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return "EtlColumnMapping{" +
                    "functionName='" + functionName + '\'' +
                    ", args=" + args +
                    ", expr=" + expr +
                    '}';
        }
    }

    public static class HiddenAnnotationExclusionStrategy implements ExclusionStrategy {
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getAnnotation(SerializedName.class) == null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }
    }
}