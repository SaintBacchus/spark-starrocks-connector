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
import com.google.gson.annotations.SerializedName;
import com.starrocks.format.StarRocksUtils;
import com.starrocks.format.rest.model.Column;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.collections4.CollectionUtils;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.starrocks.format.StarRocksUtils.STARROKCS_TABLE_ID;
import static com.starrocks.format.StarRocksUtils.STARROKCS_TABLE_KEY_TYPE;

@Deprecated
public class EtlJobConfig implements Serializable {

    private static final long serialVersionUID = 1296428870540141623L;

    @SerializedName(value = "tables")
    private Map<Long, EtlTable> tables;

    @SerializedName(value = "outputPath")
    private String outputPath;

    @SerializedName(value = "outputFilePattern")
    private String outputFilePattern;

    @SerializedName(value = "label")
    private String label;

    @SerializedName(value = "properties")
    private EtlJobProperty properties;

    @SerializedName(value = "configVersion")
    private ConfigVersion configVersion;

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

    public static class EtlJobProperty implements Serializable {

        private static final long serialVersionUID = -8837955983454262294L;

        @SerializedName(value = "strictMode")
        private boolean strictMode;

        @SerializedName(value = "timezone")
        private String timezone;

        @Override
        public String toString() {
            return "EtlJobProperty{" +
                    "strictMode=" + strictMode +
                    ", timezone='" + timezone + '\'' +
                    '}';
        }

        public boolean isStrictMode() {
            return strictMode;
        }

        public String getTimezone() {
            return timezone;
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

    public static class EtlTable implements Serializable {

        private static final long serialVersionUID = 363034002729151431L;

        @SerializedName(value = "indexes")
        private List<EtlIndex> indexes;

        @SerializedName(value = "partitionInfo")
        private EtlPartitionInfo partitionInfo;

        @SerializedName(value = "fileGroups")
        private List<EtlFileGroup> fileGroups;

        public EtlTable() {
        }

        public EtlTable(List<EtlIndex> etlIndexes, EtlPartitionInfo etlPartitionInfo) {
            this.indexes = etlIndexes;
            this.partitionInfo = etlPartitionInfo;
            this.fileGroups = Lists.newArrayList();
        }

        public void addFileGroup(EtlFileGroup etlFileGroup) {
            fileGroups.add(etlFileGroup);
        }

        public Schema toArrowSchema(ZoneId tz) {
            return this.toArrowSchema(new ArrayList<>(0), tz);
        }

        public Schema toArrowSchema(List<String> columnNames, ZoneId tz) {
            EtlIndex index = indexes.get(0);

            List<Field> fields = new ArrayList<>();
            if (CollectionUtils.isEmpty(columnNames)) {
                fields.addAll(index.columns.stream()
                        .map(col -> col.toArrowField(tz)).collect(Collectors.toList()));
            } else {
                Map<String, EtlColumn> nameToColumns = index.columns.stream()
                        .collect(Collectors.toMap(e -> e.columnName, e -> e));
                for (String colName : columnNames) {
                    EtlColumn etlColumn = nameToColumns.get(colName);
                    if (null == etlColumn) {
                        throw new IllegalStateException("Column not found: " + colName);
                    }
                    fields.add(etlColumn.toArrowField(tz));
                }
            }

            return new Schema(
                    fields,
                    new HashMap<String, String>() {
                        private static final long serialVersionUID = -3654733348923452351L;

                        {
                            put(STARROKCS_TABLE_ID, String.valueOf(index.indexId));
                            put(STARROKCS_TABLE_KEY_TYPE, index.indexType);
                        }
                    }
            );
        }

        @Override
        public String toString() {
            return "EtlTable{" +
                    "indexes=" + indexes +
                    ", partitionInfo=" + partitionInfo +
                    ", fileGroups=" + fileGroups +
                    '}';
        }

        public List<EtlIndex> getIndexes() {
            return indexes;
        }

        public EtlPartitionInfo getPartitionInfo() {
            return partitionInfo;
        }

        public List<EtlFileGroup> getFileGroups() {
            return fileGroups;
        }
    }

    public static class EtlColumn implements Serializable {

        private static final long serialVersionUID = -7442811990090735000L;

        @SerializedName(value = "columnName")
        private String columnName;

        @SerializedName(value = "columnType")
        private Column.Type columnType;

        @SerializedName(value = "aggregationType")
        private String aggregationType;

        @SerializedName(value = "isKey")
        private boolean key;

        @SerializedName(value = "isAllowNull")
        private boolean allowNull;

        @SerializedName("isAutoIncrement")
        private boolean autoIncrement;

        @SerializedName(value = "defaultValue")
        private String defaultValue;

        @SerializedName(value = "defineExpr")
        private String defineExpr;

        @SerializedName("uniqueId")
        private int uniqueId;

        // for unit test
        public EtlColumn() {
        }

        public EtlColumn(String columnName,
                         Column.Type columnType,
                         boolean allowNull,
                         boolean autoIncrement,
                         boolean key,
                         String aggregationType,
                         String defaultValue,
                         int uniqueId) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.allowNull = allowNull;
            this.autoIncrement = autoIncrement;
            this.key = key;
            this.aggregationType = aggregationType;
            this.defaultValue = defaultValue;
            this.defineExpr = null;
            this.uniqueId = uniqueId;
        }

        Field toArrowField(ZoneId tz) {
            return StarRocksUtils.toArrowField(columnName, columnType, tz);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", "[", "]")
                    .add("columnName='" + columnName + "'")
                    .add("columnType=" + columnType)
                    .add("aggregationType='" + aggregationType + "'")
                    .add("isKey=" + key)
                    .add("allowNull=" + allowNull)
                    .add("autoIncrement=" + autoIncrement)
                    .add("defaultValue='" + defaultValue + "'")
                    .add("defineExpr='" + defineExpr + "'")
                    .add("uniqueId=" + uniqueId)
                    .toString();
        }

        public String getColumnName() {
            return columnName;
        }

        public Column.Type getColumnType() {
            return columnType;
        }

        public String getAggregationType() {
            return aggregationType;
        }

        public boolean isKey() {
            return key;
        }

        public boolean isAllowNull() {
            return allowNull;
        }

        public boolean isAutoIncrement() {
            return autoIncrement;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public String getDefineExpr() {
            return defineExpr;
        }

        public int getUniqueId() {
            return uniqueId;
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

    public static class EtlIndex implements Serializable {

        private static final long serialVersionUID = -7886373791486829055L;

        @SerializedName(value = "indexId")
        private long indexId;

        @SerializedName(value = "columns")
        private List<EtlColumn> columns;

        @SerializedName(value = "indexType")
        private String indexType;

        @SerializedName(value = "isBaseIndex")
        private boolean baseIndex;

        public EtlIndex() {
        }

        public EtlIndex(long indexId,
                        List<EtlColumn> etlColumns,
                        // int schemaHash,
                        String indexType,
                        boolean baseIndex) {
            this.indexId = indexId;
            this.columns = etlColumns;
            // this.schemaHash = schemaHash;
            this.indexType = indexType;
            this.baseIndex = baseIndex;
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
                    ", isBaseIndex=" + baseIndex +
                    '}';
        }

        public long getIndexId() {
            return indexId;
        }

        public List<EtlColumn> getColumns() {
            return columns;
        }

        public String getIndexType() {
            return indexType;
        }

        public boolean isBaseIndex() {
            return baseIndex;
        }

    }

    public static class EtlPartitionInfo implements Serializable {

        private static final long serialVersionUID = 3300197736693620106L;

        @SerializedName(value = "partitionType")
        private String partitionType;

        @SerializedName(value = "partitionColumnRefs")
        private List<String> partitionColumnRefs;

        @SerializedName(value = "distributionColumnRefs")
        private List<String> distributionColumnRefs;

        @SerializedName(value = "partitions")
        private List<EtlPartition> partitions;

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

        public String getPartitionType() {
            return partitionType;
        }

        public List<String> getPartitionColumnRefs() {
            return partitionColumnRefs;
        }

        public List<String> getDistributionColumnRefs() {
            return distributionColumnRefs;
        }

        public List<EtlPartition> getPartitions() {
            return partitions;
        }
    }

    public static class EtlPartition implements Serializable {

        private static final long serialVersionUID = -6016118376366100710L;

        @SerializedName(value = "partitionId")
        private Long partitionId;

        @SerializedName(value = "startKeys")
        private List<Object> startKeys;

        @SerializedName(value = "endKeys")
        private List<Object> endKeys;

        @SerializedName(value = "inKeys")
        private List<List<Object>> inKeys;

        @SerializedName(value = "isMinPartition")
        private boolean minPartition;

        @SerializedName(value = "isMaxPartition")
        private boolean maxPartition;

        @SerializedName(value = "bucketNum")
        private Integer bucketNum;

        @SerializedName(value = "storagePath")
        private String storagePath;

        @SerializedName(value = "tabletIds")
        private List<Long> tabletIds;

        @SerializedName(value = "backendIds")
        private List<Long> backendIds;

        public EtlPartition() {
        }

        public EtlPartition(Long partitionId,
                            List<Object> startKeys,
                            List<Object> endKeys,
                            List<List<Object>> inKeys,
                            boolean minPartition,
                            boolean maxPartition,
                            Integer bucketNum,
                            String storagePath,
                            List<Long> tabletIds,
                            List<Long> backendIds) {
            this.partitionId = partitionId;
            this.startKeys = startKeys;
            this.endKeys = endKeys;
            this.inKeys = inKeys;
            this.minPartition = minPartition;
            this.maxPartition = maxPartition;
            this.bucketNum = bucketNum;
            this.storagePath = storagePath;
            this.tabletIds = tabletIds;
            this.backendIds = backendIds;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", "[", "]")
                    .add("partitionId=" + partitionId)
                    .add("startKeys=" + startKeys)
                    .add("endKeys=" + endKeys)
                    .add("inKeys=" + inKeys)
                    .add("minPartition=" + minPartition)
                    .add("maxPartition=" + maxPartition)
                    .add("bucketNum=" + bucketNum)
                    .add("storagePath='" + storagePath + "'")
                    .add("tabletIds=" + tabletIds)
                    .add("backendIds=" + backendIds)
                    .toString();
        }

        public Long getPartitionId() {
            return partitionId;
        }

        public List<Object> getStartKeys() {
            return startKeys;
        }

        public List<Object> getEndKeys() {
            return endKeys;
        }

        public List<List<Object>> getInKeys() {
            return inKeys;
        }

        public boolean isMinPartition() {
            return minPartition;
        }

        public boolean isMaxPartition() {
            return maxPartition;
        }

        public Integer getBucketNum() {
            return bucketNum;
        }

        public String getStoragePath() {
            return storagePath;
        }

        public List<Long> getTabletIds() {
            return tabletIds;
        }

        public List<Long> getBackendIds() {
            return backendIds;
        }
    }

    public static class EtlFileGroup implements Serializable {

        private static final long serialVersionUID = -7177962445754842299L;

        @SerializedName(value = "sourceType")
        private SourceType sourceType = SourceType.FILE;

        @SerializedName(value = "filePaths")
        private List<String> filePaths;

        @SerializedName(value = "fileFieldNames")
        private List<String> fileFieldNames;

        @SerializedName(value = "columnsFromPath")
        private List<String> columnsFromPath;

        @SerializedName(value = "columnSeparator")
        private String columnSeparator;

        @SerializedName(value = "lineDelimiter")
        private String lineDelimiter;

        @SerializedName(value = "isNegative")
        private boolean isNegative;

        @SerializedName(value = "fileFormat")
        private String fileFormat;

        @SerializedName(value = "columnMappings")
        private Map<String, EtlColumnMapping> columnMappings;

        @SerializedName(value = "where")
        private String where;

        @SerializedName(value = "partitions")
        private List<Long> partitions;

        @SerializedName(value = "hiveDbTableName")
        private String hiveDbTableName;

        @SerializedName(value = "hiveTableProperties")
        private Map<String, String> hiveTableProperties;

        // for data infile path
        public EtlFileGroup(SourceType sourceType,
                            List<String> filePaths,
                            List<String> fileFieldNames,
                            List<String> columnsFromPath,
                            String columnSeparator,
                            String lineDelimiter,
                            boolean isNegative,
                            String fileFormat,
                            Map<String, EtlColumnMapping> columnMappings,
                            String where,
                            List<Long> partitions) {
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
        public EtlFileGroup(SourceType sourceType,
                            List<String> fileFieldNames,
                            String hiveDbTableName,
                            Map<String, String> hiveTableProperties,
                            boolean isNegative,
                            Map<String, EtlColumnMapping> columnMappings,
                            String where,
                            List<Long> partitions) {
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

        public SourceType getSourceType() {
            return sourceType;
        }

        public List<String> getFilePaths() {
            return filePaths;
        }

        public List<String> getFileFieldNames() {
            return fileFieldNames;
        }

        public List<String> getColumnsFromPath() {
            return columnsFromPath;
        }

        public String getColumnSeparator() {
            return columnSeparator;
        }

        public String getLineDelimiter() {
            return lineDelimiter;
        }

        public boolean isNegative() {
            return isNegative;
        }

        public String getFileFormat() {
            return fileFormat;
        }

        public Map<String, EtlColumnMapping> getColumnMappings() {
            return columnMappings;
        }

        public String getWhere() {
            return where;
        }

        public List<Long> getPartitions() {
            return partitions;
        }

        public String getHiveDbTableName() {
            return hiveDbTableName;
        }

        public Map<String, String> getHiveTableProperties() {
            return hiveTableProperties;
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
        private String functionName;

        @SerializedName(value = "args")
        private List<String> args;

        @SerializedName(value = "expr")
        private String expr;

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

        public String getFunctionName() {
            return functionName;
        }

        public List<String> getArgs() {
            return args;
        }

        public String getExpr() {
            return expr;
        }
    }

    public Map<Long, EtlTable> getTables() {
        return tables;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getOutputFilePattern() {
        return outputFilePattern;
    }

    public String getLabel() {
        return label;
    }

    public EtlJobProperty getProperties() {
        return properties;
    }

    public ConfigVersion getConfigVersion() {
        return configVersion;
    }
}
