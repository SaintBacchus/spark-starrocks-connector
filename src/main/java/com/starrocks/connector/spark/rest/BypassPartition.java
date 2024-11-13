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

package com.starrocks.connector.spark.rest;

import org.apache.spark.sql.connector.read.InputPartition;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Starrocks RDD partition info.
 */
public class BypassPartition implements Serializable, Comparable<BypassPartition>, InputPartition {

    private final Long tabletId;
    private final Long version;
    private final String storagePath;
    private final String queryPlan;

    private final String selectClause;
    private final String filterClause;

    private Set<String> requiredFieldNames = new HashSet<>(0);

    public BypassPartition(Long tabletId,
                           Long version,
                           String storagePath,
                           String queryPlan,
                           String selectClause,
                           String filterClause) {
        this.tabletId = tabletId;
        this.version = version;
        this.storagePath = storagePath;
        this.queryPlan = queryPlan;
        this.selectClause = selectClause;
        this.filterClause = filterClause;
    }

    public Long getTabletId() {
        return tabletId;
    }

    public Long getVersion() {
        return version;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public String getQueryPlan() {
        return queryPlan;
    }

    public String getSelectClause() {
        return selectClause;
    }

    public String getFilterClause() {
        return filterClause;
    }

    public Set<String> getRequiredFieldNames() {
        return requiredFieldNames;
    }

    public void setRequiredFieldNames(Set<String> requiredFieldNames) {
        this.requiredFieldNames = requiredFieldNames;
    }

    @Override
    public int compareTo(@NotNull BypassPartition that) {
        return Long.compare(tabletId, that.tabletId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BypassPartition that = (BypassPartition) o;
        return Objects.equals(tabletId, that.tabletId) &&
                Objects.equals(version, that.version) &&
                Objects.equals(storagePath, that.storagePath) &&
                Objects.equals(queryPlan, that.queryPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTabletId(), getVersion(), getStoragePath(), getQueryPlan());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", "[", "]")
                .add("tabletId=" + tabletId)
                .add("version=" + version)
                .add("storagePath='" + storagePath + "'")
                .add("queryPlan='" + queryPlan + "'")
                .add("selectClause='" + selectClause + "'")
                .add("filterClause='" + filterClause + "'")
                .toString();
    }
}
