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

package com.starrocks.connector.spark.rest.models;

import java.util.List;
import java.util.Objects;

public class Tablet {

    private List<String> routings;
    private Long version;
    private Long versionHash;
    private Long schemaHash;

    public Tablet() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tablet tablet = (Tablet) o;
        return Objects.equals(getRoutings(), tablet.getRoutings())
                && Objects.equals(getVersion(), tablet.getVersion())
                && Objects.equals(getVersionHash(), tablet.getVersionHash())
                && Objects.equals(getSchemaHash(), tablet.getSchemaHash());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRoutings(), getVersion(), getVersionHash(), getSchemaHash());
    }

    public List<String> getRoutings() {
        return routings;
    }

    public void setRoutings(List<String> routings) {
        this.routings = routings;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Long getVersionHash() {
        return versionHash;
    }

    public void setVersionHash(Long versionHash) {
        this.versionHash = versionHash;
    }

    public Long getSchemaHash() {
        return schemaHash;
    }

    public void setSchemaHash(Long schemaHash) {
        this.schemaHash = schemaHash;
    }
}
