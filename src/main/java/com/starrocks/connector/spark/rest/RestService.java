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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.starrocks.connector.spark.cfg.ConfigurationOptions;
import com.starrocks.connector.spark.cfg.Settings;
import com.starrocks.connector.spark.exception.ConnectedFailedException;
import com.starrocks.connector.spark.exception.IllegalArgumentException;
import com.starrocks.connector.spark.exception.ShouldNeverHappenException;
import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.rest.models.QueryPlan;
import com.starrocks.connector.spark.rest.models.Schema;
import com.starrocks.connector.spark.rest.models.Tablet;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.sql.schema.TableIdentifier;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FENODES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FILTER_QUERY;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_READ_FIELD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_DEFAULT;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_MIN;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER;
import static com.starrocks.connector.spark.util.ErrorMessages.CONNECT_FAILED_MESSAGE;
import static com.starrocks.connector.spark.util.ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE;
import static com.starrocks.connector.spark.util.ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE;
import static com.starrocks.connector.spark.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;

/**
 * Service for communicate with StarRocks FE.
 */
public class RestService implements Serializable {

    private static Logger LOG = LoggerFactory.getLogger(RestService.class);

    public static final int REST_RESPONSE_STATUS_OK = 200;
    private static final String API_PREFIX = "/api";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";

    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    static {
        JSON_OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * send request to StarRocks FE and get response json string.
     *
     * @param cfg     configuration of request
     * @param request {@link HttpRequestBase} real request
     * @param logger  {@link Logger}
     * @return StarRocks FE response in json string
     * @throws ConnectedFailedException throw when cannot connect to StarRocks FE
     */
    private static String send(Settings cfg, HttpRequestBase request, Logger logger) throws
            ConnectedFailedException {
        int connectTimeout = cfg.getIntegerProperty(ConfigurationOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS,
                ConfigurationOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT);
        int socketTimeout = cfg.getIntegerProperty(ConfigurationOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS,
                ConfigurationOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT);
        int retries = cfg.getIntegerProperty(ConfigurationOptions.STARROCKS_REQUEST_RETRIES,
                ConfigurationOptions.STARROCKS_REQUEST_RETRIES_DEFAULT);
        logger.trace("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                connectTimeout, socketTimeout, retries);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .setRedirectsEnabled(true)
                .build();

        request.setConfig(requestConfig);

        String user = cfg.getProperty(STARROCKS_REQUEST_AUTH_USER, "");
        String password = cfg.getProperty(STARROCKS_REQUEST_AUTH_PASSWORD, "");
        UsernamePasswordCredentials creds = new UsernamePasswordCredentials(user, password);
        HttpClientContext context = HttpClientContext.create();
        try {
            request.addHeader(new BasicScheme().authenticate(creds, request, context));
        } catch (AuthenticationException e) {
            logger.error(CONNECT_FAILED_MESSAGE, request.getURI(), e);
            throw new ConnectedFailedException(request.getURI().toString(), e);
        }

        logger.info("Send request to StarRocks FE '{}' with user '{}'.", request.getURI(), user);

        IOException ex = null;
        String status = null;
        String responseEntity = null;

        for (int attempt = 0; attempt < retries; attempt++) {
            try (CloseableHttpClient httpClient = HttpClients.custom()
                    .setRedirectStrategy(new LaxRedirectStrategy() {
                        @Override
                        public HttpUriRequest getRedirect(HttpRequest req, HttpResponse rep, HttpContext ctx)
                                throws ProtocolException {
                            String method = req.getRequestLine().getMethod();
                            if (HttpPost.METHOD_NAME.equalsIgnoreCase(method)) {
                                // FIXME deep copy?
                                request.setURI(getLocationURI(req, rep, ctx));
                                return request;
                            }
                            return super.getRedirect(req, rep, ctx);
                        }
                    })
                    .addInterceptorFirst((HttpRequestInterceptor) (req, ctx) -> req.removeHeaders(HTTP.CONTENT_LEN))
                    .build()) {
                logger.debug("Attempt {} to request {}.", attempt, request.getURI());
                CloseableHttpResponse response = httpClient.execute(request, context);
                status = response.getStatusLine().toString();
                responseEntity = response.getEntity() == null
                        ? null : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                    logger.warn("Failed to get response from StarRocks FE {}, http status is {}, entity: {}",
                            request.getURI(), status, responseEntity);
                    continue;
                }
                logger.trace("Success get response from StarRocks FE: {}, response is: {}.",
                        request.getURI(), responseEntity);
                return responseEntity;
            } catch (IOException e) {
                ex = e;
                logger.warn(CONNECT_FAILED_MESSAGE, request.getURI(), e);
            }
        }

        logger.error(CONNECT_FAILED_MESSAGE, request.getURI(), ex);
        throw new ConnectedFailedException(request.getURI().toString(), status, responseEntity, ex);
    }

    /**
     * parse table identifier to array.
     *
     * @param tableIdentifier table identifier string
     * @return first element is db name, second element is table name
     * @throws IllegalArgumentException table identifier is illegal
     */
    @VisibleForTesting
    public static String[] parseIdentifier(String tableIdentifier) throws IllegalArgumentException {
        if (StringUtils.isEmpty(tableIdentifier)) {
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        String[] identifier = tableIdentifier.split("\\.");
        if (identifier.length != 2) {
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        return identifier;
    }

    /**
     * choice a StarRocks FE node to request.
     *
     * @param feNodes StarRocks FE node list, separate be comma
     * @param logger  slf4j logger
     * @return the chosen one StarRocks FE node
     * @throws IllegalArgumentException fe nodes is illegal
     */
    @VisibleForTesting
    static String randomEndpoint(String feNodes, Logger logger) throws IllegalArgumentException {
        logger.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new IllegalArgumentException("fenodes", feNodes);
        }
        List<String> nodes = Arrays.asList(feNodes.split(","));
        Collections.shuffle(nodes);
        return nodes.get(0).trim();
    }

    @VisibleForTesting
    static String getSchemaUriStr(Settings cfg) throws IllegalArgumentException {
        String[] identifier = parseIdentifier(cfg.getProperty(STARROCKS_TABLE_IDENTIFIER));
        String endPoint = randomEndpoint(cfg.getProperty(STARROCKS_FENODES), LOG);
        if (!endPoint.startsWith("http")) {
            endPoint = "http://" + endPoint;
        }
        return endPoint + API_PREFIX + "/" + identifier[0] + "/" + identifier[1] + "/";
    }

    /**
     * discover StarRocks table schema from StarRocks FE.
     *
     * @param cfg    configuration of request
     * @param logger slf4j logger
     * @return StarRocks table schema
     * @throws StarRocksException throw when discover failed
     */
    public static Schema getSchema(Settings cfg, Logger logger)
            throws StarRocksException {
        logger.trace("Finding schema.");
        HttpGet httpGet = new HttpGet(getSchemaUriStr(cfg) + SCHEMA + "?withExtendedInfo=true");
        String response = send(cfg, httpGet, logger);
        logger.debug("Find schema response is '{}'.", response);
        return parseSchema(response, logger);
    }

    /**
     * translate StarRocks FE response to inner {@link Schema} struct.
     *
     * @param response StarRocks FE response
     * @param logger   {@link Logger}
     * @return inner {@link Schema} struct
     * @throws StarRocksException throw when translate failed
     */
    @VisibleForTesting
    public static Schema parseSchema(String response, Logger logger) throws StarRocksException {
        logger.trace("Parse response '{}' to schema.", response);
        Schema schema;
        try {
            schema = JSON_OBJECT_MAPPER.readValue(response, Schema.class);
        } catch (JsonParseException e) {
            String errMsg = "StarRocks FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "StarRocks FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        }

        if (schema == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (schema.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "StarRocks FE's response is not OK, status is " + schema.getStatus();
            logger.error(errMsg);
            throw new StarRocksException(errMsg);
        }
        logger.debug("Parsing schema result is '{}'.", schema);
        return schema;
    }

    /**
     * find StarRocks RDD partitions from StarRocks FE.
     */
    public static List<RpcPartition> findPartitions(Settings settings, Logger logger) throws StarRocksException {
        TableIdentifier identifier = TableIdentifier.createFrom(settings.getProperty(STARROCKS_TABLE_IDENTIFIER));
        String selectClause = settings.getProperty(STARROCKS_READ_FIELD, "*");
        String filterClause = settings.getProperty(STARROCKS_FILTER_QUERY);
        return findPartitions(identifier, selectClause, filterClause, settings, logger);
    }

    /**
     * find StarRocks RDD partitions from StarRocks FE.
     */
    public static List<RpcPartition> findPartitions(TableIdentifier identifier,
                                                    String selectClause,
                                                    String filterClause,
                                                    Settings settings,
                                                    Logger logger) throws StarRocksException {
        QueryPlan queryPlan = getQueryPlan(identifier, selectClause, filterClause, settings, logger);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan, logger);
        return tabletsMapToPartition(
                settings,
                be2Tablets,
                selectClause,
                filterClause,
                queryPlan.getOpaquedQueryPlan(),
                identifier.getDatabase(),
                identifier.getTable(),
                logger);
    }

    /**
     * find StarRocks RDD partitions from StarRocks FE.
     */
    public static List<BypassPartition> splitTasks(TableIdentifier identifier,
                                                   String selectClause,
                                                   String filterClause,
                                                   StarRocksSchema schema,
                                                   Settings settings,
                                                   Logger logger) {
        QueryPlan queryPlan = getQueryPlan(identifier, selectClause, filterClause, settings, logger);

        Map<Long, String> tablet2StoragePath = new HashMap<>();
        for (EtlJobConfig.EtlPartition partition : schema.getEtlTable().getPartitionInfo().getPartitions()) {
            partition.getTabletIds().forEach(
                    tabletId -> tablet2StoragePath.put(tabletId, partition.getStoragePath())
            );
        }

        Map<String, Tablet> partitions = queryPlan.getPartitions();
        return partitions.entrySet().stream()
                .map(entry -> {
                    long tabletId = Long.parseLong(entry.getKey());
                    Tablet tablet = entry.getValue();
                    String storagePath = tablet2StoragePath.get(tabletId);
                    if (null == storagePath) {
                        logger.warn("No storage path found for tablet {}", tabletId);
                    }
                    return new BypassPartition(
                            tabletId,
                            tablet.getVersion(),
                            storagePath,
                            queryPlan.getOpaquedQueryPlan(),
                            selectClause,
                            filterClause
                    );
                })
                .collect(Collectors.toList());
    }

    /**
     * Get sql's query plan from FE.
     *
     * @see #getQueryPlan(TableIdentifier, String, String, Settings, Logger)
     */
    public static QueryPlan getQueryPlan(TableIdentifier identifier, Settings settings, Logger logger) {
        String selectClause = settings.getProperty(STARROCKS_READ_FIELD, "*");
        String filterClause = settings.getProperty(STARROCKS_FILTER_QUERY);
        return getQueryPlan(identifier, selectClause, filterClause, settings, logger);
    }

    /**
     * Get sql's query plan from FE.
     */
    public static QueryPlan getQueryPlan(TableIdentifier identifier,
                                         String selectClause,
                                         String filterClause,
                                         Settings settings,
                                         Logger logger) {
        String finalSelectClause = Optional.ofNullable(selectClause).orElse("*");
        String sql = String.format("SELECT %s FROM `%s`.`%s`",
                finalSelectClause, identifier.getDatabase(), identifier.getTable()
        );
        if (StringUtils.isNotBlank(filterClause)) {
            sql += String.format(" WHERE %s", filterClause);
        }
        logger.info("Get query plan for table[{}] by sql[{}]", identifier.toFullName(), sql);
        if (StringUtils.isBlank(finalSelectClause)) {
            throw new IllegalStateException("Invalid sql when get query plan: " + sql);
        }

        HttpPost httpPost = new HttpPost(getSchemaUriStr(settings) + QUERY_PLAN);
        String entity = "{\"sql\": \"" + sql + "\"}";
        logger.debug("Post body Sending to StarRocks FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentType(ContentType.APPLICATION_JSON.toString());
        httpPost.setEntity(stringEntity);

        String resStr = send(settings, httpPost, logger);
        logger.debug("Find partition response is '{}'.", resStr);
        return parseQueryPlan(resStr, logger);
    }

    /**
     * translate StarRocks FE response string to inner {@link QueryPlan} struct.
     *
     * @param response StarRocks FE response string
     * @param logger   {@link Logger}
     * @return inner {@link QueryPlan} struct
     * @throws StarRocksException throw when translate failed.
     */
    @VisibleForTesting
    static QueryPlan parseQueryPlan(String response, Logger logger) throws StarRocksException {
        QueryPlan queryPlan;
        try {
            queryPlan = JSON_OBJECT_MAPPER.readValue(response, QueryPlan.class);
        } catch (JsonParseException e) {
            String errMsg = "StarRocks FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "StarRocks FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        }

        if (queryPlan == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (queryPlan.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "StarRocks FE's response is not OK, status is " + queryPlan.getStatus();
            logger.error(errMsg);
            throw new StarRocksException(errMsg);
        }
        logger.debug("Parsing partition result is '{}'.", queryPlan);
        return queryPlan;
    }

    /**
     * select which StarRocks BE to get tablet data.
     *
     * @param queryPlan {@link QueryPlan} translated from StarRocks FE response
     * @param logger    {@link Logger}
     * @return BE to tablets {@link Map}
     * @throws StarRocksException throw when select failed.
     */
    @VisibleForTesting
    static Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan, Logger logger) throws StarRocksException {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Map.Entry<String, Tablet> part : queryPlan.getPartitions().entrySet()) {
            logger.debug("Parse tablet info: '{}'.", part);
            long tabletId;
            try {
                tabletId = Long.parseLong(part.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + part.getKey() + "' to long failed.";
                logger.error(errMsg, e);
                throw new StarRocksException(errMsg, e);
            }
            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : part.getValue().getRoutings()) {
                logger.trace("Evaluate StarRocks BE '{}' to tablet '{}'.", candidate, tabletId);
                if (!be2Tablets.containsKey(candidate)) {
                    logger.debug("Choice a new StarRocks BE '{}' for tablet '{}'.", candidate, tabletId);
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                        logger.debug("Current candidate StarRocks BE to tablet '{}' is '{}' with tablet count {}.",
                                tabletId, target, tabletCount);
                    }
                }
            }
            if (target == null) {
                String errMsg = "Cannot choice StarRocks BE for tablet " + tabletId;
                logger.error(errMsg);
                throw new StarRocksException(errMsg);
            }

            logger.debug("Choice StarRocks BE '{}' for tablet '{}'.", target, tabletId);
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }

    /**
     * tablet count limit for one StarRocks RDD partition
     *
     * @param cfg    configuration of request
     * @param logger {@link Logger}
     * @return tablet count limit
     */
    @VisibleForTesting
    static int tabletCountLimitForOnePartition(Settings cfg, Logger logger) {
        int tabletsSize = STARROCKS_TABLET_SIZE_DEFAULT;
        if (cfg.getProperty(STARROCKS_TABLET_SIZE) != null) {
            try {
                tabletsSize = Integer.parseInt(cfg.getProperty(STARROCKS_TABLET_SIZE));
            } catch (NumberFormatException e) {
                logger.warn(PARSE_NUMBER_FAILED_MESSAGE, STARROCKS_TABLET_SIZE, cfg.getProperty(STARROCKS_TABLET_SIZE));
            }
        }
        if (tabletsSize < STARROCKS_TABLET_SIZE_MIN) {
            logger.warn("{} is less than {}, set to default value {}.",
                    STARROCKS_TABLET_SIZE, STARROCKS_TABLET_SIZE_MIN, STARROCKS_TABLET_SIZE_MIN);
            tabletsSize = STARROCKS_TABLET_SIZE_MIN;
        }
        logger.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    /**
     * translate BE tablets map to StarRocks RDD partition.
     *
     * @param cfg              configuration of request
     * @param be2Tablets       BE to tablets {@link Map}
     * @param opaquedQueryPlan StarRocks BE execute plan getting from StarRocks FE
     * @param database         database name of StarRocks table
     * @param table            table name of StarRocks table
     * @param logger           {@link Logger}
     * @return Starrocks RDD partition {@link List}
     * @throws IllegalArgumentException throw when translate failed
     */
    @VisibleForTesting
    static List<RpcPartition> tabletsMapToPartition(Settings cfg,
                                                    Map<String, List<Long>> be2Tablets,
                                                    String selectClause,
                                                    String filterClause,
                                                    String opaquedQueryPlan,
                                                    String database,
                                                    String table,
                                                    Logger logger) {
        int tabletsSize = tabletCountLimitForOnePartition(cfg, logger);
        List<RpcPartition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            logger.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets = new HashSet<>(beInfo.getValue().subList(
                        first, Math.min(beInfo.getValue().size(), first + tabletsSize)));
                first = first + tabletsSize;
                RpcPartition rpcPartition = new RpcPartition(
                        database, table, cfg, beInfo.getKey(),
                        partitionTablets, opaquedQueryPlan, selectClause, filterClause);
                logger.debug("Generate one PartitionDefinition '{}'.", rpcPartition);
                partitions.add(rpcPartition);
            }
        }
        return partitions;
    }

}
