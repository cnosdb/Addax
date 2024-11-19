/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.reader.influxdbreader;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;

public class InfluxDBReaderTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBReaderTask.class);

    private static final int CONNECT_TIMEOUT_SECONDS_DEFAULT = 15;
    private static final int SOCKET_TIMEOUT_SECONDS_DEFAULT = 20;
    private static final boolean SEND_TABLE_INFO_DEFAULT = false;

    private String querySql;
    private final String database;
    private final String endpoint;
    private final String username;
    private final String password;

    private final int connTimeout;

    private final String epoch;
    private final boolean sendTableInfo;
    private final String table;
    private final Map<String, String> tableSchema;

    public InfluxDBReaderTask(Configuration configuration)
    {
        Configuration conn = configuration.getConfiguration(InfluxDBKey.CONNECTION);
        this.querySql = configuration.getString(InfluxDBKey.QUERY_SQL, null);
        this.database = conn.getString(InfluxDBKey.DATABASE);
        this.endpoint = conn.getString(InfluxDBKey.ENDPOINT);
        if (this.querySql == null) {
            this.querySql = "select * from " + conn.getString(InfluxDBKey.TABLE);
        }
        if (!"".equals(configuration.getString(InfluxDBKey.WHERE, ""))) {
            this.querySql += " where " + configuration.getString(InfluxDBKey.WHERE);
        }
        this.username = configuration.getString(InfluxDBKey.USERNAME, "");
        this.password = configuration.getString(InfluxDBKey.PASSWORD, "");
        this.connTimeout = configuration.getInt(InfluxDBKey.CONNECT_TIMEOUT_SECONDS, CONNECT_TIMEOUT_SECONDS_DEFAULT);

        this.epoch = configuration.getString(InfluxDBKey.EPOCH);
        this.sendTableInfo = configuration.getBool(InfluxDBKey.SEND_TABLE_INFO, SEND_TABLE_INFO_DEFAULT);
        this.table = conn.getString(InfluxDBKey.TABLE);
        if (this.sendTableInfo && StringUtils.isNotBlank(this.table)) {
            this.tableSchema = this.queryTableInfo();
        } else {
            this.tableSchema = null;
        }
    }

    public void post()
    {
        //
    }

    public void destroy()
    {
        //
    }

    public void startRead(RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        if (querySql.contains("#lastMinute#")) {
            this.querySql = querySql.replace("#lastMinute#", getLastMinute());
        }
        String result = this.executeQuery(this.querySql);
        try {
            JSONObject jsonObject = JSONObject.parseObject(result);
            JSONArray results = (JSONArray) jsonObject.get("results");
            JSONObject resultsMap = (JSONObject) results.get(0);
            if (resultsMap.containsKey("series")) {
                JSONArray series = (JSONArray) resultsMap.get("series");
                JSONObject seriesMap = (JSONObject) series.get(0);
                List<String> columnNames = null;
                if (this.sendTableInfo) {
                    if (seriesMap.containsKey("columns")) {
                        JSONArray columns = (JSONArray) seriesMap.get("columns");
                        columnNames = new ArrayList<>(columns.size());
                        for (Object col : columns) {
                            columnNames.add(col.toString());
                        }
                    } else {
                        throw AddaxException.asAddaxException(
                                ILLEGAL_VALUE, "JSON path results[].series[].columns[] not exists！", null);
                    }
                }
                if (seriesMap.containsKey("values")) {
                    JSONArray values = (JSONArray) seriesMap.get("values");
                    for (Object row : values) {
                        JSONArray rowArray = (JSONArray) row;
                        Record record = recordSender.createRecord();
                        for (Object s : rowArray) {
                            if (null != s) {
                                record.addColumn(new StringColumn(s.toString()));
                            }
                            else {
                                record.addColumn(new StringColumn());
                            }
                        }
                        if (this.sendTableInfo) {
                            Map<String, String> tableInfo = this.buildTableInfo(columnNames);
                            record.setMeta(tableInfo);
                        }
                        recordSender.sendToWriter(record);
                    }
                }
            }
            else if (resultsMap.containsKey("error")) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, "Error occurred in data sets！", null);
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "Failed to send data", e);
        }
    }

    private String combineUrl(String sql)
    {
        String enc = "utf-8";
        try {
            String url = endpoint + "/query?&db=" + URLEncoder.encode(database, enc);
            if (StringUtils.isNotBlank(this.epoch)) {
                url += "&epoch=" + URLEncoder.encode(this.epoch, enc);
            }
            if (!"".equals(username)) {
                url += "&u=" + URLEncoder.encode(username, enc);
            }
            if (!"".equals(password)) {
                url += "&p=" + URLEncoder.encode(password, enc);
            }
            url += "&q=" + URLEncoder.encode(sql, enc);
            return url;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "Failed to get data point！", e);
        }
    }

    @SuppressWarnings("JavaTimeDefaultTimeZone")
    private String getLastMinute()
    {
        long lastMinuteMilli = LocalDateTime.now().plusMinutes(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return String.valueOf(lastMinuteMilli);
    }

    private String executeQuery(String url) {
        LOG.info("connect influxdb: {} with username: {}", endpoint, username);
        String result;
        try {
            result = Request.get(combineUrl(url))
                    .connectTimeout(Timeout.ofSeconds(connTimeout))
                    .execute()
                    .returnContent().asString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (StringUtils.isBlank(result)) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "Get nothing!", null);
        }
        if (StringUtils.isBlank(result)) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "Get nothing!", null);
        }
        return result;
    }

    private void parseResult(String result, Consumer<JSONArray> consumer) {
        try {
            JSONObject jsonObject = JSONObject.parseObject(result);
            JSONArray results = (JSONArray) jsonObject.get("results");
            JSONObject resultsMap = (JSONObject) results.get(0);
            if (resultsMap.containsKey("series")) {
                JSONArray series = (JSONArray) resultsMap.get("series");
                JSONObject seriesMap = (JSONObject) series.get(0);
                if (seriesMap.containsKey("values")) {
                    JSONArray values = (JSONArray) seriesMap.get("values");
                    for (Object row : values) {
                        JSONArray rowArray = (JSONArray) row;
                        consumer.accept(rowArray);
                    }
                }
            } else if (resultsMap.containsKey("error")) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, "Error occurred in data sets！", null);
            }
        } catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "Failed to send data", e);
        }
    }

    /**
     * Query tag keys and field keys&types and build the table information.
     *
     * <h3>Column type to {column_type}</h3>
     * <ul>
     *     <li>Tag -> T</li>
     *     <li>Time -> t</li>
     *     <li>Float -> f</li>
     *     <li>Integer -> i</li>
     *     <li>String -> s</li>
     *     <li>Boolean -> b</li>
     * </ul>
     * @return The mapping of {column_name} to {column_type}.
     */
    private HashMap<String, String> queryTableInfo() {
        HashMap<String, String> meta = new HashMap<>();

        String result = this.executeQuery("SHOW TAG KEYS FROM " + this.table);
        parseResult(result, (row) -> {
            Object tagKey = row.get(0);
            if (tagKey != null) {
                meta.put(tagKey.toString(), "T");
            } else {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, "Unknown tag key: null, string expected", null);
            }
        });

        result = this.executeQuery("SHOW FIELD KEYS FROM " + this.table);
        parseResult(result, (row) -> {
            Object fieldKey = row.get(0);
            if (fieldKey != null) {
                String key = fieldKey.toString();
                Object fieldType = row.get(1);
                if (fieldType != null) {
                    String typ = fieldType.toString();
                    switch (typ) {
                        case "float":
                            meta.put(key, "f");
                            break;
                        case "integer":
                            meta.put(key, "i");
                            break;
                        case "string":
                            meta.put(key, "s");
                            break;
                        case "boolean":
                            meta.put(key, "b");
                            break;
                        default:
                            throw AddaxException.asAddaxException(
                                    ILLEGAL_VALUE,
                                    String.format("Unknown field type of key '%s': '%s', expected: 'float','integer','string','boolean'", key, typ),
                                    null);
                    }
                }
            } else {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, "Unknown field key: null, string expected", null);
            }
        });

        return meta;
    }

    /**
     * Build table information for a list of columns.
     * @param columnNames the column names of the series.
     * @return The mapping of {column_name} to {column_index}_{column_type}.
     */
    private Map<String, String> buildTableInfo(List<String> columnNames) {
        // Only check if column names passed-in is null.
        if (columnNames == null) {
            return null;
        }
        Map<String, String> tableInfo = new HashMap<>();

        int i = 0;
        for (String columnName : columnNames) {
            if (StringUtils.equalsIgnoreCase(columnName, "time")) {
                tableInfo.put(columnName, String.format("%d_t", i));
            } else {
                String colType = this.tableSchema.get(columnName);
                if (colType != null) {
                    tableInfo.put(columnName, String.format("%d_%s", i, colType));
                }
            }
            i++;
        }

        return tableInfo;
    }

}
