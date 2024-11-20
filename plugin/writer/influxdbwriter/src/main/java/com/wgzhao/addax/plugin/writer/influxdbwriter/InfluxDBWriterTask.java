/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.writer.influxdbwriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

public class InfluxDBWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBWriterTask.class);

    private static final int CONNECT_TIMEOUT_SECONDS_DEFAULT = 15;
    private static final int READ_TIMEOUT_SECONDS_DEFAULT = 20;
    private static final int WRITE_TIMEOUT_SECONDS_DEFAULT = 20;
    private static final String TIME_UNIT_DEFAULT = "ms";
    private static final boolean RECEIVE_TABLE_INFO_DEFAULT = false;

    static class PointColumnDefine
    {
        PointColumnDefine() {
            isTime = false;
        }
        public String name;
        public String type;
        public boolean isTime;
    }

    static class OrderedPointColumnDefine extends PointColumnDefine {
        OrderedPointColumnDefine() {
            super();
        }

        public int order;
    }

    protected List<PointColumnDefine> columns = new ArrayList<>();

    private final int columnNumber;
    private final int batchSize;
    private InfluxDB influxDB;

    private final List<String> postSqls;
    private final String table;
    private final String database;
    private final String endpoint;
    private final String username;
    private final String password;
    private final Configuration retentionPolicy;

    private final int connTimeout;
    private final int readTimeout;
    private final int writeTimeout;

    private final TimeUnit timeUnit;
    private final boolean receiveTableInfo;

    public InfluxDBWriterTask(Configuration configuration)
    {
        Configuration conn = configuration.getConfiguration(InfluxDBKey.CONNECTION);
        this.endpoint = conn.getString(InfluxDBKey.ENDPOINT);
        this.table = conn.getString(InfluxDBKey.TABLE);
        this.database = conn.getString(InfluxDBKey.DATABASE);

        this.receiveTableInfo = configuration.getBool(InfluxDBKey.RECEIVE_TABLE_INFO, RECEIVE_TABLE_INFO_DEFAULT);
        if (this.receiveTableInfo) {
            this.columnNumber = 0;
        } else {
            List<Configuration> columns = configuration.getListConfiguration(InfluxDBKey.COLUMN);
            this.columnNumber = columns.size();
            boolean foundTimeColumn = false;
            for (Configuration column : columns) {
                String name = column.getString("name");
                String type = column.getString("type");

                PointColumnDefine columnDefine = new PointColumnDefine();
                columnDefine.name = name;

                if (name.equals("time")) {
                    if (foundTimeColumn) {
                        throw new RuntimeException("already exist time column");
                    }
                    columnDefine.isTime = true;
                    foundTimeColumn = true;
                    if (type != null) {
                        LOG.warn("the time column not need type, will ignore");
                    }
                } else {
                    columnDefine.type = type.toUpperCase();
                }

                this.columns.add(columnDefine);
            }
            if (!foundTimeColumn) {
                LOG.warn("your column config not have time");
            }
        }

        this.username = configuration.getString(InfluxDBKey.USERNAME);
        this.password = configuration.getString(InfluxDBKey.PASSWORD, null);
        this.connTimeout = configuration.getInt(InfluxDBKey.CONNECT_TIMEOUT_SECONDS, CONNECT_TIMEOUT_SECONDS_DEFAULT);
        this.readTimeout = configuration.getInt(InfluxDBKey.READ_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS_DEFAULT);
        this.writeTimeout = configuration.getInt(InfluxDBKey.WRITE_TIMEOUT_SECONDS, WRITE_TIMEOUT_SECONDS_DEFAULT);
        this.batchSize = configuration.getInt(InfluxDBKey.BATCH_SIZE, 1024);
        this.postSqls = configuration.getList(InfluxDBKey.POST_SQL, String.class);
        this.retentionPolicy = configuration.getConfiguration(InfluxDBKey.RETENTION_POLICY);
        String precision = configuration.getString(InfluxDBKey.PRECISION, TIME_UNIT_DEFAULT);
        switch (precision) {
            case "ns":
                this.timeUnit = TimeUnit.NANOSECONDS;
                break;
            case "u":
                this.timeUnit = TimeUnit.MICROSECONDS;
                break;
            case "ms":
                this.timeUnit = TimeUnit.MILLISECONDS;
                break;
            case "s":
                this.timeUnit = TimeUnit.SECONDS;
                break;
            case "m":
                this.timeUnit = TimeUnit.MINUTES;
                break;
            case "h":
                this.timeUnit = TimeUnit.HOURS;
                break;
            default:
                LOG.warn("precision not set, set to 'ms'");
                this.timeUnit = TimeUnit.MILLISECONDS;
        }
    }

    public void init()
    {
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder()
                .connectTimeout(connTimeout, TimeUnit.SECONDS)
                .readTimeout(readTimeout, TimeUnit.SECONDS)
                .writeTimeout(writeTimeout, TimeUnit.SECONDS);

        if (this.username == null) {
            this.influxDB = InfluxDBFactory.connect(this.endpoint, okHttpClientBuilder);
        } else {
            this.influxDB = InfluxDBFactory.connect(this.endpoint, this.username, this.password, okHttpClientBuilder);
        }
        this.influxDB.enableBatch(this.batchSize, this.writeTimeout, TimeUnit.SECONDS);
        influxDB.setDatabase(database);
        Pong pong = influxDB.ping();
        LOG.info("ping influxdb: '{}' with username: '{}', pong: '{}'", endpoint, username, pong.toString());
        if (this.retentionPolicy != null) {
            //create custom retention policy
            String rpName = this.retentionPolicy.getString(InfluxDBKey.RP_NAME, "rp");
            String duration = this.retentionPolicy.getString(InfluxDBKey.RP_DURATION, "1d");
            int replication = this.retentionPolicy.getInt(InfluxDBKey.RP_REPLICATION, 1);
            influxDB.query(new Query("CREATE RETENTION POLICY " + rpName
                    + " ON " + database + " DURATION " + duration + " REPLICATION " + replication));
            influxDB.setRetentionPolicy(rpName);
        }
    }

    public void prepare()
    {
        //
    }

    public void post()
    {

        if (!postSqls.isEmpty()) {
            for (String sql : postSqls) {
                this.influxDB.query(new Query(sql));
            }
        }
    }

    public void destroy()
    {
        this.influxDB.close();
    }

    public void startWrite(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector)
    {
        Record record = null;
        try {
            while ((record = recordReceiver.getFromReader()) != null) {
                Point.Builder builder = Point.measurement(table);
                Map<String, Object> fields = new HashMap<>();

                if (this.receiveTableInfo) {
                    Map<String, String> meta = record.getMeta();
                    if (meta == null) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR,
                                "列配置信息有错误. 因为您配置的任务中，列配置信息需要丛读入数据中取得，但读入数据中并未包含列配置信息."
                        );
                    }
                    List<OrderedPointColumnDefine> columnDefines = this.parseTableInfo(meta);

                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        Column column = record.getColumn(i);
                        if (column == null || column.getRawData() == null) {
                            continue;
                        }
                        PointColumnDefine columnDefine = columnDefines.get(i);
                        // if the column is `time`, we set the point's time.
                        if (columnDefine.isTime) {
                            builder.time(record.getColumn(i).asLong(), this.timeUnit);
                            continue;
                        }
                        try {
                            switch (columnDefine.type) {
                                case "f":
                                    fields.put(columnDefine.name, column.asDouble());
                                    break;
                                case "i":
                                    fields.put(columnDefine.name, column.asLong());
                                    break;
                                case "s":
                                    fields.put(columnDefine.name, column.asString());
                                    break;
                                case "b":
                                    fields.put(columnDefine.name, column.asBoolean());
                                    break;
                                case "T":
                                    builder.tag(columnDefine.name, column.asString());
                                    break;
                                default:
                                    fields.put(columnDefine.name, column.asString());
                                    break;
                            }
                        } catch (java.lang.NullPointerException e) {
                            LOG.warn("Caused NPE: col:'{}', typ:{}, value:'{}", columnDefine.name, columnDefine.type, column);
                        }
                    }
                } else {
                    if (record.getColumnNumber() != this.columnNumber) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR,
                                String.format(
                                        "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                        record.getColumnNumber(),
                                        this.columnNumber)
                        );
                    }
                    for (int i = 0; i < columnNumber; i++) {
                        PointColumnDefine columnDefine = this.columns.get(i);
                        // if the column is `time`, we set the point's time.
                        if (columnDefine.isTime) {
                            builder.time(record.getColumn(i).asLong(), this.timeUnit);
                            continue;
                        }

                        Column column = record.getColumn(i);
                        switch (columnDefine.type) {
                            case "INT":
                            case "LONG":
                                fields.put(columnDefine.name, column.asLong());
                                break;
                            case "DATE":
                                fields.put(columnDefine.name, column.asDate());
                                break;
                            case "DOUBLE":
                                fields.put(columnDefine.name, column.asDouble());
                                break;
                            case "DECIMAL":
                                fields.put(columnDefine.name, column.asBigDecimal());
                                break;
                            case "BINARY":
                                fields.put(columnDefine.name, column.asBytes());
                                break;
                            case "TAG":
                                builder.tag(columnDefine.name, column.asString());
                                break;
                            default:
                                fields.put(columnDefine.name, column.asString());
                                break;
                        }
                    }
                }

                builder.fields(fields);
                influxDB.write(builder.build());
            }
            // flush last batch manual to avoid missing data
            if (influxDB.isBatchEnabled()) {
                influxDB.flush();
            }
        }
        catch (Exception e) {
            taskPluginCollector.collectDirtyRecord(record, e);
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    private List<OrderedPointColumnDefine> parseTableInfo(Map<String, String> tableInfo) {
        List<OrderedPointColumnDefine> columnDefinitions = new ArrayList<>();
        for (Map.Entry<String, String> entry : tableInfo.entrySet()) {
            OrderedPointColumnDefine colDef = new OrderedPointColumnDefine();
            colDef.name = entry.getKey();
            String value = entry.getValue();
            String[] indexAndType = value.split("_");
            if (indexAndType.length != 2) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        String.format(
                                "接收到错误的列配置：'%s'，期望值：{列索引}_{列类型}.", value)
                );
            }
            colDef.order = Integer.parseInt(indexAndType[0]);
            colDef.type = indexAndType[1];
            colDef.isTime = colDef.type.equals("t");
            columnDefinitions.add(colDef);
        }
        columnDefinitions.sort(Comparator.comparingInt(a -> a.order));
        return columnDefinitions;
    }

}
