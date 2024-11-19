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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class InfluxDBWriter
        extends Writer
{

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;

        private String endpoint;
        private String username;
        private String password;
        private String database;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            Configuration conn = originalConfig.getConfiguration(InfluxDBKey.CONNECTION);
            conn.getNecessaryValue(InfluxDBKey.TABLE, REQUIRED_VALUE);
            this.endpoint = conn.getNecessaryValue(InfluxDBKey.ENDPOINT, REQUIRED_VALUE);
            this.database = conn.getNecessaryValue(InfluxDBKey.DATABASE, REQUIRED_VALUE);
            this.username = originalConfig.getString(InfluxDBKey.USERNAME);
            this.password = originalConfig.getString(InfluxDBKey.PASSWORD);
            List<String> columns = originalConfig.getList(InfluxDBKey.COLUMN, String.class);
            boolean receiveTableInfo = originalConfig.getBool(InfluxDBKey.RECEIVE_TABLE_INFO, false);
            if (!receiveTableInfo && (columns == null || columns.isEmpty())) {
                throw AddaxException.asAddaxException(
                        REQUIRED_VALUE,
                        "The parameter [" + InfluxDBKey.COLUMN + "] is not set when [" + InfluxDBKey.RECEIVE_TABLE_INFO +"] is set to true.");
            }
            String precision = originalConfig.getString(InfluxDBKey.PRECISION);
            if (!StringUtils.equalsAny(precision, "ns", "u", "ms", "s", "m", "h")) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        "The parameter [" + InfluxDBKey.PRECISION + "] is not valid, expected: ns, u, ms, s, m, h.");
            }
        }

        @Override
        public void prepare()
        {
            List<String> preSqls = originalConfig.getList(InfluxDBKey.PRE_SQL, String.class);
            if (!preSqls.isEmpty()) {
                InfluxDB influxDB = null;
                try {
                    if (this.username == null) {
                        influxDB = InfluxDBFactory.connect(this.endpoint);
                    } else {
                        influxDB = InfluxDBFactory.connect(this.endpoint, this.username, this.password);
                    }
                    influxDB.setDatabase(database);
                    for (String sql : preSqls) {
                        influxDB.query(new Query(sql));
                    }
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(
                            CONNECT_ERROR, e
                    );
                }
                finally {
                    if (influxDB != null) {
                        influxDB.close();
                    }
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(this.originalConfig.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void post()
        {
            List<String> postSqls = originalConfig.getList(InfluxDBKey.POST_SQL, String.class);
            if (!postSqls.isEmpty()) {
                InfluxDB influxDB = null;
                try {
                    if (this.username == null) {
                        influxDB = InfluxDBFactory.connect(this.endpoint);
                    } else {
                        influxDB = InfluxDBFactory.connect(this.endpoint, this.username, this.password);
                    }
                    influxDB.setDatabase(database);
                    for (String sql : postSqls) {
                        influxDB.query(new Query(sql));
                    }
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE, e
                    );
                }
                finally {
                    if (influxDB != null) {
                        influxDB.close();
                    }
                }
            }
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Writer.Task
    {
        private InfluxDBWriterTask influxDBWriterTask;

        @Override
        public void init()
        {
            Configuration writerSliceConfig = getPluginJobConf();
            this.influxDBWriterTask = new InfluxDBWriterTask(writerSliceConfig);
            this.influxDBWriterTask.init();
        }

        @Override
        public void prepare()
        {
            this.influxDBWriterTask.prepare();
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.influxDBWriterTask.startWrite(recordReceiver, getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.influxDBWriterTask.post();
        }

        @Override
        public void destroy()
        {
            this.influxDBWriterTask.destroy();
        }
    }
}
