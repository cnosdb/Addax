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

import com.wgzhao.addax.common.base.Key;

public final class InfluxDBKey
        extends Key
{
    public static final String ENDPOINT = "endpoint";
    public static final String CONNECT_TIMEOUT_SECONDS = "connTimeout";
    public static final String SOCKET_TIMEOUT_SECONDS = "socketTimeout";

    /**
     *  Request parameter 'epoch' to convert result timestamps.
     */
    public static final String EPOCH = "epoch";

    /**
     * Request parameter 'chunked'.
     */
    public static final String CHUNKED = "chunked";

    /**
     * Request parameter 'chunk_size'.
     */
    public static final String CHUNK_SIZE = "chunkSize";

    /**
     * Fetch table info from InfluxDB by 'SHOW TAG KEYS FROM <u>table</u>' and 'SHOW FIELD KEYS FROM <u>table</u>'
     * and send to writer.
     */
    public static final String SEND_TABLE_INFO = "sendTableInfo";
}
