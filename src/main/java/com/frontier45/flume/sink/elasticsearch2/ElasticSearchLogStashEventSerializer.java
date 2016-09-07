/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.frontier45.flume.sink.elasticsearch2;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Maps;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * Serialize flume events into the same format LogStash uses</p>
 * <p>
 * This can be used to send events to ElasticSearch and use clients such as
 * Kabana which expect Logstash formated indexes
 * <p>
 * <pre>
 * {
 *    "@timestamp": "2010-12-21T21:48:33.309258Z",
 *    "@tags": [ "array", "of", "tags" ],
 *    "@type": "string",
 *    "@source": "source of the event, usually a URL."
 *    "@source_host": ""
 *    "@source_path": ""
 *    "@fields":{
 *       # a set of fields for this event
 *       "user": "jordan",
 *       "command": "shutdown -r":
 *     }
 *     "@message": "the original plain-text message"
 *   }
 * </pre>
 * <p>
 * If the following headers are present, they will map to the above logstash
 * output as long as the logstash fields are not already present.</p>
 * <p>
 * <pre>
 *  timestamp: long -> @timestamp:Date
 *  host: String -> @source_host: String
 *  src_path: String -> @source_path: String
 *  type: String -> @type: String
 *  source: String -> @source: String
 * </pre>
 *
 * @see https
 * ://github.com/logstash/logstash/wiki/logstash%27s-internal-message-
 * format
 */
public class ElasticSearchLogStashEventSerializer implements
        ElasticSearchEventSerializer {

    @Override
    public XContentBuilder getContentBuilder(Event event) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        appendBody(builder, event);
        appendHeaders(builder, event);
        return builder;
    }

    private void appendBody(XContentBuilder builder, Event event)
            throws IOException {
        byte[] body = event.getBody();
        ContentBuilderUtil.appendField(builder, "@message", body);
    }

    private void appendHeaders(XContentBuilder builder, Event event)
            throws IOException {
        Map<String, String> headers = Maps.newHashMap(event.getHeaders());
        Set<String> set = new HashSet<>();

        String timestamp = headers.get("timestamp");
        if (!StringUtils.isBlank(timestamp)
                && StringUtils.isBlank(headers.get("@timestamp"))) {
            long timestampMs = Long.parseLong(timestamp);
            builder.field("@timestamp", new Date(timestampMs));
            set.add("timestamp");
        }

        String source = headers.get("source");
        if (!StringUtils.isBlank(source)
                && StringUtils.isBlank(headers.get("@source"))) {
//            ContentBuilderUtil.appendField(builder, "@source",source.getBytes(charset));
            builder.field("@source", source);
            set.add("source");
        }

        String type = headers.get("type");
        if (!StringUtils.isBlank(type)
                && StringUtils.isBlank(headers.get("@type"))) {
//            ContentBuilderUtil.appendField(builder, "@type", type.getBytes(charset));
            builder.field("@type", type);
            set.add("type");
        }

        String host = headers.get("host");
        if (!StringUtils.isBlank(host)
                && StringUtils.isBlank(headers.get("@source_host"))) {
//            ContentBuilderUtil.appendField(builder, "@source_host", host.getBytes(charset));
            builder.field("@host", host);
            set.add("host");
        }

        String srcPath = headers.get("src_path");
        if (!StringUtils.isBlank(srcPath)
                && StringUtils.isBlank(headers.get("@source_path"))) {
//            ContentBuilderUtil.appendField(builder, "@source_path", srcPath.getBytes(charset));
            builder.field("@source_path", srcPath);
            set.add("src_path");
        }

        String row = headers.get("row");
        if (!StringUtils.isBlank(row)
                && StringUtils.isBlank(headers.get("@row"))) {
            builder.field("@row", Integer.valueOf(row));
            set.add("row");
        }

        String length = headers.get("length");
        if (!StringUtils.isBlank(length)
                && StringUtils.isBlank(headers.get("@length"))) {
            builder.field("@length", Long.valueOf(length));
            set.add("length");
        }

        builder.startObject("@fields");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if(set.contains(entry.getKey())){
                continue;
            }
//            byte[] val = entry.getValue().getBytes(charset);
//            ContentBuilderUtil.appendField(builder, entry.getKey(), val);
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
    }

    @Override
    public void configure(Context context) {
        // NO-OP...
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // NO-OP...
    }
}
