/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hspark.test.kafka;

import kafka.consumer.ConsumerConfig;

import java.util.Properties;

public interface KafkaProperties {
    public final static String zkConnect = "localhost:2181";
    public final static String groupId = "test-consumer-group";
    public final static String topic = "yaxin-test";
    public final static String kafkaServerURL = "localhost";
    public final static int kafkaServerPort = 9092;
    public final static int kafkaProducerBufferSize = 64 * 1024;
    public final static int connectionTimeOut = 10000;
    public final static int reconnectInterval = 10000;
    public final static String topic2 = "yaxin-test";
    public final static String topic3 = "topic3";
    public final static String clientId = "SimpleConsumerDemoClient";

    public static final String brokerList = "192.168.1.234:9092,192.168.1.235:9092";

    public static final String serializerClass = "kafka.serializer.StringEncoder";



}
