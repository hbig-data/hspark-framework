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


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Kafka 生产者测试
 */
public class SimpleKafkaProducer extends Thread {
    private final Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public SimpleKafkaProducer(String topic) {
        props.put("serializer.class", KafkaProperties.serializerClass);
        props.put("metadata.broker.list", KafkaProperties.brokerList);
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {
        int messageNo = 1;
        while (true) {
            String messageStr = new String("Message_" + messageNo);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            messageNo++;
        }
    }

    public static void main(String[] args) {

        SimpleKafkaProducer consumerThread = new SimpleKafkaProducer(KafkaProperties.topic);
        consumerThread.start();
    }
}
