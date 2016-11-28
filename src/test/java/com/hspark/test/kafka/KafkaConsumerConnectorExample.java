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


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kakfa 原生 API 接口
 *
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/10/10 9:43.
 */
public class KafkaConsumerConnectorExample extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;

    public KafkaConsumerConnectorExample(String topic) {
       consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "nowledgedata-n7:2181");
        props.put("group.id", "default");
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "10000");
        props.put("auto.offset.reset", "largest");

        return new ConsumerConfig(props);

    }

    @Override
    public void run() {
        System.out.println("开始执行。。。");

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = consumerMap.get(topic);

        KafkaStream<byte[], byte[]> stream = kafkaStreams.get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {

            MessageAndMetadata<byte[], byte[]> next = it.next();

            if(null != next.key()){
                System.out.println("当前消息Offset : " + new String(next.key()) + "当前消息体：" + new String(next.message()));
            } else {
                System.out.println("当前消息Offset : " + next.offset() + ", 当前消息体：" + new String(next.message()));
            }

            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        KafkaConsumerConnectorExample producerThread = new KafkaConsumerConnectorExample(KafkaProperties.topic);
        producerThread.start();
    }
}
