package com.hspark.test.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleConsumerDemo {

    private SimpleConsumer simpleConsumer = new SimpleConsumer(KafkaProperties.kafkaServerURL,
            KafkaProperties.kafkaServerPort,
            KafkaProperties.connectionTimeOut,
            KafkaProperties.kafkaProducerBufferSize,
            KafkaProperties.clientId);



    /**
     * @param messageSet
     * @throws UnsupportedEncodingException
     */
    private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));
        }
    }

    /**
     * 根据时间戳找到某个客户端消费的offset
     *
     * @param topic     topic
     * @param partition 分区
     * @param clientID  客户端的ID
     * @param whichTime 时间戳
     * @return offset
     */
    public long getLastOffset(String topic, int partition, String clientID, long whichTime) {

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientID);
        OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
        long[] offsets = response.offsets(topic, partition);

        return offsets[0];
    }


    public void main(String[] args) throws Exception {

        System.out.println("Testing single fetch");
        FetchRequest req = new FetchRequestBuilder()
                .clientId(KafkaProperties.clientId)
                // 添加fetch指定目标tipic，分区，起始offset及fetchSize(字节)，可以添加多个fetch
                .addFetch(KafkaProperties.topic, 0, 0L, 100)
                .build();

        /**
         * 拉取消息
          */
        FetchResponse fetchResponse = simpleConsumer.fetch(req);
        printMessages((ByteBufferMessageSet) fetchResponse.messageSet(KafkaProperties.topic, 0));



        System.out.println("Testing single multi-fetch");
        Map<String, List<Integer>> topicMap = new HashMap<String, List<Integer>>() {{
            put(KafkaProperties.topic2, new ArrayList<Integer>() {{
                add(0);
            }});
            put(KafkaProperties.topic3, new ArrayList<Integer>() {{
                add(0);
            }});
        }};

        req = new FetchRequestBuilder().clientId(KafkaProperties.clientId)
                .addFetch(KafkaProperties.topic2, 0, 0L, 100)
                .addFetch(KafkaProperties.topic3, 0, 0L, 100)
                .build();

        fetchResponse = simpleConsumer.fetch(req);


        int fetchReq = 0;
        for (Map.Entry<String, List<Integer>> entry : topicMap.entrySet()) {
            String topic = entry.getKey();
            for (Integer offset : entry.getValue()) {
                System.out.println("Response from fetch request no: " + (++fetchReq));
                printMessages((ByteBufferMessageSet) fetchResponse.messageSet(topic, offset));
            }
        }
    }
}
