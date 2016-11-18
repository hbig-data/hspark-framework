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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/10/10 9:43.
 */
public class KafkaFetchOffset extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFetchOffset.class);

    private SimpleConsumer simpleConsumer = null;

    private long offsetStart = 0;
    private int fetchSize = 10;
    private int partition = 0;

    public KafkaFetchOffset(int partition, long offsetStart) {

        simpleConsumer = new SimpleConsumer(KafkaProperties.kafkaServerURL,
                KafkaProperties.kafkaServerPort,
                KafkaProperties.connectionTimeOut,
                KafkaProperties.kafkaProducerBufferSize,
                KafkaProperties.clientId);

        this.offsetStart = offsetStart;
        this.partition = partition;
    }


    /**
     * 获取某时间段的最后一个 offset
     *
     * @param whichTime
     * @return
     */
    public long getLastOffset(long whichTime) {

        SimpleConsumer consumer = new SimpleConsumer("192.168.1.225", 9092, 30000,
                KafkaProperties.kafkaProducerBufferSize,
                KafkaProperties.clientId);

        TopicAndPartition topicAndPartition = new TopicAndPartition(KafkaProperties.topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), KafkaProperties.clientId);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        long[] offsets = response.offsets(KafkaProperties.topic, partition);

        return offsets[0];
    }

    /**
     * @param messageAndOffsets
     */
    public long printKafkaMessage(ByteBufferMessageSet messageAndOffsets) {
        Iterator<MessageAndOffset> iterator = messageAndOffsets.iterator();

        long offsetStart = 0;
        while (iterator.hasNext()) {
            MessageAndOffset next = iterator.next();
            ByteBuffer payload = next.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);

            offsetStart = next.nextOffset();
            LOG.info("当前数据Offset : {} ,数据长度：{}, 数据值：{}", next.offset(), bytes.length, new String(bytes));
        }


        return offsetStart;
    }

    /**
     * 获取数据
     *
     * @param offsetStart
     * @throws Exception
     */
    public long getKafkaData(int partition, long offsetStart, int fetchSize) throws Exception {
        FetchRequest req = new FetchRequestBuilder().clientId(KafkaProperties.clientId)
                .addFetch(KafkaProperties.topic, partition, offsetStart, fetchSize).build();

        FetchResponse fetchResponse = simpleConsumer.fetch(req);

        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(KafkaProperties.topic, partition);

        return printKafkaMessage(messageAndOffsets);
    }


    @Override
    public void run() {
        long kafkaOffset = offsetStart;
        while (true) {
            try {
                long nextOffset = getKafkaData(partition, kafkaOffset, fetchSize);
                if (nextOffset <= kafkaOffset) {
                    Thread.currentThread().sleep(3000);
                } else {
                    kafkaOffset = nextOffset;
                    LOG.info("=================================== have a rest ===========================================");
                    Thread.currentThread().sleep(5000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]) throws Exception {

        KafkaFetchOffset kafkaFetchOffset = new KafkaFetchOffset(0, 0);
       // kafkaFetchOffset.start();


        long lastOffset = kafkaFetchOffset.getLastOffset(System.currentTimeMillis());
        LOG.info("当前最后的 Offset 为： {}", lastOffset);

    }
}
