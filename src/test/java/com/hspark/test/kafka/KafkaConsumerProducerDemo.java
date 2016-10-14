package com.hspark.test.kafka;

public class KafkaConsumerProducerDemo implements KafkaProperties {

    public static void main(String[] args) {
        SimpleKafkaProducer producerThread = new SimpleKafkaProducer(KafkaProperties.topic);
        producerThread.start();

        SimpleKafkaConsumer consumerThread = new SimpleKafkaConsumer(KafkaProperties.topic);
        consumerThread.start();

    }
}
