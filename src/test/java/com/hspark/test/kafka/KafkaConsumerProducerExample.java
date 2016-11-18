package com.hspark.test.kafka;

public class KafkaConsumerProducerExample implements KafkaProperties {

    public static void main(String[] args) {
        SimpleKafkaProducer producerThread = new SimpleKafkaProducer(KafkaProperties.topic);
        producerThread.start();

        KafkaConsumerConnectorExample consumerThread = new KafkaConsumerConnectorExample(KafkaProperties.topic);
        consumerThread.start();

    }
}
