package com.hspark.test.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by zjy on 2016/8/12.
 */
public class TestKafkaProducer {


    private  static Producer<String,byte[]> kafkaProducer = null;



    static {
        Properties originalProps = new Properties();

        try {
            originalProps.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        ProducerConfig config = new ProducerConfig(originalProps);
        kafkaProducer = new Producer<String, byte[]>(config);

    }

    public static void sendData(String data) {

        kafkaProducer.send(new KeyedMessage<String, byte[]>("yaxin-test", data.getBytes()));

    }

    public static void main(String[] args) throws InterruptedException {

        int i = 0;

        /*while (i < 10) {
            sendData("hello kafka " + i++);
        }*/
        while (true){
            sendData("{\"age\":20,\"name\":\"test\"}");
            System.out.println("{\"age\":20,\"name\":\"test\"}");

            Thread.sleep(1000);
        }

    }



}
