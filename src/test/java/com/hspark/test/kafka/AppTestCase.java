package com.hspark.test.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * <pre>
 * User:        Ryan
 * Date:        2017/6/26
 * Email:       liuwei412552703@163.com
 * Version      V1.0
 * Discription:
 */
public class AppTestCase {

    public static void main(String[] args) throws InterruptedException {


        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list","192.168.1.225:9092");

        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("producer.type","sync");

        ProducerConfig config = new ProducerConfig(props);

        // 创建producer
        Producer<String, String> producer = new Producer<String, String>(config);
        for (int j = 0; j < 100000; j++) {
            String s = "hello world " + j;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("data720", s);
            producer.send(data);
            Thread.sleep(3000);
        }
        producer.close();

    }
}
