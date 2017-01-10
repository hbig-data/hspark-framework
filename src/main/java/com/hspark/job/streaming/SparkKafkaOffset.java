package com.hspark.job.streaming;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/10 11:38.
 */
public class SparkKafkaOffset {

    private static final Logger LOG = LoggerFactory.getLogger(SparkKafkaOffset.class);

    public static void main(String[] args) {
        JavaStreamingContext kafka = new JavaStreamingContext(args[0], "SparkKafkaOffset", new Duration(Long.parseLong(args[1])));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zookeeper.connect", "localhost:2181");
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("group.id", "default");


        Set<String> topic = new HashSet<String>();
        topic.add(args[5]);

        Map<TopicAndPartition, Long> fromOffset = new HashMap<>();

        JavaInputDStream<Tuple2> directStream = KafkaUtils.createDirectStream(kafka, String.class, String.class, StringDecoder.class, StringDecoder.class, Tuple2.class, kafkaParams, fromOffset, new Function<MessageAndMetadata<String, String>, Tuple2>() {
            @Override
            public Tuple2 call(MessageAndMetadata<String, String> v1) throws Exception {
                String key = v1.key();
                String message = v1.message();
                int partition = v1.partition();
                String topic = v1.topic();


                return new Tuple2(topic, message);
            }
        });

        /**
         * 处理流数据
         */
        directStream.foreachRDD(new VoidFunction<JavaRDD<Tuple2>>() {
            @Override
            public void call(JavaRDD<Tuple2> javardd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) javardd.rdd()).offsetRanges();

                for (OffsetRange o : offsets) {
                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());

                }
            }
        });


        /**
         *
         */
        JavaPairDStream<String, String> pairDStream = directStream.transformToPair(new Function<JavaRDD<Tuple2>, JavaPairRDD<String, String>>() {
            @Override
            public JavaPairRDD<String, String> call(JavaRDD<Tuple2> v1) throws Exception {

                return null;
            }
        });

    }

}
