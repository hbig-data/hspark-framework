package com.hspark.job.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Rayn on 2016/9/20.
 * @email liuwei412552703@163.com.
 */
public class SparkReadKafka {

    public static void main(String[] args) throws InterruptedException {
        if(args.length < 4){
            System.err.println("Please Usage : ");
            System.err.println("java -cp com.test.spark.kafka <master> <batchDuration> <zookeeper.connect> <metadata.broker.list> <group.id> <topic>");
            System.exit(0);
        }
        JavaStreamingContext kafka = new JavaStreamingContext(args[0], "SparkReadKafka", new Duration(Long.parseLong(args[1])));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zookeeper.connect", args[2]);
        kafkaParams.put("metadata.broker.list", args[3]);
        kafkaParams.put("group.id", args[4]);


        Set<String> topic = new HashSet<String>();
        topic.add(args[5]);

        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(kafka, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topic);

        JavaDStream<Long> count = stream.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._1() + " ------";
            }
        }).count();

        System.out.println(" ============================== " + count);
        count.print();


        kafka.start();
        kafka.awaitTermination();
    }
}
