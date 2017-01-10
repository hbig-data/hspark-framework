package com.hspark.job.streaming;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;
import scala.collection.mutable.ArrayBuffer;
import scala.util.Either;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Rayn on 2016/9/20.
 * @email liuwei412552703@163.com.
 */
public class SparkReadKafka {

    public static void main(String[] args) throws InterruptedException {
//        if(args.length < 4){
//            System.err.println("Please Usage : ");
//            System.err.println("java -cp com.test.spark.kafka <master> <batchDuration> <zookeeper.connect> <metadata.broker.list> <group.id> <topic>");
//            System.exit(0);
//        }

        String zk = "newledgedata-n7:2181";
        String brokerList = "192.168.1.101:2181";

        JavaStreamingContext kafka = new JavaStreamingContext(args[0], "SparkReadKafka", new Duration(Long.parseLong(args[1])));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zookeeper.connect", zk);
        kafkaParams.put("metadata.broker.list", brokerList);
        kafkaParams.put("group.id", args[4]);


        Set<String> topic = new HashSet<String>();
        topic.add(args[5]);

        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(kafka, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topic);

        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

        JavaDStream<String> javaDStream = stream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);

                String topic = offsets[0].topic();
                int partition = offsets[0].partition();

                return rdd.values();
            }
        });


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
