package com.hspark.test.kafka;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/11/10 14:54.
 */
public class KafkaOffsetExample {


    private static KafkaCluster kafkaCluster = null;

    private static HashMap<String, String> kafkaParam = new HashMap<String, String>();

    private static Broadcast<HashMap<String, String>> kafkaParamBroadcast = null;

    private static scala.collection.immutable.Set<String> immutableTopics = null;

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("tachyon-test-consumer");

        Set<String> topicSet = new HashSet<String>();
        topicSet.add("test_topic");


        kafkaParam.put("metadata.broker.list", "test:9092");
        kafkaParam.put("group.id", "com.xueba207.test");

        // transform java Map to scala immutable.map
        scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParam);
        scala.collection.immutable.Map<String, String> scalaKafkaParam =
                testMap.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(Tuple2<String, String> v1) {
                        return v1;
                    }
                });

        // init KafkaCluster
        kafkaCluster = new KafkaCluster(scalaKafkaParam);

        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        immutableTopics = mutableTopics.toSet();
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = (scala.collection.immutable.Set<TopicAndPartition>)kafkaCluster.getPartitions(immutableTopics).right().get();

        // kafka direct stream 初始化时使用的offset数据
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();

        // 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
        if (kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).isLeft()) {

            System.out.println(kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).left().get());

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }

        }
        // offset已存在, 使用保存的offset
        else {

            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = (scala.collection.immutable.Map<TopicAndPartition, Object>)kafkaCluster.getConsumerOffsets("com.ryan.test", topicAndPartitionSet2).right().get();

            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }

        }

        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
        kafkaParamBroadcast = jssc.sparkContext().broadcast(kafkaParam);

        // create direct stream
        JavaInputDStream<String> message = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParam,
                consumerOffsetsLong,
                new Function<MessageAndMetadata<String, String>, String>() {
                    public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                }
        );

        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<String> javaDStream = message.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
            }
        });

        // output
        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> v1) throws Exception {
                if (v1.isEmpty()) return;

                //处理rdd数据，这里保存数据为hdfs的parquet文件
                HiveContext hiveContext = new HiveContext(jssc.sparkContext());
                Dataset<Row> rowDataset = hiveContext.jsonRDD(v1);

                for (OffsetRange o : offsetRanges.get()) {

                    // 封装topic.partition 与 offset对应关系 java Map
                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

                    // 转换java map to scala immutable.map
                    scala.collection.mutable.Map<TopicAndPartition, Object> testMap =
                            JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                            testMap.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                                public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                                    return v1;
                                }
                            });

                    // 更新offset到kafkaCluster
                    kafkaCluster.setConsumerOffsets(kafkaParamBroadcast.getValue().get("group.id"), scalatopicAndPartitionObjectMap);

//                    System.out.println(
//                            o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
//                    );
                }
            }
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}