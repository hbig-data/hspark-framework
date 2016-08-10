package com.hspark.job.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/7 17:43.
 */
public class SparkStreamingKafka implements Serializable {

    /**
     * 写offset 到 zookeeper
     *
     * @param zkClient
     * @param zkPathRoot
     * @param offsetRanges
     */
    public static void writeOffsetToZookeeper(String zkClient, String zkPathRoot, OffsetRange[] offsetRanges) {

    }

    /**
     * @param rdd
     * @param outputFolderPath
     * @param definedDuration
     */
    public static long processEachRDD(JavaRDD<String> rdd, String outputFolderPath, String definedDuration) {

        return 0;
    }


    public static void main(String[] args) {

        final String zkClient = "";
        final String zkPathRoot = "/kafka/offset";
        final String outputFolderPath = "E:/test";
        final String definedDuration = "E:/test";

        Map<String, String> kafkaParams = new HashMap<String, String>();
        Set<String> topicsSet = new HashSet<String>();

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.streaming.receiver.writeAheadLog.enable","true");
        sparkConf.setMaster("local[1]");
        sparkConf.setAppName("kafkaSpark");

        JavaSparkContext jssc = new JavaSparkContext(sparkConf);
        jssc.checkpointFile("E:/test/checkpoint");

        JavaStreamingContext jsc = new JavaStreamingContext(jssc, Duration.apply(3 * 1_000));

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        messages.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                JavaRDD<String> valueRDD = rdd.values();
                long msgNum = processEachRDD(valueRDD, outputFolderPath, definedDuration);
                if (msgNum > 0 && zkPathRoot != null) {
                    writeOffsetToZookeeper(zkClient, zkPathRoot, offsets);
                }
                return null;
            }
        });


        /**
         * =========================================================================================================
         */
        JavaPairInputDStream<String, String> messages2 = KafkaUtils.createDirectStream(jsc, String.class, String.class, kafka.serializer.StringDecoder.class, kafka.serializer.StringDecoder.class, kafkaParams, topicsSet);

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference();
        JavaDStream<String> lines2 = messages2.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {


            @Override
            public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
            }
        }).map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        lines2.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> rdd) throws Exception {
                long msgNum = processEachRDD(rdd, outputFolderPath, definedDuration);
                if (msgNum > 0 && zkPathRoot != null) {
                    OffsetRange[] offsets = offsetRanges.get();
                    writeOffsetToZookeeper(zkClient, zkPathRoot, offsets);

                }
                return null;
            }
        });
    }
}
